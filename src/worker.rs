use std::sync::Arc;
use std::time::Duration;

use futures_util::future::{BoxFuture, Either, FutureExt};

type WorkerHandler<TInput, TOutput> = Arc<
    dyn Fn(WorkerJob<TInput>) -> BoxFuture<'static, Result<TOutput, RequestError>> + Send + Sync,
>;
use futures_util::stream::StreamExt;
use mongodb::Collection;
use mongodb::bson::{Bson, DateTime, Document, doc, to_bson};
use mongodb::change_stream::ChangeStream;
use mongodb::error::{CommandError, ErrorKind};
use mongodb::options::{
    ChangeStreamOptions, FindOneAndUpdateOptions, FullDocumentType, ReturnDocument,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{self, Duration as TokioDuration, MissedTickBehavior};
use tracing::{error, info, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

use crate::config::Config;
use crate::error::RequestError;
use crate::storage::connect_collection;
use crate::trace::TraceContext;

const DEFAULT_MAX_INFLIGHT: usize = 32;

/// Long-lived component that keeps reading the configured Mongo collection,
/// claims pending tasks, and executes user logic for each `TaskInput`.
#[derive(Debug)]
pub struct Worker {
    config: Config,
    collection: Collection<Document>,
    worker_id: String,
    max_inflight: usize,
}

/// Wrapper passed to user handlers containing the task id (which doubles as the
/// idempotency key), the optional caller `TraceContext`, and the typed payload.
#[derive(Debug)]
pub struct WorkerJob<TInput> {
    pub task_id: String,
    pub trace_context: Option<TraceContext>,
    pub payload: TInput,
}

impl Worker {
    /// Connects to the Mongo deployment described by `config`. Use the same config
    /// that the caller used so both roles share a collection.
    pub async fn connect(config: Config) -> Result<Self, RequestError> {
        let collection = connect_collection(&config).await?;
        Ok(Worker {
            config,
            collection,
            worker_id: format!("worker-{}", Uuid::new_v4()),
            max_inflight: DEFAULT_MAX_INFLIGHT,
        })
    }

    /// Starts the background loop with the provided async handler. Each task is
    /// executed in its own Tokio task up to `max_inflight`.
    pub fn run<TInput, TOutput, H, Fut>(self, handler: H) -> WorkerHandle
    where
        TInput: DeserializeOwned + Send + 'static,
        TOutput: Serialize + Send + Sync + 'static,
        H: Fn(WorkerJob<TInput>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<TOutput, RequestError>> + Send + 'static,
    {
        let (stop_tx, stop_rx) = oneshot::channel();
        let Worker {
            config,
            collection,
            worker_id,
            max_inflight,
        } = self;
        let handler = Arc::new(
            move |job: WorkerJob<TInput>| -> BoxFuture<'static, Result<TOutput, RequestError>> {
                handler(job).boxed()
            },
        );
        let join_handle = tokio::spawn(worker_loop(
            collection,
            stop_rx,
            config.worker_switch_timeout,
            worker_id,
            max_inflight,
            handler,
        ));
        WorkerHandle {
            stop_signal: Some(stop_tx),
            join_handle: Some(join_handle),
        }
    }
}

#[tracing::instrument(skip(collection, stop_rx, handler))]
async fn worker_loop<TInput, TOutput>(
    collection: Collection<Document>,
    mut stop_rx: oneshot::Receiver<()>,
    worker_switch_timeout: Duration,
    worker_id: String,
    max_inflight: usize,
    handler: WorkerHandler<TInput, TOutput>,
) -> Result<(), RequestError>
where
    TInput: DeserializeOwned + Send + 'static,
    TOutput: Serialize + Send + Sync + 'static,
{
    let semaphore = Arc::new(Semaphore::new(max_inflight));
    let mut join_set = JoinSet::new();
    let mut change_stream = match open_change_stream(&collection).await {
        Ok(stream) => stream,
        Err(err) => {
            error!(error=%err, "change streams unavailable; worker exiting");
            return Err(err);
        }
    };

    pump_available_tasks(
        &collection,
        &worker_id,
        worker_switch_timeout,
        &semaphore,
        &handler,
        &mut join_set,
    )
    .await;

    loop {
        if change_stream.is_none() {
            change_stream = match open_change_stream(&collection).await {
                Ok(stream) => stream,
                Err(err) => {
                    error!(error=%err, "change streams unavailable; worker exiting");
                    return Err(err);
                }
            };
            pump_available_tasks(
                &collection,
                &worker_id,
                worker_switch_timeout,
                &semaphore,
                &handler,
                &mut join_set,
            )
            .await;
            continue;
        }
        let change_future = change_stream
            .as_mut()
            .map(|stream| Either::Left(stream.next()))
            .unwrap_or_else(|| Either::Right(futures_util::future::pending()));
        tokio::select! {
            _ = &mut stop_rx => break,
            Some(result) = join_set.join_next(), if !join_set.is_empty() => {
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => error!(error=%e, "worker task error"),
                    Err(e) => error!(error=%e, "worker task panicked"),
                }
            }
            event = change_future => {
                match event {
                    Some(Ok(_)) => {
                        pump_available_tasks(
                            &collection,
                            &worker_id,
                            worker_switch_timeout,
                            &semaphore,
                            &handler,
                            &mut join_set,
                        ).await;
                    }
                    Some(Err(e)) => {
                        warn!(error=%e, "change stream error, will restart");
                        change_stream = None;
                    }
                    None => {
                        warn!("change stream closed, will restart");
                        change_stream = None;
                    }
                }
            }
        }
    }

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => error!(error=%e, "worker task error"),
            Err(e) => error!(error=%e, "worker task panicked"),
        }
    }
    Ok(())
}

fn is_change_stream_unsupported(error: &mongodb::error::Error) -> bool {
    match error.kind.as_ref() {
        ErrorKind::Command(CommandError {
            code, code_name, ..
        }) => {
            matches!(code, 40573 | 40574 | 40615)
                || code_name.eq_ignore_ascii_case("ChangeStreamNotSupported")
                || code_name.eq_ignore_ascii_case("Location40573")
        }
        _ => {
            let msg = error.to_string();
            msg.contains("$changeStream stage is only supported")
                || msg.contains("ChangeStreamNotSupported")
        }
    }
}

#[tracing::instrument(skip(collection))]
async fn claim_next_task(
    collection: &Collection<Document>,
    worker_id: &str,
    worker_switch_timeout: Duration,
) -> Result<Option<Document>, RequestError> {
    let now = DateTime::now();
    let threshold =
        DateTime::from_millis(now.timestamp_millis() - worker_switch_timeout.as_millis() as i64);
    let filter = doc! {
        "$or": [
            {"status": "pending"},
            {"status": "running", "worker_state.heartbeat_at": {"$lt": threshold}},
        ]
    };
    let update = doc! {
        "$set": {
            "status": "running",
            "updated_at": DateTime::now(),
            "worker_state": {
                "worker_id": worker_id,
                "started_at": DateTime::now(),
                "heartbeat_at": DateTime::now(),
                "switch_timeout_ms": worker_switch_timeout.as_millis() as i64,
            }
        }
    };
    let options = FindOneAndUpdateOptions::builder()
        .return_document(ReturnDocument::After)
        .build();
    collection
        .find_one_and_update(filter, update, options)
        .await
        .map_err(|e| RequestError::Database(e.to_string()))
}

async fn open_change_stream(
    collection: &Collection<Document>,
) -> Result<Option<ChangeStream<Document>>, RequestError> {
    let pipeline = vec![doc! {
        "$match": {
            "$or": [
                { "operationType": { "$in": ["insert", "replace"] } },
                {
                    "operationType": "update",
                    "updateDescription.updatedFields.status": "pending"
                }
            ]
        }
    }];
    let options = ChangeStreamOptions::builder()
        .full_document(Some(FullDocumentType::UpdateLookup))
        .build();
    match collection.watch(pipeline, Some(options)).await {
        Ok(stream) => Ok(Some(stream.with_type())),
        Err(e) => {
            if is_change_stream_unsupported(&e) {
                return Err(RequestError::Database(
                    "change streams unsupported; MongoDB must be a replica set or sharded cluster"
                        .into(),
                ));
            }
            warn!(error=%e, "failed to open change stream");
            Ok(None)
        }
    }
}

#[tracing::instrument(skip(collection, semaphore, handler, join_set))]
async fn pump_available_tasks<TInput, TOutput>(
    collection: &Collection<Document>,
    worker_id: &str,
    worker_switch_timeout: Duration,
    semaphore: &Arc<Semaphore>,
    handler: &WorkerHandler<TInput, TOutput>,
    join_set: &mut JoinSet<Result<(), RequestError>>,
) where
    TInput: DeserializeOwned + Send + 'static,
    TOutput: Serialize + Send + Sync + 'static,
{
    loop {
        let Ok(permit) = semaphore.clone().try_acquire_owned() else {
            break;
        };
        match claim_next_task(collection, worker_id, worker_switch_timeout).await {
            Ok(Some(task)) => {
                if let Ok(id) = task.get_str("task_id") {
                    info!(%worker_id, task_id=%id, "claimed task");
                }
                let handler = handler.clone();
                join_set.spawn(process_task(
                    collection.clone(),
                    task,
                    worker_id.to_string(),
                    handler,
                    permit,
                    worker_switch_timeout,
                ));
            }
            Ok(None) => {
                drop(permit);
                break;
            }
            Err(e) => {
                error!(error=%e, "failed to claim task");
                drop(permit);
                break;
            }
        }
    }
}

#[tracing::instrument(
    skip(collection, doc, handler, permit),
    fields(task_id, worker_id, trace_id)
)]
async fn process_task<TInput, TOutput>(
    collection: Collection<Document>,
    doc: Document,
    worker_id: String,
    handler: WorkerHandler<TInput, TOutput>,
    permit: OwnedSemaphorePermit,
    worker_switch_timeout: Duration,
) -> Result<(), RequestError>
where
    TInput: DeserializeOwned + Send + 'static,
    TOutput: Serialize + Send + Sync + 'static,
{
    let _permit = permit;
    tracing::Span::current().record("worker_id", &worker_id);

    let task_id = match doc
        .get_str("task_id")
        .map_err(|_| RequestError::PayloadFormat { field: "task_id" })
    {
        Ok(id) => id.to_string(),
        Err(e) => return Err(e), // Can't mark failed without ID
    };
    tracing::Span::current().record("task_id", &task_id);

    let setup_result: Result<(TInput, Option<TraceContext>), RequestError> = (|| async {
        let payload_bson = doc
            .get("task_input")
            .ok_or(RequestError::PayloadFormat {
                field: "task_input",
            })?
            .clone();
        let payload: TInput =
            mongodb::bson::from_bson(payload_bson).map_err(|_| RequestError::PayloadFormat {
                field: "task_input",
            })?;
            
        let trace_context = doc.get("trace_context").and_then(|raw| {
            if raw == &Bson::Null {
                return None;
            }
            let ctx: TraceContext = mongodb::bson::from_bson(raw.clone()).ok()?;
            tracing::Span::current().record("trace_id", ctx.trace_id.as_str());
            Some(ctx)
        });

        if !heartbeat(&collection, &task_id, &worker_id).await? {
            error!(%worker_id, %task_id, "lost ownership before handler start");
            return Err(RequestError::WorkerGone);
        }
        Ok((payload, trace_context))
    })()
    .await;

    let (payload, trace_context) = match setup_result {
        Ok(v) => v,
        Err(err) => {
            mark_task_failed(
                &collection,
                &task_id,
                &format!("infrastructure error: {err}"),
            )
            .await;
            return Err(err);
        }
    };

    let job = WorkerJob {
        task_id: task_id.clone(),
        trace_context: trace_context.clone(),
        payload,
    };
    let (hb_stop_tx, hb_stop_rx) = oneshot::channel();
    start_heartbeat_loop(
        collection.clone(),
        task_id.clone(),
        worker_id.clone(),
        worker_switch_timeout,
        hb_stop_rx,
    );

    // Create a handler span to measure business logic duration
    // Use span link to connect this worker trace to the caller's trace
    let handler_span = tracing::info_span!(
        "worker.handler",
        %task_id,
        %worker_id,
    );

    // Add a span link to the caller's trace context if available
    if let Some(ref caller_context) = trace_context {
        if let Some(span_context) = caller_context.to_span_context() {
            handler_span.add_link(span_context);
            info!(
                %task_id,
                trace_id = %caller_context.trace_id,
                span_id = %caller_context.span_id,
                "linked worker span to caller span"
            );
        } else {
            warn!(%task_id, "trace context present but invalid");
        }
    }

    let handler_result = {
        let _guard = handler_span.enter();
        handler(job).await
    };
    let _ = hb_stop_tx.send(());
    match handler_result {
        Ok(output) => {
            let output_bson: Bson = to_bson(&output).map_err(|_| RequestError::PayloadFormat {
                field: "task_output",
            })?;
            let filter = doc! {
                "task_id": &task_id,
                "status": "running",
                "worker_state.worker_id": &worker_id,
            };
            let update = doc! {
                "$set": {
                    "status": "succeeded",
                    "task_output": output_bson,
                    "updated_at": DateTime::now(),
                    "worker_state.finished_at": DateTime::now(),
                }
            };
            let result = collection
                .update_one(filter, update, None)
                .await
                .map_err(|e| RequestError::Database(e.to_string()))?;
            if result.matched_count == 0 {
                error!(%worker_id, %task_id, "lost ownership before completing");
                return Err(RequestError::WorkerGone);
            }
            info!(%worker_id, %task_id, "completed task");
            Ok(())
        }
        Err(err) => {
            mark_task_failed(&collection, &task_id, &format!("handler error: {err}")).await;
            Err(err)
        }
    }
}

#[tracing::instrument(skip(collection))]
async fn mark_task_failed(collection: &Collection<Document>, task_id: &str, reason: &str) {
    let update = doc! {
        "$set": {
            "status": "failed",
            "error_reason": reason,
            "updated_at": DateTime::now(),
            "worker_state.finished_at": DateTime::now(),
        }
    };
    if let Err(e) = collection
        .update_one(doc! {"task_id": task_id}, update, None)
        .await
    {
        error!(task_id=%task_id, error=%e, "failed to mark task as failed");
    }
}

#[tracing::instrument(skip(collection))]
async fn heartbeat(
    collection: &Collection<Document>,
    task_id: &str,
    worker_id: &str,
) -> Result<bool, RequestError> {
    let filter = doc! {
        "task_id": task_id,
        "status": "running",
        "worker_state.worker_id": worker_id,
    };
    let update = doc! {
        "$set": {"worker_state.heartbeat_at": DateTime::now()}
    };
    let result = collection
        .update_one(filter, update, None)
        .await
        .map_err(|e| RequestError::Database(e.to_string()))?;
    Ok(result.matched_count > 0)
}

fn start_heartbeat_loop(
    collection: Collection<Document>,
    task_id: String,
    worker_id: String,
    worker_switch_timeout: Duration,
    mut stop_rx: oneshot::Receiver<()>,
) {
    let interval_ms = (worker_switch_timeout.as_millis() / 3).max(500) as u64;
    tokio::spawn(async move {
        let mut ticker = time::interval(TokioDuration::from_millis(interval_ms));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = &mut stop_rx => {
                    break;
                }
                _ = ticker.tick() => {
                    if let Err(e) = heartbeat(&collection, &task_id, &worker_id).await {
                        error!(%worker_id, %task_id, error=%e, "heartbeat failed");
                        break;
                    }
                }
            }
        }
    });
}

/// Guard returned by `Worker::run` so callers can trigger graceful shutdowns
/// (and automatically abort the worker task on drop).
#[derive(Debug)]
pub struct WorkerHandle {
    stop_signal: Option<oneshot::Sender<()>>,
    join_handle: Option<JoinHandle<Result<(), RequestError>>>,
}

impl WorkerHandle {
    /// Ask the worker loop to stop, then await the background task.
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.stop_signal.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.join_handle.take() {
            let _ = handle.await;
        }
    }

    /// Await the worker without sending a stop signal. Useful for detecting early exits.
    pub async fn wait(mut self) -> Result<(), RequestError> {
        if let Some(handle) = self.join_handle.take() {
            match handle.await {
                Ok(result) => result,
                Err(_) => Err(RequestError::WorkerGone),
            }
        } else {
            Ok(())
        }
    }
}

impl Drop for WorkerHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.stop_signal.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.join_handle.take() {
            handle.abort();
        }
    }
}
