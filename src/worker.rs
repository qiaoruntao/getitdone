use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures_util::future::{BoxFuture, Either, FutureExt};

type WorkerHandler<TInput, TOutput> = Arc<
    dyn Fn(WorkerJob<TInput>) -> BoxFuture<'static, Result<TOutput, RequestError>> + Send + Sync,
>;
use futures_util::stream::StreamExt;
use mongodb::Collection;
use mongodb::bson::{Bson, DateTime, Document, doc, from_bson, to_bson};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use mongodb::change_stream::ChangeStream;
use mongodb::options::{
    ChangeStreamOptions, FindOneAndUpdateOptions, FullDocumentType, ReturnDocument,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{self, Duration as TokioDuration, MissedTickBehavior};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::config::Config;
use crate::error::RequestError;
use crate::storage::connect_collection;

const DEFAULT_MAX_INFLIGHT: usize = 32;

pub struct Worker {
    config: Config,
    collection: Collection<Document>,
    worker_id: String,
    max_inflight: usize,
}

pub struct WorkerJob<TInput> {
    pub task_id: String,
    pub trace_context: Option<HashMap<String, String>>,
    pub payload: TInput,
}

impl Worker {
    pub async fn connect(config: Config) -> Result<Self, RequestError> {
        let collection = connect_collection(&config).await?;
        Ok(Worker {
            config,
            collection,
            worker_id: format!("worker-{}", Uuid::new_v4()),
            max_inflight: DEFAULT_MAX_INFLIGHT,
        })
    }

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

async fn worker_loop<TInput, TOutput>(
    collection: Collection<Document>,
    mut stop_rx: oneshot::Receiver<()>,
    worker_switch_timeout: Duration,
    worker_id: String,
    max_inflight: usize,
    handler: WorkerHandler<TInput, TOutput>,
) where
    TInput: DeserializeOwned + Send + 'static,
    TOutput: Serialize + Send + Sync + 'static,
{
    let semaphore = Arc::new(Semaphore::new(max_inflight));
    let mut join_set = JoinSet::new();
    let mut change_stream = open_change_stream(&collection).await;
    
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
            change_stream = open_change_stream(&collection).await;
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
}

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

async fn open_change_stream(collection: &Collection<Document>) -> Option<ChangeStream<Document>> {
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
        Ok(stream) => Some(stream.with_type()),
        Err(e) => {
            warn!(error=%e, "failed to open change stream");
            None
        }
    }
}

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
        let permit = match semaphore.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => break,
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

    let task_id = match doc
        .get_str("task_id")
        .map_err(|_| RequestError::PayloadFormat { field: "task_id" })
    {
        Ok(id) => id.to_string(),
        Err(e) => return Err(e), // Can't mark failed without ID
    };

    let setup_result: Result<(TInput, Option<HashMap<String, String>>), RequestError> = async {
        let payload_bson = doc
            .get("task_input")
            .ok_or(RequestError::PayloadFormat {
                field: "task_input",
            })?
            .clone();
        let payload_value: Value = mongodb::bson::from_bson(payload_bson).map_err(|_| {
            RequestError::PayloadFormat {
                field: "task_input",
            }
        })?;
        let payload: TInput = serde_json::from_value(payload_value).map_err(|_| {
            RequestError::PayloadFormat {
                field: "task_input",
            }
        })?;
        let trace_context: Option<HashMap<String, String>> = doc.get("trace_context").and_then(|b| from_bson(b.clone()).ok());

        if !heartbeat(&collection, &task_id, &worker_id).await? {
            error!(%worker_id, %task_id, "lost ownership before handler start");
            return Err(RequestError::WorkerGone);
        }
        Ok((payload, trace_context))
    }
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

    let span = tracing::info_span!("worker.process_task", %task_id, %worker_id);
    if let Some(ref context_map) = trace_context {
        let parent_context = opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.extract(context_map)
        });
        span.set_parent(parent_context);
    }
    let _enter = span.enter();

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
    let handler_result = handler(job).await;
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

pub struct WorkerHandle {
    stop_signal: Option<oneshot::Sender<()>>,
    join_handle: Option<JoinHandle<()>>,
}

impl WorkerHandle {
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.stop_signal.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.join_handle.take() {
            let _ = handle.await;
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
