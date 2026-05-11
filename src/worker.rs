use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::future::{BoxFuture, Either, FutureExt};

type WorkerHandler<TInput, TOutput> = Arc<
    dyn Fn(WorkerJob<TInput>) -> BoxFuture<'static, Result<TOutput, RequestError>> + Send + Sync,
>;
use bson::{Bson, DateTime, Document, doc};
use futures_util::stream::StreamExt;
use mongodb::Collection;
use mongodb::change_stream::{ChangeStream, event::ChangeStreamEvent};
use mongodb::error::{CommandError, ErrorKind};
use mongodb::options::{FullDocumentType, ReturnDocument};
#[cfg(feature = "tracing")]
use opentelemetry as _;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{self, Duration as TokioDuration, MissedTickBehavior};
#[cfg(feature = "tracing")]
use tracing::{error, info, warn};
#[cfg(feature = "tracing")]
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

use crate::config::Config;
use crate::error::RequestError;
use crate::storage::connect_collection;
#[cfg(feature = "tracing")]
use crate::trace::TraceContext;

const DEFAULT_MAX_INFLIGHT: usize = 10000;

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
    #[cfg(feature = "tracing")]
    pub trace_context: Option<TraceContext>,
    #[cfg(not(feature = "tracing"))]
    pub trace_context: Option<()>,
    pub payload: TInput,
}

impl Worker {
    /// Connects to the Mongo deployment described by `config`. Use the same config
    /// that the caller used so both roles share a collection.
    pub async fn connect(config: Config) -> Result<Self, RequestError> {
        let collection = connect_collection(&config).await?;
        let worker_id = config
            .worker_id
            .clone()
            .unwrap_or_else(|| format!("worker-{}", Uuid::new_v4()));
        Ok(Worker {
            config,
            collection,
            worker_id,
            max_inflight: DEFAULT_MAX_INFLIGHT,
        })
    }

    /// Override the number of concurrent tasks this worker will execute. The
    /// default is 32.
    pub fn with_max_inflight(mut self, n: usize) -> Self {
        self.max_inflight = n;
        self
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
        let semaphore = Arc::new(Semaphore::new(max_inflight));
        let stats = WorkerStats {
            max_inflight,
            task_semaphore: semaphore.clone(),
        };
        let join_handle = tokio::spawn(worker_loop(
            collection,
            stop_rx,
            config.worker_switch_timeout,
            worker_id,
            max_inflight,
            semaphore.clone(),
            handler,
        ));
        WorkerHandle {
            stop_signal: Some(stop_tx),
            join_handle: Some(join_handle),
            stats,
        }
    }
}

#[cfg_attr(
    feature = "tracing",
    tracing::instrument(skip(collection, stop_rx, semaphore, handler))
)]
async fn worker_loop<TInput, TOutput>(
    collection: Collection<Document>,
    mut stop_rx: oneshot::Receiver<()>,
    worker_switch_timeout: Duration,
    worker_id: String,
    max_inflight: usize,
    semaphore: Arc<Semaphore>,
    handler: WorkerHandler<TInput, TOutput>,
) -> Result<(), RequestError>
where
    TInput: DeserializeOwned + Send + 'static,
    TOutput: Serialize + Send + Sync + 'static,
{
    let mut join_set = JoinSet::new();
    let in_flight_ids: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
    let mut claim_ticker = time::interval(switch_maintenance_interval(worker_switch_timeout));
    claim_ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // Consume the immediate first tick so the sweep doesn't fire right after startup
    // (startup already calls pump_available_tasks below).
    claim_ticker.tick().await;
    let mut change_stream = match open_change_stream(&collection).await {
        Ok(stream) => stream,
        Err(err) => {
            #[cfg(feature = "tracing")]
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
        &in_flight_ids,
    )
    .await;

    loop {
        if change_stream.is_none() {
            change_stream = match open_change_stream(&collection).await {
                Ok(stream) => stream,
                Err(err) => {
                    #[cfg(feature = "tracing")]
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
                &in_flight_ids,
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
            _ = claim_ticker.tick() => {
                pump_available_tasks(
                    &collection,
                    &worker_id,
                    worker_switch_timeout,
                    &semaphore,
                    &handler,
                    &mut join_set,
                    &in_flight_ids,
                ).await;
            }
            Some(result) = join_set.join_next(), if !join_set.is_empty() => {
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(_e)) => {
                        #[cfg(feature = "tracing")]
                        error!(error=%_e, "worker task error");
                    }
                    Err(_e) => {
                        #[cfg(feature = "tracing")]
                        error!(error=%_e, "worker task panicked");
                    }
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
                            &in_flight_ids,
                        ).await;
                    }
                    Some(Err(_e)) => {
                        #[cfg(feature = "tracing")]
                        warn!(error=%_e, "change stream error, will restart");
                        change_stream = None;
                    }
                    None => {
                        #[cfg(feature = "tracing")]
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
            Ok(Err(_e)) => {
                #[cfg(feature = "tracing")]
                error!(error=%_e, "worker task error");
            }
            Err(_e) => {
                #[cfg(feature = "tracing")]
                error!(error=%_e, "worker task panicked");
            }
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

#[cfg_attr(feature = "tracing", tracing::instrument(skip(collection, excluded_ids)))]
async fn claim_next_task(
    collection: &Collection<Document>,
    worker_id: &str,
    worker_switch_timeout: Duration,
    excluded_ids: &[String],
) -> Result<Option<Document>, RequestError> {
    let now = DateTime::now();
    let default_switch_timeout_ms = worker_switch_timeout.as_millis() as i64;
    let excluded_bson: Vec<Bson> = excluded_ids
        .iter()
        .map(|id| Bson::String(id.clone()))
        .collect();
    let filter = doc! {
        "$or": [
            {"status": "pending"},
            {
                "status": "running",
                "worker_state.heartbeat_at": {"$type": "date"},
                "$expr": {
                    "$lte": [
                        {
                            "$add": [
                                "$worker_state.heartbeat_at",
                                {
                                    "$ifNull": [
                                        "$worker_switch_timeout",
                                        {
                                            "$ifNull": [
                                                "$worker_state.switch_timeout_ms",
                                                default_switch_timeout_ms,
                                            ]
                                        },
                                    ]
                                },
                            ]
                        },
                        now,
                    ]
                },
            },
            // Immediately reclaim tasks from a previous crash of this worker. The
            // excluded_ids set prevents re-claiming tasks already running in this process.
            {
                "status": "running",
                "worker_state.worker_id": worker_id,
                "task_id": {"$nin": excluded_bson},
            },
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
    collection
        .find_one_and_update(filter, update)
        .return_document(ReturnDocument::After)
        .await
        .map_err(|e| RequestError::Database(e.to_string()))
}

async fn open_change_stream(
    collection: &Collection<Document>,
) -> Result<Option<ChangeStream<ChangeStreamEvent<Document>>>, RequestError> {
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
    match collection
        .watch()
        .pipeline(pipeline)
        .full_document(FullDocumentType::UpdateLookup)
        .await
    {
        Ok(stream) => Ok(Some(stream)),
        Err(e) => {
            if is_change_stream_unsupported(&e) {
                return Err(RequestError::Database(
                    "change streams unsupported; MongoDB must be a replica set or sharded cluster"
                        .into(),
                ));
            }
            #[cfg(feature = "tracing")]
            warn!(error=%e, "failed to open change stream");
            Ok(None)
        }
    }
}

#[cfg_attr(
    feature = "tracing",
    tracing::instrument(skip(collection, semaphore, handler, join_set, in_flight_ids))
)]
async fn pump_available_tasks<TInput, TOutput>(
    collection: &Collection<Document>,
    worker_id: &str,
    worker_switch_timeout: Duration,
    semaphore: &Arc<Semaphore>,
    handler: &WorkerHandler<TInput, TOutput>,
    join_set: &mut JoinSet<Result<(), RequestError>>,
    in_flight_ids: &Arc<Mutex<HashSet<String>>>,
) where
    TInput: DeserializeOwned + Send + 'static,
    TOutput: Serialize + Send + Sync + 'static,
{
    loop {
        let Ok(permit) = semaphore.clone().try_acquire_owned() else {
            break;
        };
        let excluded: Vec<String> = in_flight_ids.lock().unwrap().iter().cloned().collect();
        match claim_next_task(collection, worker_id, worker_switch_timeout, &excluded).await {
            Ok(Some(task)) => {
                let task_id = task
                    .get_str("task_id")
                    .unwrap_or_default()
                    .to_string();
                #[cfg(feature = "tracing")]
                info!(%worker_id, %task_id, "claimed task");
                in_flight_ids.lock().unwrap().insert(task_id.clone());
                let handler = handler.clone();
                join_set.spawn(process_task(
                    collection.clone(),
                    task,
                    worker_id.to_string(),
                    handler,
                    permit,
                    worker_switch_timeout,
                    task_id,
                    in_flight_ids.clone(),
                ));
            }
            Ok(None) => {
                drop(permit);
                break;
            }
            Err(_e) => {
                #[cfg(feature = "tracing")]
                error!(error=%_e, "failed to claim task");
                drop(permit);
                break;
            }
        }
    }
}

#[cfg_attr(
    feature = "tracing",
    tracing::instrument(
        skip(collection, doc, handler, permit),
        fields(task_id, worker_id, trace_id)
    )
)]
async fn process_task<TInput, TOutput>(
    collection: Collection<Document>,
    doc: Document,
    worker_id: String,
    handler: WorkerHandler<TInput, TOutput>,
    permit: OwnedSemaphorePermit,
    worker_switch_timeout: Duration,
    inflight_task_id: String,
    in_flight_ids: Arc<Mutex<HashSet<String>>>,
) -> Result<(), RequestError>
where
    TInput: DeserializeOwned + Send + 'static,
    TOutput: Serialize + Send + Sync + 'static,
{
    let _permit = permit;
    let _in_flight_guard = InFlightGuard {
        task_id: inflight_task_id,
        in_flight_ids,
    };
    #[cfg(feature = "tracing")]
    tracing::Span::current().record("worker_id", &worker_id);

    let task_id = match doc
        .get_str("task_id")
        .map_err(|_| RequestError::PayloadFormat { field: "task_id" })
    {
        Ok(id) => id.to_string(),
        Err(e) => return Err(e), // Can't mark failed without ID
    };
    #[cfg(feature = "tracing")]
    tracing::Span::current().record("task_id", &task_id);

    #[cfg(feature = "tracing")]
    type SetupResult<T> = Result<(T, Option<TraceContext>), RequestError>;
    #[cfg(not(feature = "tracing"))]
    type SetupResult<T> = Result<(T, Option<()>), RequestError>;

    let setup_result: SetupResult<TInput> = async {
        let payload_bson = doc
            .get("task_input")
            .ok_or(RequestError::PayloadFormat {
                field: "task_input",
            })?
            .clone();
        let payload: TInput =
            bson::deserialize_from_bson(payload_bson).map_err(|_| RequestError::PayloadFormat {
                field: "task_input",
            })?;

        #[cfg(feature = "tracing")]
        let trace_context = doc.get("trace_context").and_then(|raw| {
            if raw == &Bson::Null {
                return None;
            }
            let ctx: TraceContext = bson::deserialize_from_bson(raw.clone()).ok()?;
            tracing::Span::current().record("trace_id", ctx.trace_id.as_str());
            Some(ctx)
        });
        #[cfg(not(feature = "tracing"))]
        let trace_context = None;

        if !heartbeat(&collection, &task_id, &worker_id).await? {
            #[cfg(feature = "tracing")]
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

    let job = WorkerJob {
        task_id: task_id.clone(),
        trace_context: trace_context.clone(),
        payload,
    };
    let task_switch_timeout = task_switch_timeout(&doc, worker_switch_timeout);
    let (hb_stop_tx, hb_stop_rx) = oneshot::channel();
    start_heartbeat_loop(
        collection.clone(),
        task_id.clone(),
        worker_id.clone(),
        task_switch_timeout,
        hb_stop_rx,
    );

    #[cfg(feature = "tracing")]
    let handler_span = tracing::info_span!(
        "worker.handler",
        %task_id,
        %worker_id,
    );

    #[cfg(feature = "tracing")]
    if let Some(ref caller_context) = trace_context {
        if let Some(span_context) = caller_context.to_span_context() {
            handler_span.add_link(span_context);
            info!(
                %task_id,
                target_trace_id = %caller_context.trace_id,
                target_span_id = %caller_context.span_id,
                "linked worker span to caller span"
            );
        } else {
            warn!(%task_id, "trace context present but invalid");
        }
    }

    #[cfg(feature = "tracing")]
    let handler_result = {
        let _guard = handler_span.enter();
        handler(job).await
    };
    #[cfg(not(feature = "tracing"))]
    let handler_result = handler(job).await;

    let _ = hb_stop_tx.send(());
    match handler_result {
        Ok(output) => {
            let output_bson: Bson =
                bson::serialize_to_bson(&output).map_err(|_| RequestError::PayloadFormat {
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
                .update_one(filter, update)
                .await
                .map_err(|e| RequestError::Database(e.to_string()))?;
            if result.matched_count == 0 {
                #[cfg(feature = "tracing")]
                error!(%worker_id, %task_id, "lost ownership before completing");
                return Err(RequestError::WorkerGone);
            }
            #[cfg(feature = "tracing")]
            info!(%worker_id, %task_id, "completed task");
            Ok(())
        }
        Err(err) => {
            mark_task_failed(&collection, &task_id, &format!("handler error: {err}")).await;
            Err(err)
        }
    }
}

#[cfg_attr(feature = "tracing", tracing::instrument(skip(collection)))]
async fn mark_task_failed(collection: &Collection<Document>, task_id: &str, reason: &str) {
    let update = doc! {
        "$set": {
            "status": "failed",
            "error_reason": reason,
            "updated_at": DateTime::now(),
            "worker_state.finished_at": DateTime::now(),
        }
    };
    if let Err(_e) = collection
        .update_one(doc! {"task_id": task_id}, update)
        .await
    {
        #[cfg(feature = "tracing")]
        error!(task_id=%task_id, error=%_e, "failed to mark task as failed");
    }
}

#[cfg_attr(feature = "tracing", tracing::instrument(skip(collection)))]
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
        .update_one(filter, update)
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
    let interval = switch_maintenance_interval(worker_switch_timeout);
    tokio::spawn(async move {
        let mut ticker = time::interval(interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = &mut stop_rx => {
                    break;
                }
                _ = ticker.tick() => {
                    if let Err(_e) = heartbeat(&collection, &task_id, &worker_id).await {
                        #[cfg(feature = "tracing")]
                        error!(%worker_id, %task_id, error=%_e, "heartbeat failed");
                        break;
                    }
                }
            }
        }
    });
}

fn task_switch_timeout(doc: &Document, fallback: Duration) -> Duration {
    doc.get_i64("worker_switch_timeout")
        .or_else(|_| {
            doc.get_document("worker_state")
                .and_then(|worker_state| worker_state.get_i64("switch_timeout_ms"))
        })
        .ok()
        .and_then(|millis| u64::try_from(millis).ok())
        .map(Duration::from_millis)
        .unwrap_or(fallback)
}

fn switch_maintenance_interval(worker_switch_timeout: Duration) -> TokioDuration {
    let third = worker_switch_timeout / 3;
    third.clamp(
        TokioDuration::from_millis(50),
        TokioDuration::from_millis(500),
    )
}

struct InFlightGuard {
    task_id: String,
    in_flight_ids: Arc<Mutex<HashSet<String>>>,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.in_flight_ids.lock().unwrap().remove(&self.task_id);
    }
}

/// Guard returned by `Worker::run` so callers can trigger graceful shutdowns
/// (and automatically abort the worker task on drop).
pub struct WorkerHandle {
    stop_signal: Option<oneshot::Sender<()>>,
    join_handle: Option<JoinHandle<Result<(), RequestError>>>,
    stats: WorkerStats,
}

/// Cheap shared stats view for a running worker.
#[derive(Clone)]
pub struct WorkerStats {
    max_inflight: usize,
    task_semaphore: Arc<Semaphore>,
}

impl std::fmt::Debug for WorkerStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerStats")
            .field("max_inflight", &self.max_inflight)
            .field("task_semaphore", &self.task_semaphore.available_permits())
            .finish()
    }
}

impl std::fmt::Debug for WorkerHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerHandle")
            .field("stop_signal", &self.stop_signal)
            .field("join_handle", &self.join_handle.as_ref().map(|_| "..."))
            .field("stats", &self.stats)
            .finish()
    }
}

impl WorkerHandle {
    /// Returns a cloneable stats view for metrics and observability.
    pub fn stats(&self) -> WorkerStats {
        self.stats.clone()
    }

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
            handle
                .await
                .unwrap_or_else(|_| Err(RequestError::WorkerGone))
        } else {
            Ok(())
        }
    }

    /// Get the current count of running tasks.
    pub fn get_running_task_cnt(&self) -> usize {
        self.stats.get_running_task_cnt()
    }

    /// Get the maximum number of inflight tasks.
    pub fn get_max_inflight(&self) -> usize {
        self.stats.get_max_inflight()
    }
}

impl WorkerStats {
    /// Get the current count of running tasks.
    pub fn get_running_task_cnt(&self) -> usize {
        self.max_inflight - self.task_semaphore.available_permits()
    }

    /// Get the maximum number of inflight tasks.
    pub fn get_max_inflight(&self) -> usize {
        self.max_inflight
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
