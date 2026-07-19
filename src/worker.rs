mod expiry;

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::future::{BoxFuture, Either, FutureExt};

type WorkerHandler<TInput, TOutput> = Arc<
    dyn Fn(WorkerJob<TInput>) -> BoxFuture<'static, Result<TOutput, RequestError>> + Send + Sync,
>;
use bson::{Bson, DateTime, Document, doc, oid::ObjectId};
use futures_util::stream::StreamExt;
use mongodb::Collection;
use mongodb::change_stream::{ChangeStream, event::ChangeStreamEvent};
use mongodb::error::{CommandError, ErrorKind};
use mongodb::options::ReturnDocument;
#[cfg(feature = "tracing")]
use opentelemetry::KeyValue;
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
#[cfg(feature = "tracing")]
use crate::metrics::WorkerMetrics;
use crate::storage::connect_collection;
#[cfg(feature = "tracing")]
use crate::trace::TraceContext;
use expiry::{
    ExpiryTracker, apply_change_event_to_expirations, refresh_pending_tasks,
    refresh_running_expirations, schedule_expiration_from_task,
};

// Metrics are entirely optional (Config::enable_metrics) and require the
// `tracing` feature for the opentelemetry dependency they're built on. This
// alias lets `Option<WorkerMetricsHandle>` be threaded through the same way
// regardless of feature flags, mirroring how `WorkerJob::trace_context` swaps
// between `Option<TraceContext>` and `Option<()>` above.
#[cfg(feature = "tracing")]
type WorkerMetricsHandle = WorkerMetrics;
#[cfg(not(feature = "tracing"))]
type WorkerMetricsHandle = ();

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
        #[cfg(feature = "tracing")]
        let metrics: Option<WorkerMetricsHandle> = config.enable_metrics.then(WorkerMetrics::new);
        #[cfg(not(feature = "tracing"))]
        let metrics: Option<WorkerMetricsHandle> = None;
        let join_handle = tokio::spawn(worker_loop(
            collection,
            stop_rx,
            config.worker_switch_timeout,
            worker_id,
            semaphore.clone(),
            handler,
            metrics,
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
    tracing::instrument(skip(collection, stop_rx, semaphore, handler, metrics))
)]
async fn worker_loop<TInput, TOutput>(
    collection: Collection<Document>,
    mut stop_rx: oneshot::Receiver<()>,
    worker_switch_timeout: Duration,
    worker_id: String,
    semaphore: Arc<Semaphore>,
    handler: WorkerHandler<TInput, TOutput>,
    metrics: Option<WorkerMetricsHandle>,
) -> Result<(), RequestError>
where
    TInput: DeserializeOwned + Send + 'static,
    TOutput: Serialize + Send + Sync + 'static,
{
    let mut join_set = JoinSet::new();
    let in_flight_ids: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
    let mut expiry_tracker = ExpiryTracker::new();
    let mut stale_recovery_ticker = time::interval(stale_recovery_interval());
    stale_recovery_ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // Consume the immediate first tick: this branch is only a rare fallback
    // detector, not part of the normal change-stream scheduling path.
    stale_recovery_ticker.tick().await;
    let mut change_stream = match open_change_stream(
        &collection,
        #[cfg(feature = "tracing")]
        &metrics,
        #[cfg(feature = "tracing")]
        "startup",
    )
    .await
    {
        Ok(stream) => stream,
        Err(err) => {
            #[cfg(feature = "tracing")]
            error!(error=%err, "change streams unavailable; worker exiting");
            return Err(err);
        }
    };
    refresh_running_expirations(
        &collection,
        worker_switch_timeout,
        &mut expiry_tracker,
        #[cfg(feature = "tracing")]
        &metrics,
        #[cfg(feature = "tracing")]
        "startup",
    )
    .await;
    refresh_pending_tasks(
        &collection,
        &mut expiry_tracker,
        #[cfg(feature = "tracing")]
        &metrics,
        #[cfg(feature = "tracing")]
        "startup",
    )
    .await;

    pump_available_tasks(
        &collection,
        &worker_id,
        worker_switch_timeout,
        &semaphore,
        &handler,
        &mut join_set,
        &in_flight_ids,
        &metrics,
        ClaimMode::Ready,
        &mut expiry_tracker,
    )
    .await;

    loop {
        if change_stream.is_none() {
            change_stream = match open_change_stream(
                &collection,
                #[cfg(feature = "tracing")]
                &metrics,
                #[cfg(feature = "tracing")]
                "reconnect",
            )
            .await
            {
                Ok(stream) => stream,
                Err(err) => {
                    #[cfg(feature = "tracing")]
                    error!(error=%err, "change streams unavailable; worker exiting");
                    return Err(err);
                }
            };
            refresh_running_expirations(
                &collection,
                worker_switch_timeout,
                &mut expiry_tracker,
                #[cfg(feature = "tracing")]
                &metrics,
                #[cfg(feature = "tracing")]
                "reconnect",
            )
            .await;
            refresh_pending_tasks(
                &collection,
                &mut expiry_tracker,
                #[cfg(feature = "tracing")]
                &metrics,
                #[cfg(feature = "tracing")]
                "reconnect",
            )
            .await;
            pump_available_tasks(
                &collection,
                &worker_id,
                worker_switch_timeout,
                &semaphore,
                &handler,
                &mut join_set,
                &in_flight_ids,
                &metrics,
                ClaimMode::Ready,
                &mut expiry_tracker,
            )
            .await;
            continue;
        }
        let expiry_delay = expiry_tracker.next_delay();
        let change_future = change_stream
            .as_mut()
            .map(|stream| Either::Left(stream.next()))
            .unwrap_or_else(|| Either::Right(futures_util::future::pending()));
        tokio::select! {
            _ = &mut stop_rx => break,
            _ = async {
                if let Some(delay) = expiry_delay {
                    time::sleep(delay).await;
                } else {
                    futures_util::future::pending::<()>().await;
                }
            } => {
                pump_expired_tasks(
                    &collection,
                    &worker_id,
                    worker_switch_timeout,
                    &semaphore,
                    &handler,
                    &mut join_set,
                    &in_flight_ids,
                    &metrics,
                    &mut expiry_tracker,
                ).await;
            }
            _ = stale_recovery_ticker.tick() => {
                refresh_running_expirations(
                    &collection,
                    worker_switch_timeout,
                    &mut expiry_tracker,
                    #[cfg(feature = "tracing")]
                    &metrics,
                    #[cfg(feature = "tracing")]
                    "periodic",
                )
                .await;
                refresh_pending_tasks(
                    &collection,
                    &mut expiry_tracker,
                    #[cfg(feature = "tracing")]
                    &metrics,
                    #[cfg(feature = "tracing")]
                    "periodic",
                )
                .await;
                pump_available_tasks(
                    &collection,
                    &worker_id,
                    worker_switch_timeout,
                    &semaphore,
                    &handler,
                    &mut join_set,
                    &in_flight_ids,
                    &metrics,
                    ClaimMode::StaleRecovery,
                    &mut expiry_tracker,
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
                // A permit was just released. Without this, a freed permit sits
                // idle until the next qualifying change-stream event or the
                // 10-minute fallback tick -- even with pending tasks waiting --
                // because none of the other branches above fire on completion.
                // This probe must not be gated on the pending hint: a prior
                // no-task probe can clear that hint before ready work arrives.
                // `claim_next_task` is atomic and safely returns `None` when
                // no ready task exists.
                pump_available_tasks(
                    &collection,
                    &worker_id,
                    worker_switch_timeout,
                    &semaphore,
                    &handler,
                    &mut join_set,
                    &in_flight_ids,
                    &metrics,
                    ClaimMode::Ready,
                    &mut expiry_tracker,
                ).await;
            }
            event = change_future => {
                match event {
                    Some(Ok(event)) => {
                        let ready = apply_change_event_to_expirations(
                            &event,
                            worker_switch_timeout,
                            &mut expiry_tracker,
                        );
                        if ready {
                        pump_available_tasks(
                            &collection,
                            &worker_id,
                            worker_switch_timeout,
                            &semaphore,
                            &handler,
                            &mut join_set,
                            &in_flight_ids,
                            &metrics,
                            ClaimMode::Ready,
                            &mut expiry_tracker,
                        ).await;
                        }
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

#[derive(Clone, Copy, Debug)]
enum ClaimMode {
    Ready,
    StaleRecovery,
}

impl ClaimMode {
    /// Metric `source` tag: which claim path a `claim_next_task`/`claim_batch_size`
    /// observation came from. `claim_expired_task_by_id`'s calls (a different claim
    /// path, but not represented by `ClaimMode`) use "expiry_targeted" directly.
    #[cfg(feature = "tracing")]
    fn metric_source(self) -> &'static str {
        match self {
            ClaimMode::Ready => "ready",
            ClaimMode::StaleRecovery => "stale_recovery",
        }
    }
}

#[cfg_attr(
    feature = "tracing",
    tracing::instrument(skip(collection, excluded_ids))
)]
async fn claim_next_task(
    collection: &Collection<Document>,
    worker_id: &str,
    worker_switch_timeout: Duration,
    excluded_ids: &[String],
    mode: ClaimMode,
) -> Result<Option<Document>, RequestError> {
    let now = DateTime::now();
    let excluded_bson: Vec<Bson> = excluded_ids
        .iter()
        .map(|id| Bson::String(id.clone()))
        .collect();
    let claim_token = Uuid::new_v4().to_string();
    let claim_started_at = DateTime::now();
    let claim_filter_attempts = match mode {
        ClaimMode::Ready => vec![
            doc! {"status": "pending"},
            // Immediately reclaim tasks from a previous crash of this worker. The
            // excluded_ids set prevents re-claiming tasks already running in this process.
            doc! {
                "status": "running",
                "worker_state.worker_id": worker_id,
                "task_id": {"$nin": excluded_bson},
            },
        ],
        ClaimMode::StaleRecovery => {
            vec![stale_claim_filter(
                now,
                worker_switch_timeout,
                excluded_bson,
            )]
        }
    };
    let update = claim_update(
        worker_id,
        &claim_token,
        claim_started_at,
        worker_switch_timeout,
    );

    for filter in claim_filter_attempts {
        let task = claim_with_filter(collection, filter, update.clone()).await?;
        if task.is_some() {
            return Ok(task);
        }
    }

    Ok(None)
}

async fn claim_with_filter(
    collection: &Collection<Document>,
    filter: Document,
    update: Document,
) -> Result<Option<Document>, RequestError> {
    collection
        .find_one_and_update(filter, update)
        .return_document(ReturnDocument::After)
        .await
        .map_err(|e| RequestError::Database(e.to_string()))
}

fn claim_update(
    worker_id: &str,
    claim_token: &str,
    claim_started_at: DateTime,
    worker_switch_timeout: Duration,
) -> Document {
    doc! {
        "$set": {
            "status": "running",
            "updated_at": claim_started_at,
            "worker_state": {
                "worker_id": worker_id,
                // Unique per claim (not per worker), so a superseded claim can be
                // told apart from the one currently holding the task even when
                // both share the same worker_id -- see heartbeat()/mark_task_failed().
                "claim_token": claim_token,
                "started_at": claim_started_at,
                "heartbeat_at": claim_started_at,
                "switch_timeout_ms": worker_switch_timeout.as_millis() as i64,
            }
        }
    }
}

fn stale_claim_filter(
    now: DateTime,
    worker_switch_timeout: Duration,
    excluded_bson: Vec<Bson>,
) -> Document {
    let default_switch_timeout_ms = worker_switch_timeout.as_millis() as i64;
    doc! {
        "status": "running",
        "worker_state.heartbeat_at": {"$type": "date"},
        "task_id": {"$nin": excluded_bson},
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
    }
}

async fn open_change_stream(
    collection: &Collection<Document>,
    #[cfg(feature = "tracing")] metrics: &Option<WorkerMetricsHandle>,
    #[cfg(feature = "tracing")] trigger: &'static str,
) -> Result<Option<ChangeStream<ChangeStreamEvent<Document>>>, RequestError> {
    let pipeline = expiry_change_stream_pipeline();
    #[cfg(feature = "tracing")]
    let started_at = std::time::Instant::now();
    let result = collection.watch().pipeline(pipeline).await;
    #[cfg(feature = "tracing")]
    if let Some(m) = metrics.as_ref() {
        m.db_operation_duration_ms.record(
            started_at.elapsed().as_secs_f64() * 1000.0,
            &[
                KeyValue::new("operation", "change_stream_open"),
                KeyValue::new("trigger", trigger),
            ],
        );
    }
    match result {
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

fn expiry_change_stream_pipeline() -> Vec<Document> {
    // `updateDescription.updatedFields` is a document keyed by the *literal*
    // updated field names. A heartbeat `$set` therefore appears as
    // `"worker_state.heartbeat_at"`, not as a nested `worker_state` document.
    // `$getField` is required here: ordinary dotted-path matching silently
    // misses that key, leaving the local expiry tracker on the old deadline.
    let dotted_update_field_exists = |field: &str| {
        doc! {
            "$ne": [
                {
                    "$type": {
                        "$getField": {
                            "input": "$updateDescription.updatedFields",
                            "field": field,
                        }
                    }
                },
                "missing"
            ]
        }
    };
    vec![doc! {
        "$match": {
            "$or": [
                { "operationType": { "$in": ["insert", "replace", "delete"] } },
                {
                    "operationType": "update",
                    "$or": [
                        { "updateDescription.updatedFields.status": { "$exists": true } },
                        { "updateDescription.updatedFields.worker_state": { "$exists": true } },
                        { "$expr": dotted_update_field_exists("worker_state.heartbeat_at") },
                        { "$expr": dotted_update_field_exists("worker_state.switch_timeout_ms") }
                    ]
                }
            ]
        }
    }]
}

#[cfg_attr(
    feature = "tracing",
    tracing::instrument(skip(collection, semaphore, handler, join_set, in_flight_ids, metrics))
)]
async fn pump_available_tasks<TInput, TOutput>(
    collection: &Collection<Document>,
    worker_id: &str,
    worker_switch_timeout: Duration,
    semaphore: &Arc<Semaphore>,
    handler: &WorkerHandler<TInput, TOutput>,
    join_set: &mut JoinSet<Result<(), RequestError>>,
    in_flight_ids: &Arc<Mutex<HashSet<String>>>,
    metrics: &Option<WorkerMetricsHandle>,
    claim_mode: ClaimMode,
    expiry_tracker: &mut ExpiryTracker,
) where
    TInput: DeserializeOwned + Send + 'static,
    TOutput: Serialize + Send + Sync + 'static,
{
    #[cfg(feature = "tracing")]
    let mut claimed_count: u64 = 0;
    #[cfg(feature = "tracing")]
    let mut attempted = false;
    loop {
        let Ok(permit) = semaphore.clone().try_acquire_owned() else {
            break;
        };
        let excluded: Vec<String> = in_flight_ids.lock().unwrap().iter().cloned().collect();
        #[cfg(feature = "tracing")]
        {
            attempted = true;
        }
        #[cfg(feature = "tracing")]
        let claim_started_at = std::time::Instant::now();
        let claim_result = claim_next_task(
            collection,
            worker_id,
            worker_switch_timeout,
            &excluded,
            claim_mode,
        )
        .await;
        #[cfg(feature = "tracing")]
        if let Some(m) = metrics.as_ref() {
            m.db_operation_duration_ms.record(
                claim_started_at.elapsed().as_secs_f64() * 1000.0,
                &[
                    KeyValue::new("operation", "claim"),
                    KeyValue::new("source", claim_mode.metric_source()),
                ],
            );
        }
        match claim_result {
            Ok(Some(task)) => {
                #[cfg(feature = "tracing")]
                {
                    claimed_count += 1;
                }
                schedule_expiration_from_task(&task, worker_switch_timeout, expiry_tracker);
                let task_id = task.get_str("task_id").unwrap_or_default().to_string();
                #[cfg(feature = "tracing")]
                if matches!(claim_mode, ClaimMode::StaleRecovery) {
                    error!(
                        %worker_id,
                        %task_id,
                        "fallback stale claim detected abandoned running task; normal change-stream path did not complete it"
                    );
                }
                #[cfg(feature = "tracing")]
                info!(%worker_id, %task_id, "claimed task");
                spawn_claimed_task(
                    collection,
                    worker_id,
                    worker_switch_timeout,
                    handler,
                    join_set,
                    in_flight_ids,
                    metrics,
                    permit,
                    task,
                    task_id,
                );
            }
            Ok(None) => {
                if matches!(claim_mode, ClaimMode::Ready) {
                    // No ready task exists at the point of this atomic claim,
                    // so avoid another idle completion probe until a pending
                    // change event or periodic existence check re-arms it.
                    expiry_tracker.set_pending_may_exist(false);
                }
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
    #[cfg(feature = "tracing")]
    if attempted && let Some(m) = metrics.as_ref() {
        m.claim_batch_size.record(
            claimed_count,
            &[KeyValue::new("source", claim_mode.metric_source())],
        );
    }
}

async fn pump_expired_tasks<TInput, TOutput>(
    collection: &Collection<Document>,
    worker_id: &str,
    worker_switch_timeout: Duration,
    semaphore: &Arc<Semaphore>,
    handler: &WorkerHandler<TInput, TOutput>,
    join_set: &mut JoinSet<Result<(), RequestError>>,
    in_flight_ids: &Arc<Mutex<HashSet<String>>>,
    metrics: &Option<WorkerMetricsHandle>,
    expiry_tracker: &mut ExpiryTracker,
) where
    TInput: DeserializeOwned + Send + 'static,
    TOutput: Serialize + Send + Sync + 'static,
{
    #[cfg(feature = "tracing")]
    let mut claimed_count: u64 = 0;
    #[cfg(feature = "tracing")]
    let mut attempted = false;
    for (id, expiring_task) in expiry_tracker.pop_due() {
        let Ok(permit) = semaphore.clone().try_acquire_owned() else {
            expiry_tracker.defer(id, expiring_task, TokioDuration::from_secs(1));
            break;
        };
        let excluded: Vec<String> = in_flight_ids.lock().unwrap().iter().cloned().collect();
        #[cfg(feature = "tracing")]
        {
            attempted = true;
        }
        #[cfg(feature = "tracing")]
        let claim_started_at = std::time::Instant::now();
        let claim_result = claim_expired_task_by_id(
            collection,
            id,
            worker_id,
            worker_switch_timeout,
            expiring_task.task_id.as_deref(),
            &excluded,
        )
        .await;
        #[cfg(feature = "tracing")]
        if let Some(m) = metrics.as_ref() {
            m.db_operation_duration_ms.record(
                claim_started_at.elapsed().as_secs_f64() * 1000.0,
                &[
                    KeyValue::new("operation", "claim"),
                    KeyValue::new("source", "expiry_targeted"),
                ],
            );
        }

        match claim_result {
            Ok(Some(task)) => {
                #[cfg(feature = "tracing")]
                {
                    claimed_count += 1;
                }
                schedule_expiration_from_task(&task, worker_switch_timeout, expiry_tracker);
                let task_id = task.get_str("task_id").unwrap_or_default().to_string();
                #[cfg(feature = "tracing")]
                warn!(%worker_id, %task_id, "claimed task after tracked heartbeat expiry");
                spawn_claimed_task(
                    collection,
                    worker_id,
                    worker_switch_timeout,
                    handler,
                    join_set,
                    in_flight_ids,
                    metrics,
                    permit,
                    task,
                    task_id,
                );
            }
            Ok(None) => {
                // A newer heartbeat/status change made this task ineligible after
                // it was popped from the local tracker. Its matching change-stream
                // event will install the newer deadline or remove the entry; do
                // not re-query Mongo against the obsolete deadline.
                drop(permit);
            }
            Err(_e) => {
                #[cfg(feature = "tracing")]
                error!(error=%_e, "failed to claim expired task");
                drop(permit);
            }
        }
    }
    #[cfg(feature = "tracing")]
    if attempted && let Some(m) = metrics.as_ref() {
        m.claim_batch_size.record(
            claimed_count,
            &[KeyValue::new("source", "expiry_targeted")],
        );
    }
}

fn spawn_claimed_task<TInput, TOutput>(
    collection: &Collection<Document>,
    worker_id: &str,
    worker_switch_timeout: Duration,
    handler: &WorkerHandler<TInput, TOutput>,
    join_set: &mut JoinSet<Result<(), RequestError>>,
    in_flight_ids: &Arc<Mutex<HashSet<String>>>,
    metrics: &Option<WorkerMetricsHandle>,
    permit: OwnedSemaphorePermit,
    task: Document,
    task_id: String,
) where
    TInput: DeserializeOwned + Send + 'static,
    TOutput: Serialize + Send + Sync + 'static,
{
    in_flight_ids.lock().unwrap().insert(task_id.clone());
    join_set.spawn(process_task(
        collection.clone(),
        task,
        worker_id.to_string(),
        handler.clone(),
        permit,
        worker_switch_timeout,
        task_id,
        in_flight_ids.clone(),
        metrics.clone(),
    ));
}

async fn claim_expired_task_by_id(
    collection: &Collection<Document>,
    id: ObjectId,
    worker_id: &str,
    worker_switch_timeout: Duration,
    task_id: Option<&str>,
    excluded_ids: &[String],
) -> Result<Option<Document>, RequestError> {
    let now = DateTime::now();
    let excluded_bson: Vec<Bson> = excluded_ids
        .iter()
        .map(|id| Bson::String(id.clone()))
        .collect();
    let mut filter = stale_claim_filter(now, worker_switch_timeout, excluded_bson);
    filter.insert("_id", id);
    if let Some(task_id) = task_id {
        filter.insert("task_id", task_id);
    }

    let claim_token = Uuid::new_v4().to_string();
    let claim_started_at = DateTime::now();
    let update = claim_update(
        worker_id,
        &claim_token,
        claim_started_at,
        worker_switch_timeout,
    );

    claim_with_filter(collection, filter, update).await
}

#[cfg_attr(
    feature = "tracing",
    tracing::instrument(
        skip(collection, doc, handler, permit, metrics),
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
    metrics: Option<WorkerMetricsHandle>,
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

    // Identifies this specific claim, not just this worker: heartbeat/completion/
    // failure writes below must all be scoped to it so a superseded claim (e.g. one
    // this same worker_id re-claimed after a stale heartbeat) can never affect a
    // task another, current claim now owns.
    let claim_token = doc
        .get_document("worker_state")
        .ok()
        .and_then(|ws| ws.get_str("claim_token").ok())
        .unwrap_or_default()
        .to_string();

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

        if !heartbeat(&collection, &task_id, &worker_id, &claim_token).await? {
            #[cfg(feature = "tracing")]
            error!(%worker_id, %task_id, "lost ownership before handler start");
            return Err(RequestError::WorkerGone);
        }
        Ok((payload, trace_context))
    }
    .await;

    let (payload, trace_context) = match setup_result {
        Ok(v) => v,
        Err(RequestError::WorkerGone) => {
            // Ownership is already confirmed lost (the heartbeat check above didn't
            // match this claim_token) -- another claim owns this task now. Marking
            // it "failed" here would stomp whatever that current claim is doing.
            return Err(RequestError::WorkerGone);
        }
        Err(err) => {
            mark_task_failed(
                &collection,
                &task_id,
                &worker_id,
                &claim_token,
                &format!("infrastructure error: {err}"),
                #[cfg(feature = "tracing")]
                &metrics,
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
    // Fires when the heartbeat loop finds this claim_token no longer owns the task
    // (superseded by a newer claim), so the in-flight handler below can be aborted
    // instead of silently racing a claim that already moved on.
    let (lost_ownership_tx, mut lost_ownership_rx) = oneshot::channel::<()>();
    start_heartbeat_loop(
        collection.clone(),
        task_id.clone(),
        worker_id.clone(),
        claim_token.clone(),
        task_switch_timeout,
        hb_stop_rx,
        lost_ownership_tx,
        metrics.clone(),
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

    let handler_future = async {
        #[cfg(feature = "tracing")]
        {
            let _guard = handler_span.enter();
            handler(job).await
        }
        #[cfg(not(feature = "tracing"))]
        {
            handler(job).await
        }
    };
    tokio::pin!(handler_future);

    let handler_result = tokio::select! {
        result = &mut handler_future => result,
        _ = &mut lost_ownership_rx => {
            #[cfg(feature = "tracing")]
            error!(%worker_id, %task_id, "task reclaimed by another claim; aborting in-flight handler");
            // handler_future is dropped here, cancelling whatever it was doing.
            return Err(RequestError::WorkerGone);
        }
    };

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
                "worker_state.claim_token": &claim_token,
            };
            let update = doc! {
                "$set": {
                    "status": "succeeded",
                    "task_output": output_bson,
                    "updated_at": DateTime::now(),
                    "worker_state.finished_at": DateTime::now(),
                }
            };
            #[cfg(feature = "tracing")]
            let completion_started_at = std::time::Instant::now();
            let result = collection
                .update_one(filter, update)
                .await
                .map_err(|e| RequestError::Database(e.to_string()))?;
            #[cfg(feature = "tracing")]
            if let Some(m) = metrics.as_ref() {
                m.db_operation_duration_ms.record(
                    completion_started_at.elapsed().as_secs_f64() * 1000.0,
                    &[
                        KeyValue::new("operation", "task_completion"),
                        KeyValue::new("outcome", "succeeded"),
                    ],
                );
            }
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
            mark_task_failed(
                &collection,
                &task_id,
                &worker_id,
                &claim_token,
                &format!("handler error: {err}"),
                #[cfg(feature = "tracing")]
                &metrics,
            )
            .await;
            Err(err)
        }
    }
}

#[cfg_attr(feature = "tracing", tracing::instrument(skip(collection, metrics)))]
async fn mark_task_failed(
    collection: &Collection<Document>,
    task_id: &str,
    worker_id: &str,
    claim_token: &str,
    reason: &str,
    #[cfg(feature = "tracing")] metrics: &Option<WorkerMetricsHandle>,
) {
    // Scoped to this exact claim: an already-superseded claim must never be able
    // to overwrite a task a newer claim owns, even by marking it "failed".
    let filter = doc! {
        "task_id": task_id,
        "status": "running",
        "worker_state.worker_id": worker_id,
        "worker_state.claim_token": claim_token,
    };
    let update = doc! {
        "$set": {
            "status": "failed",
            "error_reason": reason,
            "updated_at": DateTime::now(),
            "worker_state.finished_at": DateTime::now(),
        }
    };
    #[cfg(feature = "tracing")]
    let started_at = std::time::Instant::now();
    let write_result = collection.update_one(filter, update).await;
    #[cfg(feature = "tracing")]
    if let Some(m) = metrics.as_ref() {
        m.db_operation_duration_ms.record(
            started_at.elapsed().as_secs_f64() * 1000.0,
            &[
                KeyValue::new("operation", "task_completion"),
                KeyValue::new("outcome", "failed"),
            ],
        );
    }
    match write_result {
        Ok(result) if result.matched_count == 0 => {
            // Not an error: this claim was already superseded, so the task is no
            // longer ours to fail. Whoever holds it now is responsible for it.
            #[cfg(feature = "tracing")]
            info!(%worker_id, %task_id, "skipped marking task failed; claim already superseded");
        }
        Ok(_) => {}
        Err(_e) => {
            #[cfg(feature = "tracing")]
            error!(task_id=%task_id, error=%_e, "failed to mark task as failed");
        }
    }
}

#[cfg_attr(feature = "tracing", tracing::instrument(skip(collection)))]
async fn heartbeat(
    collection: &Collection<Document>,
    task_id: &str,
    worker_id: &str,
    claim_token: &str,
) -> Result<bool, RequestError> {
    let filter = doc! {
        "task_id": task_id,
        "status": "running",
        "worker_state.worker_id": worker_id,
        "worker_state.claim_token": claim_token,
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

/// What a single heartbeat attempt tells us about ownership. Kept as an explicit
/// enum (rather than matching `Result<bool, RequestError>` inline) so the
/// distinction between "definitely superseded" and "couldn't confirm either way"
/// is a named, independently testable decision -- not just a comment next to a
/// match arm. Only `Superseded` may ever trigger aborting the running handler;
/// `TransientError` (a network blip, timeout, etc.) must never be treated the
/// same way, since the task may still be legitimately ours.
#[derive(Debug, PartialEq, Eq)]
enum HeartbeatOutcome {
    /// The write landed and matched: this claim still owns the task.
    Alive,
    /// The write landed but matched nothing: this exact claim_token no longer
    /// owns the task (a newer claim replaced it, or it finished/failed). This
    /// is the only outcome that should ever cause the handler to be aborted.
    Superseded,
    /// The write itself failed (network, timeout, transient Mongo error). This
    /// proves nothing about ownership -- treat it as "couldn't confirm", not
    /// as evidence of loss.
    TransientError,
}

fn classify_heartbeat_result(result: Result<bool, RequestError>) -> HeartbeatOutcome {
    match result {
        Ok(true) => HeartbeatOutcome::Alive,
        Ok(false) => HeartbeatOutcome::Superseded,
        Err(_) => HeartbeatOutcome::TransientError,
    }
}

fn start_heartbeat_loop(
    collection: Collection<Document>,
    task_id: String,
    worker_id: String,
    claim_token: String,
    worker_switch_timeout: Duration,
    mut stop_rx: oneshot::Receiver<()>,
    lost_ownership_tx: oneshot::Sender<()>,
    metrics: Option<WorkerMetricsHandle>,
) {
    let interval = heartbeat_interval(worker_switch_timeout);
    tokio::spawn(async move {
        let mut ticker = time::interval(interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = &mut stop_rx => {
                    break;
                }
                _ = ticker.tick() => {
                    #[cfg(feature = "tracing")]
                    let started_at = std::time::Instant::now();
                    let result = heartbeat(
                        &collection,
                        &task_id,
                        &worker_id,
                        &claim_token,
                    ).await;
                    #[cfg(feature = "tracing")]
                    let err_for_log = if let Err(ref e) = result { Some(e.to_string()) } else { None };
                    let outcome = classify_heartbeat_result(result);
                    #[cfg(feature = "tracing")]
                    if let Some(m) = metrics.as_ref() {
                        m.db_operation_duration_ms.record(
                            started_at.elapsed().as_secs_f64() * 1000.0,
                            &[KeyValue::new("operation", "heartbeat")],
                        );
                        let outcome_label = match outcome {
                            HeartbeatOutcome::Alive => "alive",
                            HeartbeatOutcome::Superseded => "superseded",
                            HeartbeatOutcome::TransientError => "transient_error",
                        };
                        m.heartbeat_outcome
                            .add(1, &[KeyValue::new("outcome", outcome_label)]);
                    }
                    match outcome {
                        HeartbeatOutcome::Alive => {}
                        HeartbeatOutcome::Superseded => {
                            // Definitive: tell process_task to abort the handler
                            // rather than let it keep running unsupervised.
                            #[cfg(feature = "tracing")]
                            error!(%worker_id, %task_id, "heartbeat found claim superseded; signaling abort");
                            let _ = lost_ownership_tx.send(());
                            break;
                        }
                        HeartbeatOutcome::TransientError => {
                            // Does NOT signal lost_ownership_tx: a network/DB
                            // hiccup doesn't prove the task was reclaimed. Stop
                            // trying to refresh the lease and let staleness or
                            // the final completion check decide instead.
                            #[cfg(feature = "tracing")]
                            error!(%worker_id, %task_id, error = err_for_log.as_deref().unwrap_or(""), "heartbeat failed");
                            break;
                        }
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

/// Interval between heartbeat ticks for a running task: one-third of the
/// switch timeout, so at least three heartbeats land before the lease expires.
fn heartbeat_interval(worker_switch_timeout: Duration) -> TokioDuration {
    worker_switch_timeout / 3
}

/// Rare fallback detector for abandoned running tasks. Normal task pickup is
/// change-stream driven; this should not participate in steady-state scheduling.
fn stale_recovery_interval() -> TokioDuration {
    TokioDuration::from_secs(10 * 60)
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
            handle.await.unwrap_or(Err(RequestError::WorkerGone))
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

#[cfg(test)]
mod claim_tests {
    use super::*;
    use crate::config::Config;
    use crate::worker::expiry::expiration_update_from_fields;
    use std::time::SystemTime;

    async fn test_collection(name_prefix: &str) -> Collection<Document> {
        let config = Config::builder()
            .mongo_uri("mongodb://localhost:27017")
            .database("getitdone_test")
            .collection(format!("{name_prefix}_{}", Uuid::new_v4()))
            .worker_switch_timeout(Duration::from_millis(50))
            .build();
        connect_collection(&config)
            .await
            .expect("connect collection")
    }

    async fn insert_stale_running_task(
        collection: &Collection<Document>,
        task_id: &str,
        worker_id: &str,
    ) {
        let stale = Bson::DateTime(DateTime::from_system_time(
            SystemTime::now() - Duration::from_secs(60),
        ));
        collection
            .insert_one(doc! {
                "task_id": task_id,
                "status": "running",
                "task_input": {},
                "updated_at": DateTime::now(),
                "worker_state": {
                    "worker_id": worker_id,
                    "started_at": stale.clone(),
                    "heartbeat_at": stale,
                    "switch_timeout_ms": 50i64,
                }
            })
            .await
            .expect("insert stale running task");
    }

    // A worker must never re-claim a task it is currently processing, even if
    // that task's heartbeat looks stale (e.g. the heartbeat write fell behind
    // under load). Otherwise it spawns a second concurrent handler for the
    // same task_id -- the exact double-claim bug this test guards against.
    #[tokio::test]
    async fn stale_heartbeat_does_not_reclaim_own_in_flight_task() {
        let collection = test_collection("claim_self_steal").await;
        let task_id = "in-flight-task";
        let worker_id = "worker-1";
        insert_stale_running_task(&collection, task_id, worker_id).await;

        let excluded = vec![task_id.to_string()];
        let result = claim_next_task(
            &collection,
            worker_id,
            Duration::from_millis(50),
            &excluded,
            ClaimMode::StaleRecovery,
        )
        .await
        .expect("claim_next_task should not error");

        assert!(
            result.is_none(),
            "worker re-claimed a task it already has in flight"
        );

        let _ = collection.drop().await;
    }

    // Sanity check: the same stale task, when NOT in the caller's in-flight
    // set (e.g. a genuinely abandoned task from a dead worker), must still be
    // reclaimable. This confirms the fix only narrows the in-flight case and
    // does not break legitimate stale-task recovery.
    #[tokio::test]
    async fn stale_heartbeat_still_reclaims_when_not_in_flight() {
        let collection = test_collection("claim_legit_steal").await;
        let task_id = "abandoned-task";
        let worker_id = "worker-1";
        insert_stale_running_task(&collection, task_id, worker_id).await;

        let result = claim_next_task(
            &collection,
            worker_id,
            Duration::from_millis(50),
            &[],
            ClaimMode::StaleRecovery,
        )
        .await
        .expect("claim_next_task should not error");

        assert!(
            result.is_some(),
            "worker failed to reclaim a genuinely stale, non-in-flight task"
        );

        let _ = collection.drop().await;
    }

    #[tokio::test]
    async fn ready_mode_does_not_reclaim_abandoned_running_tasks() {
        let collection = test_collection("ready_ignores_stale").await;
        let worker_id = "worker-1";
        insert_stale_running_task(&collection, "legacy-stale-task", "dead-worker").await;

        let result = claim_next_task(
            &collection,
            worker_id,
            Duration::from_millis(50),
            &[],
            ClaimMode::Ready,
        )
        .await
        .expect("claim_next_task should not error");

        assert!(
            result.is_none(),
            "change-stream ready claims must not do stale recovery work"
        );

        let _ = collection.drop().await;
    }

    async fn insert_running_task(
        collection: &Collection<Document>,
        task_id: &str,
        worker_id: &str,
        claim_token: &str,
    ) {
        collection
            .insert_one(doc! {
                "task_id": task_id,
                "status": "running",
                "task_input": {},
                "updated_at": DateTime::now(),
                "worker_state": {
                    "worker_id": worker_id,
                    "claim_token": claim_token,
                    "started_at": DateTime::now(),
                    "heartbeat_at": DateTime::now(),
                    "switch_timeout_ms": 50i64,
                }
            })
            .await
            .expect("insert running task");
    }

    // A stale claim_token (this claim was superseded by a newer one, e.g. after a
    // reclaim) must be rejected even though worker_id still matches -- worker_id
    // alone is not a valid ownership fence, since two claims by the same worker_id
    // are indistinguishable without it.
    #[tokio::test]
    async fn heartbeat_rejects_superseded_claim_token() {
        let collection = test_collection("heartbeat_fencing").await;
        let task_id = "task-a";
        let worker_id = "worker-1";
        insert_running_task(&collection, task_id, worker_id, "current-token").await;

        let stale = heartbeat(&collection, task_id, worker_id, "old-superseded-token")
            .await
            .expect("heartbeat should not error");
        assert!(!stale, "heartbeat matched a superseded claim_token");

        let current = heartbeat(&collection, task_id, worker_id, "current-token")
            .await
            .expect("heartbeat should not error");
        assert!(current, "heartbeat rejected the current claim_token");

        let _ = collection.drop().await;
    }

    // A superseded claim must not be able to overwrite a task a newer claim owns
    // by marking it "failed" -- that would corrupt state for the current owner.
    #[tokio::test]
    async fn mark_task_failed_ignores_superseded_claim_token() {
        let collection = test_collection("mark_failed_fencing").await;
        let task_id = "task-b";
        let worker_id = "worker-1";
        insert_running_task(&collection, task_id, worker_id, "current-token").await;

        mark_task_failed(
            &collection,
            task_id,
            worker_id,
            "old-superseded-token",
            "stale handler error",
            #[cfg(feature = "tracing")]
            &None,
        )
        .await;

        let doc = collection
            .find_one(doc! {"task_id": task_id})
            .await
            .expect("find_one should not error")
            .expect("task should still exist");
        assert_eq!(
            doc.get_str("status").unwrap(),
            "running",
            "superseded claim was able to overwrite the current claim's status"
        );

        let _ = collection.drop().await;
    }

    // Pure logic, no DB needed: the classifier is the single source of truth for
    // "does this outcome permit aborting the handler". Pin all three cases down
    // directly so the Ok(false)-vs-Err distinction can't silently drift.
    #[test]
    fn classify_heartbeat_result_distinguishes_superseded_from_transient_error() {
        assert_eq!(classify_heartbeat_result(Ok(true)), HeartbeatOutcome::Alive);
        assert_eq!(
            classify_heartbeat_result(Ok(false)),
            HeartbeatOutcome::Superseded
        );
        assert_eq!(
            classify_heartbeat_result(Err(RequestError::Database("connection reset".into()))),
            HeartbeatOutcome::TransientError
        );
        assert_eq!(
            classify_heartbeat_result(Err(RequestError::Timeout)),
            HeartbeatOutcome::TransientError
        );
    }

    #[test]
    fn stale_recovery_interval_is_rare_fallback_detector() {
        assert_eq!(stale_recovery_interval(), TokioDuration::from_secs(10 * 60));
    }

    #[test]
    fn expiry_tracker_preserves_task_id_across_heartbeat_updates() {
        let mut tracker = ExpiryTracker::new();
        let id = ObjectId::new();
        let first_heartbeat = DateTime::from_millis(1_000);
        let second_heartbeat = DateTime::from_millis(2_000);

        tracker.upsert(
            id,
            Some("task-a".to_string()),
            first_heartbeat,
            Duration::from_millis(50),
        );
        tracker.upsert(id, None, second_heartbeat, Duration::from_millis(50));

        let tracked = tracker.get(&id).expect("task should be tracked");
        assert_eq!(tracked.task_id.as_deref(), Some("task-a"));
        assert_eq!(tracked.expires_at_ms(), Some(2_050));
    }

    #[test]
    fn expiry_change_stream_includes_deletes_for_tracker_cleanup() {
        let pipeline = expiry_change_stream_pipeline();
        let alternatives = pipeline[0]
            .get_document("$match")
            .expect("match stage")
            .get_array("$or")
            .expect("operation alternatives");
        assert!(alternatives.iter().any(|condition| {
            condition
                .as_document()
                .and_then(|condition| condition.get_document("operationType").ok())
                .and_then(|operation| operation.get_array("$in").ok())
                .is_some_and(|operations| {
                    operations
                        .iter()
                        .any(|operation| operation.as_str() == Some("delete"))
                })
        }));
    }

    #[test]
    fn expiration_update_reads_whole_or_dotted_worker_state() {
        let whole = doc! {
            "worker_state": {
                "heartbeat_at": DateTime::from_millis(1_000),
                "switch_timeout_ms": 25i64,
            }
        };
        let dotted = doc! {
            "worker_state.heartbeat_at": DateTime::from_millis(2_000),
            "worker_state.switch_timeout_ms": 50i64,
        };

        let (whole_heartbeat, whole_timeout) =
            expiration_update_from_fields(&whole, Duration::from_millis(100))
                .expect("whole worker_state should be parsed");
        let (dotted_heartbeat, dotted_timeout) =
            expiration_update_from_fields(&dotted, Duration::from_millis(100))
                .expect("dotted worker_state should be parsed");

        assert_eq!(whole_heartbeat.timestamp_millis(), 1_000);
        assert_eq!(whole_timeout, Duration::from_millis(25));
        assert_eq!(dotted_heartbeat.timestamp_millis(), 2_000);
        assert_eq!(dotted_timeout, Duration::from_millis(50));
    }

    #[test]
    fn expiry_change_stream_pipeline_reads_dotted_heartbeat_update_key_literally() {
        let rendered = format!("{:?}", expiry_change_stream_pipeline());

        assert!(rendered.contains("$getField"));
        assert!(rendered.contains("worker_state.heartbeat_at"));
        assert!(rendered.contains("worker_state.switch_timeout_ms"));
        assert!(!rendered.contains("updateDescription.updatedFields.worker_state.heartbeat_at"));
        assert!(
            !rendered.contains("updateDescription.updatedFields.worker_state.switch_timeout_ms")
        );
    }

    // A network/DB failure (e.g. a timeout reaching Mongo) must surface as an
    // Err from heartbeat(), never as Ok(false). Ok(false) means "the write
    // landed and found a different claim_token" -- a network problem proves
    // neither that nor the opposite, and conflating the two would abort a
    // handler that may still legitimately own its task.
    #[tokio::test]
    async fn heartbeat_reports_unreachable_db_as_transient_error_not_superseded() {
        let config = Config::builder()
            .mongo_uri("mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=200&connectTimeoutMS=200")
            .database("getitdone_test")
            .collection(format!("unreachable_{}", Uuid::new_v4()))
            .worker_switch_timeout(Duration::from_millis(50))
            .build();
        // connect_collection succeeds without a real connection (the driver
        // connects lazily on first operation) -- the failure only surfaces once
        // we actually try to talk to it below.
        let collection = connect_collection(&config)
            .await
            .expect("connect_collection should not eagerly dial the server");

        let result = heartbeat(&collection, "some-task", "worker-1", "some-token").await;
        assert!(
            result.is_err(),
            "expected a transient Err for an unreachable DB, got {result:?}"
        );
        assert_eq!(
            classify_heartbeat_result(result),
            HeartbeatOutcome::TransientError
        );
    }
}
