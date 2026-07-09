use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

use bson::{DateTime, Document, doc, oid::ObjectId};
use futures_util::stream::StreamExt;
use mongodb::Collection;
use mongodb::change_stream::event::{ChangeStreamEvent, OperationType};
use tokio::time::Duration as TokioDuration;
#[cfg(feature = "tracing")]
use tracing::warn;

#[derive(Clone, Debug)]
pub(super) struct ExpiringTask {
    pub(super) task_id: Option<String>,
    expires_at_ms: i64,
}

#[cfg(test)]
impl ExpiringTask {
    pub(super) fn expires_at_ms(&self) -> i64 {
        self.expires_at_ms
    }
}

#[derive(Debug, Default)]
pub(super) struct ExpiryTracker {
    tasks: HashMap<ObjectId, ExpiringTask>,
    deadlines: BTreeMap<i64, HashSet<ObjectId>>,
}

impl ExpiryTracker {
    pub(super) fn new() -> Self {
        Self::default()
    }

    fn clear(&mut self) {
        self.tasks.clear();
        self.deadlines.clear();
    }

    pub(super) fn upsert(
        &mut self,
        id: ObjectId,
        task_id: Option<String>,
        heartbeat_at: DateTime,
        worker_switch_timeout: Duration,
    ) {
        let timeout_ms = i64::try_from(worker_switch_timeout.as_millis()).unwrap_or(i64::MAX);
        let expires_at_ms = heartbeat_at.timestamp_millis().saturating_add(timeout_ms);
        let task_id = task_id.or_else(|| self.tasks.get(&id).and_then(|task| task.task_id.clone()));
        self.remove(&id);
        self.tasks.insert(
            id,
            ExpiringTask {
                task_id,
                expires_at_ms,
            },
        );
        self.deadlines.entry(expires_at_ms).or_default().insert(id);
    }

    pub(super) fn remove(&mut self, id: &ObjectId) {
        if let Some(existing) = self.tasks.remove(id) {
            if let Some(ids) = self.deadlines.get_mut(&existing.expires_at_ms) {
                ids.remove(id);
                if ids.is_empty() {
                    self.deadlines.remove(&existing.expires_at_ms);
                }
            }
        }
    }

    pub(super) fn next_delay(&self) -> Option<TokioDuration> {
        let next_ms = *self.deadlines.keys().next()?;
        let now_ms = DateTime::now().timestamp_millis();
        if next_ms <= now_ms {
            Some(TokioDuration::ZERO)
        } else {
            Some(TokioDuration::from_millis((next_ms - now_ms) as u64))
        }
    }

    pub(super) fn pop_due(&mut self) -> Vec<(ObjectId, ExpiringTask)> {
        let now_ms = DateTime::now().timestamp_millis();
        let due_deadlines: Vec<i64> = self.deadlines.range(..=now_ms).map(|(k, _)| *k).collect();
        let mut due = Vec::new();
        for deadline in due_deadlines {
            if let Some(ids) = self.deadlines.remove(&deadline) {
                for id in ids {
                    if let Some(task) = self.tasks.remove(&id) {
                        due.push((id, task));
                    }
                }
            }
        }
        due
    }

    pub(super) fn defer(&mut self, id: ObjectId, task: ExpiringTask, delay: TokioDuration) {
        let delay_ms = i64::try_from(delay.as_millis()).unwrap_or(i64::MAX);
        let expires_at_ms = DateTime::now().timestamp_millis().saturating_add(delay_ms);
        self.tasks.insert(
            id,
            ExpiringTask {
                expires_at_ms,
                ..task
            },
        );
        self.deadlines.entry(expires_at_ms).or_default().insert(id);
    }

    #[cfg(test)]
    pub(super) fn get(&self, id: &ObjectId) -> Option<&ExpiringTask> {
        self.tasks.get(id)
    }
}

pub(super) async fn refresh_running_expirations(
    collection: &Collection<Document>,
    worker_switch_timeout: Duration,
    expiry_tracker: &mut ExpiryTracker,
    #[cfg(feature = "tracing")] metrics: &Option<super::WorkerMetricsHandle>,
    #[cfg(feature = "tracing")] trigger: &'static str,
) {
    // Full `status: "running"` collection scan; its cost is size-dependent (unlike
    // the other O(1) worker operations), so duration is recorded on every exit
    // path, not just success -- see callers for when each `trigger` fires.
    #[cfg(feature = "tracing")]
    let started_at = std::time::Instant::now();
    #[cfg(feature = "tracing")]
    let record_duration = || {
        if let Some(m) = metrics.as_ref() {
            m.db_operation_duration_ms.record(
                started_at.elapsed().as_secs_f64() * 1000.0,
                &[
                    opentelemetry::KeyValue::new("operation", "expiry_scan"),
                    opentelemetry::KeyValue::new("trigger", trigger),
                ],
            );
        }
    };

    let mut cursor = match collection.find(doc! {"status": "running"}).await {
        Ok(cursor) => cursor,
        Err(_e) => {
            #[cfg(feature = "tracing")]
            {
                record_duration();
                warn!(error=%_e, "failed to scan running tasks for expiry tracking");
            }
            return;
        }
    };

    expiry_tracker.clear();
    while let Some(result) = cursor.next().await {
        match result {
            Ok(task) => schedule_expiration_from_task(&task, worker_switch_timeout, expiry_tracker),
            Err(_e) => {
                #[cfg(feature = "tracing")]
                {
                    record_duration();
                    warn!(error=%_e, "failed to read running task for expiry tracking");
                }
                return;
            }
        }
    }
    #[cfg(feature = "tracing")]
    record_duration();
}

pub(super) fn schedule_expiration_from_task(
    task: &Document,
    worker_switch_timeout: Duration,
    expiry_tracker: &mut ExpiryTracker,
) {
    if task.get_str("status").ok() != Some("running") {
        if let Ok(id) = task.get_object_id("_id") {
            expiry_tracker.remove(&id);
        }
        return;
    }
    let Ok(id) = task.get_object_id("_id") else {
        return;
    };
    let task_id = task.get_str("task_id").ok().map(ToOwned::to_owned);
    let Some(heartbeat_at) = heartbeat_from_task(task) else {
        return;
    };
    expiry_tracker.upsert(
        id,
        task_id,
        heartbeat_at,
        task_switch_timeout(task, worker_switch_timeout),
    );
}

fn heartbeat_from_task(task: &Document) -> Option<DateTime> {
    task.get_document("worker_state")
        .ok()
        .and_then(|worker_state| worker_state.get_datetime("heartbeat_at").ok())
        .copied()
}

pub(super) fn apply_change_event_to_expirations(
    event: &ChangeStreamEvent<Document>,
    worker_switch_timeout: Duration,
    expiry_tracker: &mut ExpiryTracker,
) -> bool {
    match event.operation_type {
        OperationType::Insert | OperationType::Replace => {
            if let Some(task) = event.full_document.as_ref() {
                schedule_expiration_from_task(task, worker_switch_timeout, expiry_tracker);
                return task.get_str("status").ok() == Some("pending");
            }
            false
        }
        OperationType::Update => {
            let id = event
                .document_key
                .as_ref()
                .and_then(|key| key.get_object_id("_id").ok());
            let Some(update) = event.update_description.as_ref() else {
                return false;
            };
            if let Some(id) = id {
                if updated_status_is_not_running(&update.updated_fields) {
                    expiry_tracker.remove(&id);
                } else if let Some((heartbeat_at, timeout)) =
                    expiration_update_from_fields(&update.updated_fields, worker_switch_timeout)
                {
                    expiry_tracker.upsert(id, None, heartbeat_at, timeout);
                }
            }
            update.updated_fields.get_str("status").ok() == Some("pending")
        }
        OperationType::Delete => {
            if let Some(id) = event
                .document_key
                .as_ref()
                .and_then(|key| key.get_object_id("_id").ok())
            {
                expiry_tracker.remove(&id);
            }
            false
        }
        _ => false,
    }
}

fn updated_status_is_not_running(updated_fields: &Document) -> bool {
    updated_fields
        .get_str("status")
        .map(|status| status != "running")
        .unwrap_or(false)
}

pub(super) fn expiration_update_from_fields(
    updated_fields: &Document,
    fallback_timeout: Duration,
) -> Option<(DateTime, Duration)> {
    if let Ok(worker_state) = updated_fields.get_document("worker_state") {
        let heartbeat_at = worker_state.get_datetime("heartbeat_at").ok()?;
        let timeout = worker_state
            .get_i64("switch_timeout_ms")
            .ok()
            .and_then(|millis| u64::try_from(millis).ok())
            .map(Duration::from_millis)
            .unwrap_or(fallback_timeout);
        return Some((*heartbeat_at, timeout));
    }

    let heartbeat_at = updated_fields
        .get_datetime("worker_state.heartbeat_at")
        .ok()?;
    let timeout = updated_fields
        .get_i64("worker_state.switch_timeout_ms")
        .ok()
        .and_then(|millis| u64::try_from(millis).ok())
        .map(Duration::from_millis)
        .unwrap_or(fallback_timeout);
    Some((*heartbeat_at, timeout))
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
