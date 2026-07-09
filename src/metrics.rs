//! Optional OpenTelemetry metrics for worker database operations, enabled via
//! `Config::enable_metrics`. Uses the global `MeterProvider` -- the same one
//! the `tracing` feature already relies on for spans/logs -- so there is
//! nothing extra to wire up beyond installing an OTel SDK as usual.

use opentelemetry::metrics::{Counter, Histogram};

#[derive(Clone)]
pub(crate) struct WorkerMetrics {
    /// Duration of a single worker database round-trip, in milliseconds. Always
    /// tagged with `operation`: "claim", "heartbeat", "expiry_scan",
    /// "change_stream_open", or "task_completion". Some operations add a second,
    /// operation-specific tag on top of that -- `source` for "claim", `trigger`
    /// for "expiry_scan"/"change_stream_open", `outcome` for "task_completion" --
    /// so always filter by `operation` first when querying this metric.
    pub(crate) db_operation_duration_ms: Histogram<f64>,
    /// Tasks actually claimed in a single claim sweep/drain visit (recorded once
    /// per `pump_available_tasks`/`pump_expired_tasks` call, not per individual
    /// `claim_next_task` attempt). A value of 0 means the visit found nothing --
    /// the direct "unnecessary query" signal -- tagged by `source`.
    pub(crate) claim_batch_size: Histogram<u64>,
    /// Count of heartbeat outcomes, tagged by `outcome`: "alive", "superseded"
    /// (ownership lost -- the leading indicator for the double-claim class of
    /// bug), or "transient_error" (a DB/network failure, not proof of loss).
    pub(crate) heartbeat_outcome: Counter<u64>,
}

impl WorkerMetrics {
    pub(crate) fn new() -> Self {
        let meter = opentelemetry::global::meter("getitdone");
        Self {
            db_operation_duration_ms: meter
                .f64_histogram("getitdone.worker.db_operation.duration_ms")
                .with_description(
                    "Duration of a worker database round-trip, in milliseconds, tagged by operation",
                )
                .build(),
            claim_batch_size: meter
                .u64_histogram("getitdone.worker.claim.batch_size")
                .with_description(
                    "Tasks claimed in a single claim sweep/drain visit, tagged by source",
                )
                .build(),
            heartbeat_outcome: meter
                .u64_counter("getitdone.worker.heartbeat.outcome")
                .with_description(
                    "Count of heartbeat outcomes: alive, superseded (ownership lost), or transient_error",
                )
                .build(),
        }
    }
}
