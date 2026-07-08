//! Optional OpenTelemetry metrics for worker database operations, enabled via
//! `Config::enable_metrics`. Uses the global `MeterProvider` -- the same one
//! the `tracing` feature already relies on for spans/logs -- so there is
//! nothing extra to wire up beyond installing an OTel SDK as usual.

use opentelemetry::metrics::{Counter, Histogram};

#[derive(Clone)]
pub(crate) struct WorkerMetrics {
    /// Duration of a single `claim_next_task` database round-trip, in milliseconds.
    pub(crate) claim_duration_ms: Histogram<f64>,
    /// Duration of a single heartbeat database write, in milliseconds.
    pub(crate) heartbeat_duration_ms: Histogram<f64>,
    /// Count of heartbeat outcomes, tagged by `outcome`: "alive", "superseded"
    /// (ownership lost -- the leading indicator for the double-claim class of
    /// bug), or "transient_error" (a DB/network failure, not proof of loss).
    pub(crate) heartbeat_outcome: Counter<u64>,
}

impl WorkerMetrics {
    pub(crate) fn new() -> Self {
        let meter = opentelemetry::global::meter("getitdone");
        Self {
            claim_duration_ms: meter
                .f64_histogram("getitdone.worker.claim.duration_ms")
                .with_description(
                    "Duration of a single claim_next_task database round-trip, in milliseconds",
                )
                .build(),
            heartbeat_duration_ms: meter
                .f64_histogram("getitdone.worker.heartbeat.duration_ms")
                .with_description("Duration of a single heartbeat database write, in milliseconds")
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
