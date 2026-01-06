use serde::{Deserialize, Serialize};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use opentelemetry::trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState};

/// Structured representation of the caller's tracing identifiers.
/// Stored verbatim in Mongo so workers can rehydrate a `SpanContext`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TraceContext {
    /// Hex-encoded OpenTelemetry trace id captured from the caller span.
    pub trace_id: String,
    /// Hex-encoded span id for the caller span that submitted the task.
    pub span_id: String,
    /// Raw trace flags (default: sampled). Stored so sampling decisions stay intact.
    #[serde(default = "TraceContext::default_trace_flags")]
    pub trace_flags: u8,
}

impl TraceContext {
    /// Creates a new trace context from raw identifiers.
    pub fn from_parts(trace_id: impl Into<String>, span_id: impl Into<String>) -> Self {
        TraceContext {
            trace_id: trace_id.into(),
            span_id: span_id.into(),
            trace_flags: TraceContext::default_trace_flags(),
        }
    }

    /// Creates a new trace context with explicit trace flags.
    pub fn from_parts_with_flags(
        trace_id: impl Into<String>,
        span_id: impl Into<String>,
        trace_flags: TraceFlags,
    ) -> Self {
        TraceContext {
            trace_id: trace_id.into(),
            span_id: span_id.into(),
            trace_flags: trace_flags.to_u8(),
        }
    }

    /// Capture the currently active OpenTelemetry span context if available.
    pub fn capture_current() -> Option<Self> {
        let span = tracing::Span::current();
        let otel_context = span.context();
        let span_ref = otel_context.span();
        let span_context = span_ref.span_context();
        Self::from_span_context(span_context)
    }

    /// Build a trace context from a `SpanContext` reference, returning `None` if invalid.
    pub fn from_span_context(span_context: &SpanContext) -> Option<Self> {
        if !span_context.is_valid() {
            return None;
        }
        Some(TraceContext {
            trace_id: span_context.trace_id().to_string(),
            span_id: span_context.span_id().to_string(),
            trace_flags: span_context.trace_flags().to_u8(),
        })
    }

    /// Convert the stored identifiers back into an OpenTelemetry `SpanContext`.
    pub fn to_span_context(&self) -> Option<SpanContext> {
        let Ok(trace_id) = TraceId::from_hex(&self.trace_id) else {
            return None;
        };
        let Ok(span_id) = SpanId::from_hex(&self.span_id) else {
            return None;
        };

        Some(SpanContext::new(
            trace_id,
            span_id,
            TraceFlags::new(self.trace_flags),
            true,
            TraceState::default(),
        ))
    }

    fn default_trace_flags() -> u8 {
        TraceFlags::SAMPLED.to_u8()
    }
}

