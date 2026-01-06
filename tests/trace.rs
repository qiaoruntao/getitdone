#![cfg(feature = "tracing")]
use getitdone::TraceContext;
use opentelemetry::trace::{SpanContext, SpanId, TraceFlags, TraceId, TraceState};

fn sample_ids() -> (&'static str, &'static str) {
    ("00112233445566778899aabbccddeeff", "0011223344556677")
}

#[test]
fn from_parts_defaults_to_sampled() {
    let (trace_id, span_id) = sample_ids();
    let ctx = TraceContext::from_parts(trace_id, span_id);
    assert_eq!(ctx.trace_id, trace_id);
    assert_eq!(ctx.span_id, span_id);
    assert_eq!(ctx.trace_flags, TraceFlags::SAMPLED.to_u8());
    let span_context = ctx.to_span_context().expect("valid hex ids");
    assert!(span_context.is_remote());
    assert_eq!(span_context.trace_flags(), TraceFlags::SAMPLED);
}

#[test]
fn from_parts_with_flags_respects_sampling() {
    let (trace_id, span_id) = sample_ids();
    let ctx = TraceContext::from_parts_with_flags(trace_id, span_id, TraceFlags::NOT_SAMPLED);
    assert_eq!(ctx.trace_flags, TraceFlags::NOT_SAMPLED.to_u8());
    let span_context = ctx.to_span_context().expect("valid hex ids");
    assert!(!span_context.trace_flags().is_sampled());
}

#[test]
fn from_span_context_roundtrip() {
    let span_context = SpanContext::new(
        TraceId::from_hex("00112233445566778899aabbccddeeff").unwrap(),
        SpanId::from_hex("0011223344556677").unwrap(),
        TraceFlags::SAMPLED,
        true,
        TraceState::default(),
    );
    let stored = TraceContext::from_span_context(&span_context).expect("valid span");
    let rebuilt = stored.to_span_context().expect("rebuild");
    assert_eq!(rebuilt.trace_id(), span_context.trace_id());
    assert_eq!(rebuilt.span_id(), span_context.span_id());
}
