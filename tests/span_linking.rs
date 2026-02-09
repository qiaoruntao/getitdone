#![cfg(feature = "tracing")]

use getitdone::TraceContext;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::trace::{SpanData, SpanExporter, SdkTracerProvider};
use std::sync::{Arc, Mutex};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;
use tracing_subscriber::prelude::*;
use futures_util::future::BoxFuture;

#[derive(Clone, Debug, Default)]
struct CollectingExporter {
    spans: Arc<Mutex<Vec<SpanData>>>,
}

impl CollectingExporter {
    fn finished_spans(&self) -> Vec<SpanData> {
        self.spans.lock().unwrap().clone()
    }
}

impl SpanExporter for CollectingExporter {
    fn export(&mut self, batch: Vec<SpanData>) -> BoxFuture<'static, OTelSdkResult> {
        let spans = Arc::clone(&self.spans);
        Box::pin(async move {
            spans.lock().unwrap().extend(batch);
            Ok(())
        })
    }
}

#[test]
fn span_link_is_exported() {
    // This test verifies *OpenTelemetry span links* are attached to exported spans when using
    // `tracing_opentelemetry::OpenTelemetrySpanExt::add_link`.
    //
    // Note: SigNoz stores/rendering of links may differ; this test only checks the OTLP payload.
    let exporter = CollectingExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();

    let tracer = provider.tracer("getitdone/tests/span_linking");
    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    let subscriber = tracing_subscriber::registry().with(telemetry);
    let _guard = tracing::subscriber::set_default(subscriber);

    // Fake caller span context we want to link to.
    let tc = TraceContext::from_parts("00112233445566778899aabbccddeeff", "0011223344556677");
    let linked = tc.to_span_context().expect("valid span context");

    let handler_span = tracing::info_span!("worker.handler.test");
    handler_span.add_link(linked.clone());
    handler_span.in_scope(|| {
        tracing::info!("doing work");
    });
    drop(handler_span);

    provider.force_flush().expect("force_flush");

    let spans = exporter.finished_spans();
    let exported = spans
        .iter()
        .find(|s| s.name.as_ref() == "worker.handler.test")
        .expect("exported span");

    assert!(
        exported
            .links
            .links
            .iter()
            .any(|l| l.span_context.trace_id().to_string() == tc.trace_id
                && l.span_context.span_id().to_string() == tc.span_id),
        "expected exported span to contain the linked SpanContext"
    );
}
