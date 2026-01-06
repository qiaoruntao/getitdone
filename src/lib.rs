mod caller;
mod config;
mod error;
mod storage;
#[cfg(feature = "tracing")]
mod trace;
mod worker;

pub use caller::{Caller, SendBuilder, inspect_task};
pub use config::{Config, ConfigBuilder};
pub use error::RequestError;
#[cfg(feature = "tracing")]
pub use trace::TraceContext;
pub use worker::{Worker, WorkerHandle, WorkerJob};

/// Minimal tracing bootstrap intended for local examples/tests.
///
/// This helper initializes a global tracing subscriber with an OpenTelemetry layer.
/// If `otlp_endpoint` is provided, it exports spans via OTLP/gRPC.
/// Otherwise, it prints spans to stdout.
#[cfg(feature = "tracing")]
pub fn init_test_tracing(
    service_name: &str,
    otlp_endpoint: Option<&str>,
) -> Result<TestTracingGuard, Box<dyn std::error::Error>> {
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::propagation::TraceContextPropagator;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    // Ensure OTel context is propagated (e.g., via W3C TraceContext)
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

    let provider = if let Some(endpoint) = otlp_endpoint {
        use opentelemetry_otlp::WithExportConfig;
        use opentelemetry_sdk::Resource;
        use opentelemetry_sdk::trace::SdkTracerProvider;

        let resource = Resource::builder()
            .with_service_name(service_name.to_string())
            .build();

        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint.to_string())
            .build()
            .map_err(Box::<dyn std::error::Error>::from)?;

        // Batch processor prevents blocking on connect/send
        SdkTracerProvider::builder()
            .with_resource(resource)
            .with_batch_exporter(exporter)
            .build()
    } else {
        use opentelemetry_sdk::trace::SdkTracerProvider;
        use opentelemetry_stdout::SpanExporter;

        let exporter = SpanExporter::default();
        SdkTracerProvider::builder()
            .with_simple_exporter(exporter)
            .build()
    };

    // Set as global provider so other crates can use global::tracer()
    opentelemetry::global::set_tracer_provider(provider.clone());

    let tracer = provider.tracer(service_name.to_string());
    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);

    let subscriber = tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with(tracing_subscriber::fmt::layer().with_ansi(true))
        .with(telemetry);

    // If init fails (e.g. already set), we ignore it as the goal is to have *something* active.
    let _ = subscriber.try_init();

    Ok(TestTracingGuard { provider })
}

/// Guard returned by [`init_test_tracing`] so exporters live long enough to flush.
/// Dropping this guard will flush and shutdown the tracer provider.
#[cfg(feature = "tracing")]
pub struct TestTracingGuard {
    provider: opentelemetry_sdk::trace::SdkTracerProvider,
}

#[cfg(feature = "tracing")]
impl std::fmt::Debug for TestTracingGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestTracingGuard").finish()
    }
}

#[cfg(feature = "tracing")]
impl Drop for TestTracingGuard {
    fn drop(&mut self) {
        // Explicitly flush before exiting
        let _ = self.provider.force_flush();
    }
}
