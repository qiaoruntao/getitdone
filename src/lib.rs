mod caller;
mod config;
mod error;
mod storage;
mod trace;
mod worker;



pub use caller::{Caller, SendBuilder};
pub use config::{Config, ConfigBuilder};
pub use error::RequestError;
pub use trace::TraceContext;
pub use worker::{Worker, WorkerHandle, WorkerJob};

/// Minimal tracing bootstrap intended for local examples/tests.
pub fn init_test_tracing(
    service_name: &str,
    otlp_endpoint: Option<&str>,
) -> Result<TestTracingGuard, Box<dyn std::error::Error>> {
    let Some(endpoint) = otlp_endpoint else {
        return init_stdout_tracing(service_name).map(TestTracingGuard::Stdout);
    };

    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::trace::SdkTracerProvider;
    use opentelemetry_sdk::Resource;

    let resource = Resource::builder()
        .with_service_name(service_name.to_string())
        .build();

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint.to_string())
        .build()
        .map_err(Box::<dyn std::error::Error>::from)?;

    let provider = SdkTracerProvider::builder()
        .with_resource(resource)
        .with_batch_exporter(exporter)
        .build();

    let tracer = provider.tracer(service_name.to_string());
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    let guard = init_tracing_opentelemetry::TracingConfig::production()
        .init_subscriber_ext(|registry| {
            use tracing_subscriber::layer::SubscriberExt;
            registry.with(otel_layer)
        })
        .map_err(Box::<dyn std::error::Error>::from)?;

    Ok(TestTracingGuard::Otlp(guard))
}

fn init_stdout_tracing(
    service_name: &str,
) -> Result<opentelemetry_sdk::trace::SdkTracerProvider, Box<dyn std::error::Error>> {
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::propagation::TraceContextPropagator;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

    let exporter = opentelemetry_stdout::SpanExporter::default();
    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_simple_exporter(exporter)
        .build();
    let tracer = provider.tracer(service_name.to_string());

    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    let subscriber = tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .with(telemetry);

    subscriber
        .try_init()
        .map_err(|e| Box::<dyn std::error::Error>::from(e))?;
    Ok(provider)
}

/// Guard returned by [`init_test_tracing`] so exporters live long enough to flush.
pub enum TestTracingGuard {
    Otlp(init_tracing_opentelemetry::Guard),
    Stdout(opentelemetry_sdk::trace::SdkTracerProvider),
}

#[cfg(test)]
mod tests {
    #[test]
    fn stdout_initializer_smoke_test() {
        match crate::init_test_tracing("getitdone-tests", None) {
            Ok(super::TestTracingGuard::Stdout(provider)) => {
                let _ = provider.force_flush();
                let _ = provider.shutdown();
            }
            Ok(super::TestTracingGuard::Otlp(_)) => panic!("expected stdout tracing"),
            Err(e) => panic!("failed to init tracing: {e}"),
        }
    }
}
