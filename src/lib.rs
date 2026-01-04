mod caller;
mod config;
mod error;
mod storage;
mod worker;

pub use caller::{Caller, SendBuilder};
pub use config::{Config, ConfigBuilder};
pub use error::RequestError;
pub use worker::{Worker, WorkerHandle, WorkerJob};

pub fn init_tracing_otlp(service_name: &str, endpoint: &str) -> Result<init_tracing_opentelemetry::Guard, Box<dyn std::error::Error + Send + Sync>> {
    // Note: set_var is unsafe in recent Rust versions due to potential thread safety issues
    unsafe {
        std::env::set_var("OTEL_SERVICE_NAME", service_name);
        std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", endpoint);
        std::env::set_var("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc");
    }
    
    let config = init_tracing_opentelemetry::TracingConfig::production();
    config.init_subscriber().map_err(|e| e.into())
}

pub fn init_tracing_stdout() -> opentelemetry_sdk::trace::SdkTracerProvider {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use opentelemetry_sdk::propagation::TraceContextPropagator;
    use opentelemetry::trace::TracerProvider as _;

    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

    let exporter = opentelemetry_stdout::SpanExporter::default();
    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_simple_exporter(exporter)
        .build();
    let tracer = provider.tracer("getitdone");
    
    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with(tracing_subscriber::fmt::layer())
        .with(telemetry);

    let _ = subscriber.try_init();
    provider
}
