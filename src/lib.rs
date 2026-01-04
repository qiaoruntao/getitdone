mod caller;
mod config;
mod error;
mod storage;
mod worker;

pub use caller::{Caller, SendBuilder};
pub use config::{Config, ConfigBuilder};
pub use error::RequestError;
pub use worker::{Worker, WorkerHandle, WorkerJob};

pub fn init_tracing_stdout() -> opentelemetry_sdk::trace::TracerProvider {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use opentelemetry_sdk::propagation::TraceContextPropagator;
    use opentelemetry::trace::TracerProvider as _;

    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

    let exporter = opentelemetry_stdout::SpanExporter::default();
    let provider = opentelemetry_sdk::trace::TracerProvider::builder()
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
