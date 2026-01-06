use getitdone::{Caller, Config, Worker, WorkerJob};
use mongodb::{Client, bson::doc};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::{Resource, propagation::TraceContextPropagator};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _guard = init_example_tracing("string-length-example", Some("http://localhost:4317"))?;
    let config = Config::builder()
        .mongo_uri("mongodb://localhost:27017")
        .database("Test")
        .collection("getitdone_example")
        .build();

    // Ensure the collection is empty before the demo runs.
    if let Err(err) = cleanup_collection(&config).await {
        warn!(error=%err, "Unable to clean Mongo collection");
        return Ok(());
    }

    info!(mongo_uri=%config.mongo_uri, "Connecting caller and worker");

    // Simulate two separate processes sharing the same MongoDB backend.
    let caller = match Caller::connect(config.clone()).await {
        Ok(caller) => caller,
        Err(err) => {
            warn!(error=%err, "Skipping example because MongoDB is unavailable");
            return Ok(());
        }
    };
    let worker_handle = match Worker::connect(config).await {
        Ok(worker) => worker.run(|job: WorkerJob<LengthRequest>| async move {
            info!(
                "Worker processing task with trace_id: {:?}",
                job.trace_context
            );
            Ok(LengthResponse {
                length: job.payload.text.chars().count(),
            })
        }),
        Err(err) => {
            warn!(error=%err, "Skipping example because worker cannot connect");
            return Ok(());
        }
    };

    // Create a caller span - this becomes the parent span for send operations
    // The worker's process_task span will have the same trace_id, showing the link
    let caller_span = tracing::info_span!("caller_operation");
    let _guard = caller_span.enter();

    let request = LengthRequest {
        text: "hello from example".into(),
    };
    info!("Submitting task ...");
    let response: LengthResponse = match caller.send(request).await {
        Ok(resp) => resp,
        Err(err) => {
            warn!(error=%err, "Skipping example because task failed");
            worker_handle.shutdown().await;
            return Ok(());
        }
    };

    info!(result_length = response.length, "Worker completed task");

    drop(_guard);
    worker_handle.shutdown().await;
    Ok(())
}

#[derive(Serialize, Deserialize)]
struct LengthRequest {
    text: String,
}

#[derive(Serialize, Deserialize)]
struct LengthResponse {
    length: usize,
}

async fn cleanup_collection(config: &Config) -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::with_uri_str(&config.mongo_uri).await?;
    let db = client.database(&config.database);
    db.collection::<mongodb::bson::Document>(&config.collection)
        .delete_many(doc! {})
        .await?;
    Ok(())
}

struct ExampleTracingGuard {
    provider: SdkTracerProvider,
}

impl Drop for ExampleTracingGuard {
    fn drop(&mut self) {
        let _ = self.provider.force_flush();
    }
}

fn init_example_tracing(
    service_name: &str,
    otlp_endpoint: Option<&str>,
) -> Result<ExampleTracingGuard, Box<dyn std::error::Error>> {
    use opentelemetry::global;
    use opentelemetry_otlp::WithExportConfig;
    global::set_text_map_propagator(TraceContextPropagator::new());

    let provider = if let Some(endpoint) = otlp_endpoint {
        let resource = Resource::builder()
            .with_service_name(service_name.to_string())
            .build();

        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint.to_string())
            .build()
            .map_err(Box::<dyn std::error::Error>::from)?;

        SdkTracerProvider::builder()
            .with_resource(resource)
            .with_batch_exporter(exporter)
            .build()
    } else {
        use opentelemetry_stdout::SpanExporter;

        SdkTracerProvider::builder()
            .with_simple_exporter(SpanExporter::default())
            .build()
    };

    global::set_tracer_provider(provider.clone());

    let tracer = provider.tracer(service_name.to_string());
    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    let subscriber = tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .with(telemetry);
    let _ = subscriber.try_init();

    Ok(ExampleTracingGuard { provider })
}
