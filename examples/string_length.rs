use getitdone::{Caller, Config, Worker, WorkerJob};
use mongodb::{Client, bson::doc};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _guard = getitdone::init_tracing_otlp("string-length-example", "http://localhost:4317")?;
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
            info!("Worker processing task with trace_id: {:?}", job.trace_context);
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
        .delete_many(doc! {}, None)
        .await?;
    Ok(())
}
