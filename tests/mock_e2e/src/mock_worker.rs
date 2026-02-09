mod common;

use std::sync::{Arc, Mutex};

use getitdone::{RequestError, Worker, WorkerJob};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tracing::{info, instrument, Instrument};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Ping {
    msg: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Pong {
    msg: String,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let logger_ctx = common::init_tracing(env!("CARGO_BIN_NAME"));

    // Use a stable collection name by default so caller/worker match without extra env.
    let config = common::build_config("tasks");
    info!(
        mongo_uri = %config.mongo_uri,
        database = %config.database,
        collection = %config.collection,
        "mock worker starting"
    );

    let worker = Worker::connect(config)
        .await
        .expect("failed to create getitdone worker");

    let (done_tx, done_rx) = oneshot::channel::<()>();
    let done_tx = Arc::new(Mutex::new(Some(done_tx)));

    let handle = worker.run({
        let done_tx = done_tx.clone();
        move |job: WorkerJob<Ping>| {
            let done_tx = done_tx.clone();
            let span = tracing::info_span!(
                "mock.worker.job",
                task_id = %job.task_id,
                has_trace_context = job.trace_context.is_some(),
            );
            async move { handle_job(job, done_tx).await }.instrument(span)
        }
    });

    // Exit after one successful handler invocation to keep runs deterministic.
    let _ = done_rx.await;
    info!("mock worker shutting down after one job");
    handle.shutdown().await;

    // Give the batch span processor time to ship spans before runtime teardown.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let _ = logger_ctx.tracer_provider.force_flush();
    logger_ctx.shudown();
}

#[instrument(skip(done_tx), fields(task_id = %job.task_id, id = %job.task_id))]
async fn handle_job(
    job: WorkerJob<Ping>,
    done_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,
) -> Result<Pong, RequestError> {
    info!(
        task_id = %job.task_id,
        has_trace_context = job.trace_context.is_some(),
        trace_context = ?job.trace_context,
        payload = ?job.payload,
        "mock worker handling job"
    );

    if let Some(tx) = done_tx.lock().unwrap().take() {
        let _ = tx.send(());
    }

    Ok(Pong { msg: job.payload.msg })
}
