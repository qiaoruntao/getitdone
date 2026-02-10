mod common;

use getitdone::Caller;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{info, instrument, Instrument};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Ping {
    msg: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Pong {
    msg: String,
}

#[instrument(skip(caller), fields(task_id = %run_id))]
async fn send_one(caller: &Caller, run_id: &str) -> Pong {
    info!(%run_id, "mock caller sending task");
    let res: Pong = caller
        .send::<Ping, Pong>(Ping {
            msg: format!("hello from {run_id}"),
        })
        // Use run_id as task_id so we can query spans by task_id later.
        .with_idempotency_key(run_id.to_string())
        .await
        .expect("task result");
    info!(%run_id, reply = %res.msg, "mock caller got reply");
    res
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let logger_ctx = common::init_tracing(env!("CARGO_BIN_NAME"));

    let run_id = Uuid::new_v4().to_string();
    let config = common::build_config("tasks");

    let caller = Caller::connect(config)
        .await
        .expect("failed to create getitdone caller");

    let span = tracing::info_span!("mock.caller", run_id = %run_id);
    let fut = async {
        let _res = send_one(&caller, &run_id).await;

        // Print a single machine-friendly line for follow-up queries.
        println!("MOCK_RUN_ID={run_id}");
    };
    fut.instrument(span).await;

    tokio::time::sleep(Duration::from_secs(2)).await;
    let _ = logger_ctx.tracer_provider.force_flush();
    logger_ctx.shudown();
}
