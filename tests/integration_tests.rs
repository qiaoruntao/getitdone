use std::time::Duration;

use getitdone::{Caller, Config, RequestError, TraceContext, Worker, WorkerJob};
use mongodb::{
    Client, Collection,
    bson::{Document, doc},
};
use serde::{Deserialize, Serialize};
use tokio::sync::OnceCell;
use uuid::Uuid;

const MONGO_URI: &str = "mongodb://localhost:27017";

static CLEANUP_DONE: OnceCell<()> = OnceCell::const_new();

async fn global_cleanup() {
    let client = Client::with_uri_str(MONGO_URI)
        .await
        .expect("failed to connect for global cleanup");
    let database = client.database("getitdone_test");
    let collections = database
        .list_collection_names()
        .await
        .expect("failed to list collections");

    for collection_name in collections {
        if collection_name.starts_with("test_tasks_") {
            database
                .collection::<Document>(&collection_name)
                .drop()
                .await
                .expect("failed to drop collection");
        }
    }
}

async fn test_config() -> Config {
    CLEANUP_DONE.get_or_init(global_cleanup).await;

    let collection_name = format!("test_tasks_{}", Uuid::new_v4());
    let config = Config::builder()
        .mongo_uri(MONGO_URI)
        .database("getitdone_test")
        .collection(&collection_name)
        .worker_switch_timeout(Duration::from_secs(2)) // Fast switch for tests
        .build();
    ensure_test_indexes(&config).await;
    config
}

async fn ensure_test_indexes(config: &Config) {
    let client = Client::with_uri_str(&config.mongo_uri)
        .await
        .expect("create test indexes");
    let collection: Collection<Document> = client
        .database(&config.database)
        .collection(&config.collection);
    let index_model = vec![
        mongodb::IndexModel::builder()
            .keys(doc! {"task_id": 1})
            .options(
                mongodb::options::IndexOptions::builder()
                    .unique(true)
                    .build(),
            )
            .build(),
        mongodb::IndexModel::builder()
            .keys(doc! {"status": 1, "updated_at": 1})
            .build(),
        mongodb::IndexModel::builder()
            .keys(doc! {"worker_state.worker_id": 1})
            .build(),
    ];
    let _ = collection.create_indexes(index_model).await;
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct EchoInput {
    msg: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct EchoOutput {
    msg: String,
}

#[tokio::test]
async fn test_happy_path() {
    let config = test_config().await;

    // Start Worker
    let worker = Worker::connect(config.clone()).await.unwrap();
    let worker_handle = worker.run(|job: WorkerJob<EchoInput>| async move {
        Ok(EchoOutput {
            msg: job.payload.msg.chars().rev().collect(),
        })
    });

    // Start Caller
    let caller = Caller::connect(config.clone()).await.unwrap();

    let result: EchoOutput = caller
        .send(EchoInput {
            msg: "hello".into(),
        })
        .await
        .unwrap();

    assert_eq!(result.msg, "olleh");

    worker_handle.shutdown().await;
}

#[tokio::test]
async fn test_dispatch_and_await() {
    let config = test_config().await;

    let worker = Worker::connect(config.clone()).await.unwrap();
    let worker_handle = worker.run(|job: WorkerJob<EchoInput>| async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(EchoOutput {
            msg: job.payload.msg,
        })
    });

    let caller = Caller::connect(config.clone()).await.unwrap();

    // Dispatch
    let task_id = caller
        .dispatch(EchoInput {
            msg: "async".into(),
        })
        .await
        .unwrap();

    // Await separately
    let result: EchoOutput = caller.await_response(task_id).await.unwrap();
    assert_eq!(result.msg, "async");

    worker_handle.shutdown().await;
}

#[tokio::test]
async fn test_request_timeout() {
    let config = test_config().await;

    let worker = Worker::connect(config.clone()).await.unwrap();
    let worker_handle = worker.run(|_: WorkerJob<EchoInput>| async move {
        tokio::time::sleep(Duration::from_secs(2)).await;
        Ok(EchoOutput { msg: "done".into() })
    });

    let caller = Caller::connect(config.clone()).await.unwrap();

    // Send with short timeout
    let result: Result<EchoOutput, RequestError> = caller
        .send(EchoInput { msg: "slow".into() })
        .with_timeout(Duration::from_millis(500))
        .await;

    match result {
        Err(RequestError::Timeout) => (),
        _ => panic!("Expected timeout error, got {:?}", result),
    }

    worker_handle.shutdown().await;
}

#[tokio::test]
async fn test_worker_stealing() {
    // Config with fast switch timeout
    let mut config = test_config().await;
    config.worker_switch_timeout = Duration::from_millis(500);

    // Worker A: claims and dies
    let worker_a = Worker::connect(config.clone()).await.unwrap();
    // Use manual handle drop to simulate crash/hang - we simply won't process it in a loop
    // or we can start a worker that hangs forever, then drop it.

    // Let's spawn a worker that claims and then "hangs" (awaiting a signal that never comes)
    // Actually, simply dropping the worker handle sends a stop signal, but if the task is running...
    // simpler: define a handler that hangs.

    let (tx_a, rx_a) = tokio::sync::oneshot::channel::<()>();
    let tx_a = std::sync::Arc::new(std::sync::Mutex::new(Some(tx_a)));

    let handle_a = worker_a.run(move |_: WorkerJob<EchoInput>| {
        let tx_a = tx_a.clone();
        async move {
            // Signal we started
            if let Some(tx) = tx_a.lock().unwrap().take() {
                let _ = tx.send(());
            }
            // Hang forever
            std::future::pending::<Result<EchoOutput, RequestError>>().await
        }
    });

    let caller = Caller::connect(config.clone()).await.unwrap();
    let task_id = caller
        .dispatch(EchoInput {
            msg: "stolen".into(),
        })
        .await
        .unwrap();

    // Wait for A to claim (rx_a receives)
    let _ = rx_a.await;

    // Now "crash" A by dropping handle.
    // Note: implementation of shutdown sends a stop signal, but if future is pending, it might not exit cleanly
    // or update state. The important part is it stops heartbeating.
    // Our worker loop handles shutdown by stopping heartbeats.
    drop(handle_a);

    // Wait > switch timeout
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Worker B starts
    let worker_b = Worker::connect(config.clone()).await.unwrap();
    let handle_b = worker_b.run(|_: WorkerJob<EchoInput>| async move {
        Ok(EchoOutput {
            msg: "recovered".into(),
        })
    });

    // Await result - B should complete it
    let result: EchoOutput = caller.await_response(task_id).await.unwrap();
    assert_eq!(result.msg, "recovered");

    handle_b.shutdown().await;
}

#[tokio::test]
async fn test_running_peer_steals_after_heartbeat_expires_without_new_task() {
    let mut config = test_config().await;
    config.worker_switch_timeout = Duration::from_millis(300);

    let (claimed_tx, claimed_rx) = tokio::sync::oneshot::channel::<()>();
    let claimed_tx = std::sync::Arc::new(std::sync::Mutex::new(Some(claimed_tx)));

    let worker_a = Worker::connect(config.clone()).await.unwrap();
    let handle_a = worker_a.run({
        let claimed_tx = claimed_tx.clone();
        move |_: WorkerJob<EchoInput>| {
            let claimed_tx = claimed_tx.clone();
            async move {
                if let Some(tx) = claimed_tx.lock().unwrap().take() {
                    let _ = tx.send(());
                }
                std::future::pending::<Result<EchoOutput, RequestError>>().await
            }
        }
    });

    let caller = Caller::connect(config.clone()).await.unwrap();
    let task_id = caller
        .dispatch(EchoInput {
            msg: "active-peer-recovery".into(),
        })
        .await
        .unwrap();
    claimed_rx.await.unwrap();

    let worker_b = Worker::connect(config.clone()).await.unwrap();
    let handle_b = worker_b.run(|_: WorkerJob<EchoInput>| async move {
        Ok(EchoOutput {
            msg: "recovered-by-active-peer".into(),
        })
    });

    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(handle_a);

    let result: EchoOutput =
        tokio::time::timeout(Duration::from_secs(2), caller.await_response(task_id))
            .await
            .expect("active peer did not sweep stale task")
            .unwrap();
    assert_eq!(result.msg, "recovered-by-active-peer");

    handle_b.shutdown().await;
}

#[tokio::test]
async fn test_task_level_worker_switch_timeout_controls_stealing() {
    let mut config = test_config().await;
    config.worker_switch_timeout = Duration::from_secs(2);

    let (claimed_tx, claimed_rx) = tokio::sync::oneshot::channel::<()>();
    let claimed_tx = std::sync::Arc::new(std::sync::Mutex::new(Some(claimed_tx)));

    let worker_a = Worker::connect(config.clone()).await.unwrap();
    let handle_a = worker_a.run({
        let claimed_tx = claimed_tx.clone();
        move |_: WorkerJob<EchoInput>| {
            let claimed_tx = claimed_tx.clone();
            async move {
                if let Some(tx) = claimed_tx.lock().unwrap().take() {
                    let _ = tx.send(());
                }
                std::future::pending::<Result<EchoOutput, RequestError>>().await
            }
        }
    });

    let caller = Caller::connect(config.clone()).await.unwrap();
    let task_id = caller
        .send::<_, EchoOutput>(EchoInput {
            msg: "task-timeout-recovery".into(),
        })
        .with_worker_switch_timeout(Duration::from_millis(250))
        .enqueue_only()
        .await
        .unwrap();
    claimed_rx.await.unwrap();

    let worker_b = Worker::connect(config.clone()).await.unwrap();
    let handle_b = worker_b.run(|_: WorkerJob<EchoInput>| async move {
        Ok(EchoOutput {
            msg: "recovered-using-task-timeout".into(),
        })
    });

    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(handle_a);

    let result: EchoOutput =
        tokio::time::timeout(Duration::from_millis(1500), caller.await_response(task_id))
            .await
            .expect("task-level switch timeout was not honored")
            .unwrap();
    assert_eq!(result.msg, "recovered-using-task-timeout");

    handle_b.shutdown().await;
}

#[tokio::test]
async fn test_task_failure() {
    let config = test_config().await;

    let worker = Worker::connect(config.clone()).await.unwrap();
    let worker_handle = worker.run(|_: WorkerJob<EchoInput>| async move {
        Err::<EchoOutput, _>(RequestError::TaskFailed {
            reason: "oops".into(),
        })
    });

    let caller = Caller::connect(config.clone()).await.unwrap();

    let result: Result<EchoOutput, RequestError> =
        caller.send(EchoInput { msg: "fail".into() }).await;

    match result {
        Err(RequestError::TaskFailed { reason }) => assert!(reason.contains("oops")),
        _ => panic!("Expected TaskFailed, got {:?}", result),
    }

    worker_handle.shutdown().await;
}

#[tokio::test]
async fn test_idempotency() {
    let config = test_config().await;
    let caller = Caller::connect(config.clone()).await.unwrap();

    let input = EchoInput {
        msg: "idempotent".into(),
    };

    // 1. Send with key
    // Actually, if we use enqueue_only, it returns Ok(task_id).

    let result1 = caller
        .send::<_, EchoOutput>(input.clone())
        .with_idempotency_key("my_key")
        .enqueue_only()
        .await;

    assert!(result1.is_ok());

    let result2 = caller
        .send::<_, EchoOutput>(input.clone())
        .with_idempotency_key("my_key")
        .enqueue_only()
        .await;

    match result2 {
        Err(RequestError::Duplicate { .. }) => (),
        _ => panic!("Expected Duplicate error, got {:?}", result2),
    }
}

#[tokio::test]
async fn test_payload_mismatch() {
    let config = test_config().await;

    // Start Worker Expecting EchoInput
    let worker = Worker::connect(config.clone()).await.unwrap();
    let worker_handle =
        worker.run(|_: WorkerJob<EchoInput>| async move { Ok(EchoOutput { msg: "ok".into() }) });

    let caller = Caller::connect(config.clone()).await.unwrap();

    // Send a different struct (but serialized as JSON, it needs to match structure? No, purely mismatch)
    #[derive(Serialize)]
    struct WrongInput {
        number: i32,
    }

    // We need to bypass the type safety of `caller.send` by using a caller instantiated for... wait.
    // `Caller` is generic-less. `send` is generic.
    // So we can send `WrongInput`.

    let result: Result<EchoOutput, RequestError> = caller.send(WrongInput { number: 42 }).await;

    // Worker will fail to deserialize `WrongInput` as `EchoInput` (missing `msg` field).
    // Worker should report PayloadFormat error or TaskFailed?
    // Worker `process_task` => `serde_json::from_value`.

    match result {
        Err(RequestError::TaskFailed { reason }) => {
            // Worker catches deserialize error and marks task failed.
            // "task_input: missing field `msg`"
            assert!(reason.contains("task_input"));
        }
        _ => panic!("Expected TaskFailed due to format, got {:?}", result),
    }

    worker_handle.shutdown().await;
}

#[tokio::test]
async fn test_reset_finished_tasks() {
    let mut config = test_config().await;
    config.allow_reset_finished_tasks = true;
    // note: `ConfigBuilder` has `reset_finished_tasks` helper.
    let config_built = Config::builder()
        .mongo_uri(MONGO_URI)
        .database("getitdone_test")
        .collection(&config.collection)
        .reset_finished_tasks(true)
        .build();

    let worker = Worker::connect(config_built.clone()).await.unwrap();
    let worker_handle = worker.run(|job: WorkerJob<EchoInput>| async move {
        Ok(EchoOutput {
            msg: job.payload.msg,
        })
    });

    let caller = Caller::connect(config_built.clone()).await.unwrap();
    let input = EchoInput {
        msg: "replay".into(),
    };

    // Run once
    let id = "replay_task";
    let _: EchoOutput = caller
        .send(input.clone())
        .with_idempotency_key(id)
        .await
        .unwrap();

    // Now, normally duplicate key would fail.
    // `caller.send` uses `upsert_task`. `upsert_task` checks logic:
    // if allow_reset_finished_tasks { matching id AND (status in [succeeded, failed] OR new) }
    // So if we send again with same ID, it should reset the task to pending and run again.

    let result_2: EchoOutput = caller
        .send(input.clone())
        .with_idempotency_key(id)
        .await
        .unwrap();

    assert_eq!(result_2.msg, "replay");

    worker_handle.shutdown().await;
}

#[tokio::test]
async fn test_tracing_propagation() {
    let config = test_config().await;

    let worker = Worker::connect(config.clone()).await.unwrap();
    let _handle = worker.run(|job: WorkerJob<EchoInput>| async move {
        tracing::info!(
            "Worker processing task with trace context: {:?}",
            job.trace_context
        );
        Ok(EchoOutput {
            msg: job.payload.msg,
        })
    });

    let caller = Caller::connect(config.clone()).await.unwrap();

    // Create a span in the caller
    let span = tracing::info_span!("caller_root_span");
    let _enter = span.enter();

    let result: EchoOutput = caller
        .send(EchoInput {
            msg: "tracing".into(),
        })
        .await
        .unwrap();

    assert_eq!(result.msg, "tracing");
    // Verification is manual via --nocapture output (checking for linked spans)
}

#[tokio::test]
async fn test_worker_links_custom_trace_context() {
    let config = test_config().await;

    let (trace_tx, trace_rx) = tokio::sync::oneshot::channel();
    let trace_tx = std::sync::Arc::new(std::sync::Mutex::new(Some(trace_tx)));
    let worker = Worker::connect(config.clone()).await.unwrap();
    let handle = worker.run({
        let trace_tx = trace_tx.clone();
        move |job: WorkerJob<EchoInput>| {
            let trace_tx = trace_tx.clone();
            async move {
                if let Some(tx) = trace_tx.lock().unwrap().take() {
                    let _ = tx.send(job.trace_context.clone());
                }
                Ok(EchoOutput {
                    msg: job.payload.msg,
                })
            }
        }
    });

    let caller = Caller::connect(config.clone()).await.unwrap();
    let custom_trace =
        TraceContext::from_parts("00112233445566778899aabbccddeeff", "0011223344556677");

    let result: EchoOutput = caller
        .send(EchoInput {
            msg: "trace".into(),
        })
        .with_trace_context(custom_trace.clone())
        .await
        .unwrap();
    assert_eq!(result.msg, "trace");

    let received = trace_rx.await.unwrap().expect("trace context present");
    assert_eq!(received.trace_id, custom_trace.trace_id);
    assert_eq!(received.span_id, custom_trace.span_id);

    handle.shutdown().await;
}

#[tokio::test]
async fn test_worker_task_counts() {
    let config = test_config().await;

    // Use channels to coordinate task start/stop
    let (start_tx, mut start_rx) = tokio::sync::mpsc::channel::<String>(4);
    let (stop_tx, stop_rx) = tokio::sync::mpsc::channel::<String>(4);
    let stop_rx = std::sync::Arc::new(tokio::sync::Mutex::new(stop_rx));

    // Start Worker with a small inflight limit
    let worker = Worker::connect(config.clone())
        .await
        .unwrap()
        .with_max_inflight(4);

    let worker_handle = worker.run({
        let start_tx = start_tx.clone();
        let stop_rx = stop_rx.clone();
        move |job: WorkerJob<EchoInput>| {
            let start_tx = start_tx.clone();
            let stop_rx = stop_rx.clone();
            async move {
                // Signal that we've started this task
                start_tx.send(job.payload.msg.clone()).await.unwrap();

                // Wait for the signal to stop
                let stop_msg = stop_rx.lock().await.recv().await.unwrap();
                assert_eq!(job.payload.msg, stop_msg); // Ensure we stop the correct task

                Ok(EchoOutput { msg: "done".into() })
            }
        }
    });

    assert_eq!(worker_handle.get_max_inflight(), 4);
    assert_eq!(worker_handle.get_running_task_cnt(), 0);

    let caller = Caller::connect(config.clone()).await.unwrap();

    // --- Task 1 ---
    caller
        .dispatch(EchoInput {
            msg: "task1".into(),
        })
        .await
        .unwrap();

    // Wait for worker to pick it up
    assert_eq!(start_rx.recv().await.unwrap(), "task1");
    // Allow some time for the semaphore to be acquired, as it happens before the handler starts
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(worker_handle.get_running_task_cnt(), 1);

    // --- Task 2 ---
    caller
        .dispatch(EchoInput {
            msg: "task2".into(),
        })
        .await
        .unwrap();
    assert_eq!(start_rx.recv().await.unwrap(), "task2");
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(worker_handle.get_running_task_cnt(), 2);

    // --- Complete Task 1 ---
    stop_tx.send("task1".into()).await.unwrap();
    // A short sleep to allow the task to finish and release the semaphore
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(worker_handle.get_running_task_cnt(), 1);

    // --- Complete Task 2 ---
    stop_tx.send("task2".into()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(worker_handle.get_running_task_cnt(), 0);

    worker_handle.shutdown().await;
}
