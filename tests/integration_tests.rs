use std::time::Duration;

use getitdone::{Caller, Config, RequestError, Worker, WorkerJob};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

async fn test_config() -> Config {
    let collection_name = format!("test_tasks_{}", Uuid::new_v4());
    Config::builder()
        .mongo_uri("mongodb://localhost:27017")
        .database("getitdone_test")
        .collection(&collection_name)
        .worker_switch_timeout(Duration::from_secs(2)) // Fast switch for tests
        .build()
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
    let worker_handle = Worker::connect(config.clone())
        .await
        .unwrap()
        .run(|job: WorkerJob<EchoInput>| async move {
            Ok(EchoOutput {
                msg: job.payload.msg.chars().rev().collect(),
            })
        });

    // Start Caller
    let caller = Caller::connect(config.clone()).await.unwrap();
    
    let result: EchoOutput = caller
        .send(EchoInput { msg: "hello".into() })
        .await
        .unwrap();

    assert_eq!(result.msg, "olleh");
    
    worker_handle.shutdown().await;
}

#[tokio::test]
async fn test_dispatch_and_await() {
    let config = test_config().await;
    
    let worker_handle = Worker::connect(config.clone())
        .await
        .unwrap()
        .run(|job: WorkerJob<EchoInput>| async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(EchoOutput { msg: job.payload.msg })
        });

    let caller = Caller::connect(config.clone()).await.unwrap();
    
    // Dispatch
    let task_id = caller
        .dispatch(EchoInput { msg: "async".into() })
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
    
    let worker_handle = Worker::connect(config.clone())
        .await
        .unwrap()
        .run(|_: WorkerJob<EchoInput>| async move {
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
    let task_id = caller.dispatch(EchoInput { msg: "stolen".into() }).await.unwrap();
    
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
        Ok(EchoOutput { msg: "recovered".into() })
    });
    
    // Await result - B should complete it
    let result: EchoOutput = caller.await_response(task_id).await.unwrap();
    assert_eq!(result.msg, "recovered");
    
    handle_b.shutdown().await;
}

#[tokio::test]
async fn test_task_failure() {
    let config = test_config().await;
    
    let worker_handle = Worker::connect(config.clone())
        .await
        .unwrap()
        .run(|_: WorkerJob<EchoInput>| async move {
            Err::<EchoOutput, _>(RequestError::TaskFailed { reason: "oops".into() })
        });

    let caller = Caller::connect(config.clone()).await.unwrap();
    
    let result: Result<EchoOutput, RequestError> = caller
        .send(EchoInput { msg: "fail".into() })
        .await;

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

    let input = EchoInput { msg: "idempotent".into() };
    
    // 1. Send with key
    // Actually, if we use enqueue_only, it returns Ok(task_id).
    
    let result1 = caller.send::<_, EchoOutput>(input.clone())
        .with_idempotency_key("my_key")
        .enqueue_only()
        .await;
    
    assert!(result1.is_ok());
    
    let result2 = caller.send::<_, EchoOutput>(input.clone())
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
    let worker_handle = Worker::connect(config.clone())
        .await
        .unwrap()
        .run(|_: WorkerJob<EchoInput>| async move {
            Ok(EchoOutput { msg: "ok".into() })
        });
        
    let caller = Caller::connect(config.clone()).await.unwrap();
    
    // Send a different struct (but serialized as JSON, it needs to match structure? No, purely mismatch)
    #[derive(Serialize)]
    struct WrongInput {
        number: i32, 
    }
    
    // We need to bypass the type safety of `caller.send` by using a caller instantiated for... wait.
    // `Caller` is generic-less. `send` is generic.
    // So we can send `WrongInput`.
    
    let result: Result<EchoOutput, RequestError> = caller
        .send(WrongInput { number: 42 })
        .await;
        
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
        .mongo_uri("mongodb://localhost:27017")
        .database("getitdone_test")
        .collection(&config.collection)
        .reset_finished_tasks(true)
        .build();
    
    let worker_handle = Worker::connect(config_built.clone())
        .await
        .unwrap()
        .run(|job: WorkerJob<EchoInput>| async move {
            Ok(EchoOutput { msg: job.payload.msg })
        });
        
    let caller = Caller::connect(config_built.clone()).await.unwrap();
    let input = EchoInput { msg: "replay".into() };
    
    // Run once
    let id = "replay_task";
    let _: EchoOutput = caller.send(input.clone())
        .with_idempotency_key(id)
        .await
        .unwrap();
        
    // Now, normally duplicate key would fail.
    // But we use `build_with_reset` logic?
    // `caller.send` uses `upsert_task`. `upsert_task` checks logic:
    // if allow_reset_finished_tasks { matching id AND (status in [succeeded, failed] OR new) }
    // So if we send again with same ID, it should reset the task to pending and run again.
    
    let result_2: EchoOutput = caller.send(input.clone())
        .with_idempotency_key(id)
        .await
        .unwrap();
        
    assert_eq!(result_2.msg, "replay");
    
    worker_handle.shutdown().await;
}
