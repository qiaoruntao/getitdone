use getitdone::{Caller, Config, RequestError, TraceContext, inspect_task};
use mongodb::Client;
use mongodb::bson::{Document, doc, to_bson};
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use uuid::Uuid;

#[derive(Debug, Clone)]
struct FailingPayload;

impl Serialize for FailingPayload {
    fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        Err(serde::ser::Error::custom("failed"))
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct TestOutput {
    value: String,
}

fn mongo_uri() -> String {
    std::env::var("TEST_MONGO_URI").unwrap_or_else(|_| "mongodb://localhost:27017".into())
}

async fn test_config() -> Config {
    let collection = format!("caller_tests_{}", Uuid::new_v4());
    Config::builder()
        .mongo_uri(&mongo_uri())
        .database("getitdone_caller_tests")
        .collection(&collection)
        .request_timeout(None)
        .worker_switch_timeout(Duration::from_millis(200))
        .build()
}

async fn mongo_available(config: &Config) -> bool {
    match Client::with_uri_str(&config.mongo_uri).await {
        Ok(client) => client.list_database_names(None, None).await.is_ok(),
        Err(_) => false,
    }
}

async fn drop_collection(config: &Config) {
    if let Ok(client) = Client::with_uri_str(&config.mongo_uri).await {
        let _ = client
            .database(&config.database)
            .collection::<Document>(&config.collection)
            .drop(None)
            .await;
    }
}

#[tokio::test]
async fn send_returns_payload_format_error_for_bad_payload() {
    let config = test_config().await;
    if !mongo_available(&config).await {
        eprintln!(
            "skipping send_returns_payload_format_error_for_bad_payload (Mongo unavailable)"
        );
        return;
    }
    let caller = Caller::connect(config.clone()).await.unwrap();
    let err = caller
        .send::<FailingPayload, TestOutput>(FailingPayload)
        .await
        .unwrap_err();
    assert!(matches!(err, RequestError::PayloadFormat { field } if field == "task_input"));
    drop_collection(&config).await;
}

#[tokio::test]
async fn builder_overrides_are_persisted() {
    let config = test_config().await;
    if !mongo_available(&config).await {
        eprintln!("skipping builder_overrides_are_persisted (Mongo unavailable)");
        return;
    }
    let caller = Caller::connect(config.clone()).await.unwrap();
    let trace =
        TraceContext::from_parts("00112233445566778899aabbccddeeff", "0011223344556677");

    let task_id = caller
        .send::<DummyPayload, TestOutput>(DummyPayload {
            value: "trace".into(),
        })
        .with_timeout(Duration::from_secs(1))
        .with_worker_switch_timeout(Duration::from_millis(321))
        .with_trace_context(trace.clone())
        .enqueue_only()
        .await
        .unwrap();

    let client = Client::with_uri_str(&config.mongo_uri).await.unwrap();
    let stored = client
        .database(&config.database)
        .collection::<Document>(&config.collection)
        .find_one(doc! { "task_id": &task_id }, None)
        .await
        .unwrap()
        .expect("task stored");

    assert_eq!(stored.get_i64("worker_switch_timeout").unwrap(), 321);
    let stored_trace = stored.get_document("trace_context").unwrap();
    assert_eq!(stored_trace.get_str("trace_id").unwrap(), trace.trace_id);

    drop_collection(&config).await;
}

#[tokio::test]
async fn inspect_task_handles_success_failure_and_missing() {
    let config = test_config().await;
    if !mongo_available(&config).await {
        eprintln!(
            "skipping inspect_task_handles_success_failure_and_missing (Mongo unavailable)"
        );
        return;
    }
    let caller = Caller::connect(config.clone()).await.unwrap();
    let client = Client::with_uri_str(&config.mongo_uri).await.unwrap();
    let collection = client
        .database(&config.database)
        .collection::<Document>(&config.collection);

    let success_id = "success_case";
    let success_output = TestOutput {
        value: "done".into(),
    };
    collection
        .insert_one(
            doc! {
                "task_id": success_id,
                "status": "succeeded",
                "task_output": to_bson(&success_output).unwrap(),
            },
            None,
        )
        .await
        .unwrap();

    let failure_id = "failure_case";
    collection
        .insert_one(
            doc! {
                "task_id": failure_id,
                "status": "failed",
                "error_reason": "boom",
            },
            None,
        )
        .await
        .unwrap();

    let success = inspect_task::<TestOutput>(caller.collection(), success_id)
        .await
        .unwrap();
    assert_eq!(success.unwrap().unwrap(), success_output);

    let failure = inspect_task::<TestOutput>(caller.collection(), failure_id)
        .await
        .unwrap();
    assert!(matches!(
        failure.unwrap(),
        Err(RequestError::TaskFailed { reason }) if reason.contains("boom")
    ));

    let missing = inspect_task::<TestOutput>(caller.collection(), "missing")
        .await
        .unwrap();
    assert!(missing.unwrap().is_err());

    drop_collection(&config).await;
}

#[derive(Serialize)]
struct DummyPayload {
    value: String,
}
