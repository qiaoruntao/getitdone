use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

type SendFuture<T> = Pin<Box<dyn Future<Output = Result<T, RequestError>> + Send>>;

use futures_util::StreamExt;
use mongodb::Collection;
use mongodb::bson::{Bson, DateTime, Document, doc};
use mongodb::error::{ErrorKind, WriteError, WriteFailure};
use mongodb::options::{ChangeStreamOptions, FindOneOptions, FullDocumentType, UpdateOptions};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use uuid::Uuid;

use crate::config::Config;
use crate::error::RequestError;
use crate::storage::connect_collection;
use crate::trace::TraceContext;
use tracing::warn;

/// High-level API a caller uses to submit `TaskInput` payloads into the configured
/// Mongo collection and await the matching `TaskOutput`.
#[derive(Clone)]
pub struct Caller {
    pub(crate) config: Config,
    collection: Collection<Document>,
}

impl Caller {
    /// Connects to the Mongo deployment described by `config`. The same config should
    /// later be handed to a worker so both roles talk to the same collection.
    #[tracing::instrument(skip(config))]
    pub async fn connect(config: Config) -> Result<Self, RequestError> {
        let collection = connect_collection(&config).await?;
        Ok(Caller { config, collection })
    }

    #[tracing::instrument(skip(self, payload), fields(task_id))]
    /// Starts building a request that will insert a document and eventually wait for
    /// the worker response.
    pub fn send<TInput, TOutput>(&self, payload: TInput) -> SendBuilder<TOutput>
    where
        TInput: Serialize,
        TOutput: DeserializeOwned + Send + Unpin + 'static,
    {
        use mongodb::bson::to_bson;
        let res = serde_json::to_value(payload)
            .map_err(|_| RequestError::PayloadFormat {
                field: "task_input",
            })
            .and_then(|value| {
                to_bson(&value).map_err(|_| RequestError::PayloadFormat {
                    field: "task_input",
                })
            });

        let (payload, payload_err) = match res {
            Ok(bson) => (Some(bson), None),
            Err(err) => (None, Some(err)),
        };

        // Capture the current OpenTelemetry span so workers can link back to it
        let trace_context = TraceContext::capture_current();

        SendBuilder {
            caller: self.clone(),
            payload,
            payload_err,
            timeout: None,
            worker_switch_timeout: None,
            idempotency_key: None,
            trace_context,
            future: None,
            _marker: std::marker::PhantomData,
        }
    }

    #[tracing::instrument(skip(self))]
    /// Rehydrate a `TaskOutput` later on (e.g., after fire-and-forget `dispatch`)
    /// using the task id returned during submission.
    pub async fn await_response<TOutput>(&self, task_id: String) -> Result<TOutput, RequestError>
    where
        TOutput: DeserializeOwned + Unpin,
    {
        wait_for_result::<TOutput>(&self.collection, &task_id).await
    }

    #[tracing::instrument(skip(self, payload))]
    /// Fire-and-forget: enqueue a task, return the `task_id` immediately, and let
    /// another component `await_response` in the future.
    pub async fn dispatch<TInput>(&self, payload: TInput) -> Result<String, RequestError>
    where
        TInput: Serialize,
    {
        self.send::<TInput, Value>(payload).enqueue_only().await
    }
}

/// Builder returned by `Caller::send` that lets each request override defaults
/// before actually enqueuing work in Mongo.
pub struct SendBuilder<TOutput>
where
    TOutput: DeserializeOwned + Send + Unpin + 'static,
{
    caller: Caller,
    payload: Option<Bson>,
    payload_err: Option<RequestError>,
    timeout: Option<Duration>,
    worker_switch_timeout: Option<Duration>,
    idempotency_key: Option<String>,
    trace_context: Option<TraceContext>,
    future: Option<SendFuture<TOutput>>,
    _marker: std::marker::PhantomData<TOutput>,
}

impl<TOutput> SendBuilder<TOutput>
where
    TOutput: DeserializeOwned + Send + Unpin + 'static,
{
    /// Override how long this caller waits for the worker response. `None` means
    /// wait indefinitely, so omitting this call inherits the config default.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Override the worker switch timeout for this task. This governs how soon
    /// another worker may steal the document if the current worker goes away.
    pub fn with_worker_switch_timeout(mut self, timeout: Duration) -> Self {
        self.worker_switch_timeout = Some(timeout);
        self
    }

    /// Provide a custom idempotency key. When omitted we auto-generate a UUID.
    pub fn with_idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(key.into());
        self
    }

    /// Override the captured tracing metadata. Most callers can skip this and let
    /// `TraceContext::capture_current` run automatically.
    pub fn with_trace_context(mut self, trace: TraceContext) -> Self {
        self.trace_context = Some(trace);
        self
    }

    #[tracing::instrument(skip(self))]
    /// Only insert the Mongo document and return the resulting `task_id`.
    pub async fn enqueue_only(self) -> Result<String, RequestError> {
        let SendBuilder {
            caller,
            payload,
            payload_err,
            worker_switch_timeout,
            idempotency_key,
            trace_context,
            ..
        } = self;
        if let Some(err) = payload_err {
            return Err(err);
        }
        let payload = payload.ok_or(RequestError::PayloadFormat {
            field: "task_input",
        })?;
        upsert_task(
            &caller,
            payload,
            worker_switch_timeout.unwrap_or(caller.config.worker_switch_timeout),
            idempotency_key,
            trace_context,
        )
        .await
    }

    #[tracing::instrument(skip(self))]
    fn build_future(&mut self) -> Result<SendFuture<TOutput>, RequestError> {
        if let Some(err) = self.payload_err.take() {
            return Err(err);
        }
        let payload = self.payload.take().ok_or(RequestError::PayloadFormat {
            field: "task_input",
        })?;
        let caller = self.caller.clone();
        let timeout = self.timeout.or(caller.config.request_timeout);
        let worker_switch_timeout = self
            .worker_switch_timeout
            .unwrap_or(caller.config.worker_switch_timeout);
        let idempotency_key = self.idempotency_key.clone();
        let trace_context = self.trace_context.clone();

        Ok(Box::pin(async move {
            let task_id = upsert_task(
                &caller,
                payload,
                worker_switch_timeout,
                idempotency_key,
                trace_context,
            )
            .await?;
            let wait_future = wait_for_result::<TOutput>(&caller.collection, &task_id);
            if let Some(timeout) = timeout {
                tokio::time::timeout(timeout, wait_future)
                    .await
                    .map_err(|_| RequestError::Timeout)?
            } else {
                wait_future.await
            }
        }))
    }
}

impl<TOutput> Future for SendBuilder<TOutput>
where
    TOutput: DeserializeOwned + Send + Unpin + 'static,
{
    type Output = Result<TOutput, RequestError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.future.is_none() {
            let future = match this.build_future() {
                Ok(fut) => fut,
                Err(err) => return Poll::Ready(Err(err)),
            };
            this.future = Some(future);
        }
        let future = this.future.as_mut().expect("future set");
        Future::poll(future.as_mut(), cx)
    }
}

#[tracing::instrument(skip(caller, payload), fields(task_id))]
async fn upsert_task(
    caller: &Caller,
    payload: Bson,
    worker_switch_timeout: Duration,
    idempotency_key: Option<String>,
    trace_context: Option<TraceContext>,
) -> Result<String, RequestError> {
    let task_id = idempotency_key.unwrap_or_else(|| Uuid::new_v4().to_string());
    tracing::Span::current().record("task_id", &task_id);
    let now = DateTime::now();

    // Build the filter: if reset is allowed, also match finished tasks
    let filter = if caller.config.allow_reset_finished_tasks {
        doc! {
            "task_id": &task_id,
            "$or": [
                { "status": { "$exists": false } }, // for upsert insert case
                { "status": { "$in": ["succeeded", "failed"] } },
            ]
        }
    } else {
        // Only match if document doesn't exist (upsert insert path)
        doc! {
            "task_id": &task_id,
            "status": { "$exists": false }
        }
    };

    let mut set_fields = doc! {
        "task_id": &task_id,
        "task_input": payload,
        "status": "pending",
        "updated_at": now,
        "worker_switch_timeout": worker_switch_timeout.as_millis() as i64,
    };
    let trace_bson = trace_context
        .and_then(|trace| {
            mongodb::bson::to_bson(&trace)
                .map_err(|e| {
                    warn!(error=%e, "failed to serialize trace_context; proceeding without trace");
                    e
                })
                .ok()
        })
        .unwrap_or(Bson::Null);
    set_fields.insert("trace_context", trace_bson);

    let update = doc! {
        "$set": set_fields,
        "$setOnInsert": {
            "created_at": now,
        },
        "$unset": {
            "task_output": "",
            "error_reason": "",
            "worker_state": "",
        }
    };

    let options = UpdateOptions::builder().upsert(true).build();

    match caller.collection.update_one(filter, update, options).await {
        Ok(result) => {
            // upserted_id is Some if inserted, None if updated an existing doc
            if result.upserted_id.is_some() || result.matched_count > 0 {
                Ok(task_id)
            } else {
                // No match and no insert means filter didn't match any resetable task
                // and upsert couldn't insert (shouldn't happen normally)
                Err(RequestError::Duplicate { task_id })
            }
        }
        Err(e) => {
            if let ErrorKind::Write(WriteFailure::WriteError(WriteError { code: 11000, .. })) =
                *e.kind
            {
                // Duplicate key = task exists but didn't match update filter (not resetable)
                return Err(RequestError::Duplicate { task_id });
            }
            Err(RequestError::Database(e.to_string()))
        }
    }
}

async fn wait_for_result<TOutput>(
    collection: &Collection<Document>,
    task_id: &str,
) -> Result<TOutput, RequestError>
where
    TOutput: DeserializeOwned + Unpin,
{
    if let Some(outcome) = inspect_task::<TOutput>(collection, task_id).await? {
        return outcome;
    }

    watch_for_result::<TOutput>(collection, task_id).await
}

async fn inspect_task<TOutput>(
    collection: &Collection<Document>,
    task_id: &str,
) -> Result<Option<Result<TOutput, RequestError>>, RequestError>
where
    TOutput: DeserializeOwned + Unpin,
{
    let doc = collection
        .find_one(doc! {"task_id": task_id}, FindOneOptions::default())
        .await
        .map_err(|e| RequestError::Database(e.to_string()))?;
    let Some(doc) = doc else {
        return Ok(Some(Err(RequestError::TaskFailed {
            reason: format!("task {task_id} missing"),
        })));
    };
    evaluate_document::<TOutput>(&doc)
}

async fn watch_for_result<TOutput>(
    collection: &Collection<Document>,
    task_id: &str,
) -> Result<TOutput, RequestError>
where
    TOutput: DeserializeOwned + Unpin,
{
    let pipeline = vec![doc! {
        "$match": {
            "fullDocument.task_id": task_id
        }
    }];
    let options = ChangeStreamOptions::builder()
        .full_document(Some(FullDocumentType::UpdateLookup))
        .build();
    let mut stream = collection
        .watch(pipeline, options)
        .await
        .map_err(|e| RequestError::Database(e.to_string()))?;
    while let Some(event_result) = stream.next().await {
        let event = event_result.map_err(|e| RequestError::Database(e.to_string()))?;
        if let Some(full_doc) = event.full_document
            && let Some(outcome) = evaluate_document::<TOutput>(&full_doc)?
        {
            return outcome;
        }
    }
    Err(RequestError::Database("change stream closed".into()))
}

fn evaluate_document<TOutput>(
    doc: &Document,
) -> Result<Option<Result<TOutput, RequestError>>, RequestError>
where
    TOutput: DeserializeOwned + Unpin,
{
    let status = doc
        .get_str("status")
        .map_err(|_| RequestError::PayloadFormat { field: "status" })?;
    if status == "pending" {
        return Ok(None);
    }

    if status == "succeeded" {
        let output_bson = doc
            .get("task_output")
            .ok_or(RequestError::PayloadFormat {
                field: "task_output",
            })?
            .clone();
        let value: TOutput =
            mongodb::bson::from_bson(output_bson).map_err(|_| RequestError::PayloadFormat {
                field: "task_output",
            })?;
        return Ok(Some(Ok(value)));
    }

    if status == "failed" {
        let reason = doc
            .get_str("error_reason")
            .unwrap_or("worker failed")
            .to_string();
        return Ok(Some(Err(RequestError::TaskFailed { reason })));
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trace::TraceContext;
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

        let success = super::inspect_task::<TestOutput>(&caller.collection, success_id)
            .await
            .unwrap();
        assert_eq!(success.unwrap().unwrap(), success_output);

        let failure = super::inspect_task::<TestOutput>(&caller.collection, failure_id)
            .await
            .unwrap();
        assert!(matches!(
            failure.unwrap(),
            Err(RequestError::TaskFailed { reason }) if reason.contains("boom")
        ));

        let missing = super::inspect_task::<TestOutput>(&caller.collection, "missing")
            .await
            .unwrap();
        assert!(missing.unwrap().is_err());

        drop_collection(&config).await;
    }

    #[derive(Serialize)]
    struct DummyPayload {
        value: String,
    }
}
