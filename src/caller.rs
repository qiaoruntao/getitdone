use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

type SendFuture<T> = Pin<Box<dyn Future<Output = Result<T, RequestError>> + Send>>;

use bson::{Bson, DateTime, Document, doc};
use futures_util::StreamExt;
use mongodb::Collection;
use mongodb::error::{ErrorKind, WriteError, WriteFailure};
use mongodb::options::FullDocumentType;
use serde::Serialize;
use serde::de::DeserializeOwned;
use uuid::Uuid;

use crate::config::Config;
use crate::error::RequestError;
use crate::storage::connect_collection;
#[cfg(feature = "tracing")]
use crate::trace::TraceContext;
#[cfg(feature = "tracing")]
use tracing::warn;

/// High-level API a caller uses to submit `TaskInput` payloads into the configured
/// Mongo collection and await the matching `TaskOutput`.
#[derive(Clone, Debug)]
pub struct Caller {
    pub(crate) config: Config,
    collection: Collection<Document>,
}

impl Caller {
    /// Returns a reference to the underlying MongoDB collection.
    pub fn collection(&self) -> &Collection<Document> {
        &self.collection
    }
    /// Connects to the Mongo deployment described by `config`. The same config should
    /// later be handed to a worker so both roles talk to the same collection.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(config)))]
    pub async fn connect(config: Config) -> Result<Self, RequestError> {
        let collection = connect_collection(&config).await?;
        Ok(Caller { config, collection })
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(skip(self, payload), fields(task_id))
    )]
    /// Starts building a request that will insert a document and eventually wait for
    /// the worker response.
    pub fn send<TInput, TOutput>(&self, payload: TInput) -> SendBuilder<TOutput>
    where
        TInput: Serialize,
        TOutput: DeserializeOwned + Send + Unpin + 'static,
    {
        let (payload, payload_err) = match bson::serialize_to_bson(&payload) {
            Ok(bson) => (Some(bson), None),
            Err(_) => (
                None,
                Some(RequestError::PayloadFormat {
                    field: "task_input",
                }),
            ),
        };

        // Capture the current OpenTelemetry span so workers can link back to it
        #[cfg(feature = "tracing")]
        let trace_context = TraceContext::capture_current();
        #[cfg(not(feature = "tracing"))]
        let trace_context = None;

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

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    /// Rehydrate a `TaskOutput` later on (e.g., after fire-and-forget `dispatch`)
    /// using the task id returned during submission.
    pub async fn await_response<TOutput>(&self, task_id: String) -> Result<TOutput, RequestError>
    where
        TOutput: DeserializeOwned + Unpin,
    {
        wait_for_result::<TOutput>(&self.collection, &task_id).await
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self, payload)))]
    /// Fire-and-forget: enqueue a task, return the `task_id` immediately, and let
    /// another component `await_response` in the future.
    pub async fn dispatch<TInput>(&self, payload: TInput) -> Result<String, RequestError>
    where
        TInput: Serialize,
    {
        self.send::<TInput, Bson>(payload).enqueue_only().await
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
    #[cfg(feature = "tracing")]
    trace_context: Option<TraceContext>,
    #[cfg(not(feature = "tracing"))]
    trace_context: Option<()>,
    future: Option<SendFuture<TOutput>>,
    _marker: std::marker::PhantomData<TOutput>,
}

impl<TOutput> std::fmt::Debug for SendBuilder<TOutput>
where
    TOutput: DeserializeOwned + Send + Unpin + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("SendBuilder");
        d.field("caller", &self.caller)
            .field("payload", &self.payload)
            .field("payload_err", &self.payload_err)
            .field("timeout", &self.timeout)
            .field("worker_switch_timeout", &self.worker_switch_timeout)
            .field("idempotency_key", &self.idempotency_key);
        #[cfg(feature = "tracing")]
        d.field("trace_context", &self.trace_context);
        d.field("future", &self.future.as_ref().map(|_| "..."))
            .field("_marker", &self._marker)
            .finish()
    }
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
    #[cfg(feature = "tracing")]
    pub fn with_trace_context(mut self, trace: TraceContext) -> Self {
        self.trace_context = Some(trace);
        self
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
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

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
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

#[cfg_attr(
    feature = "tracing",
    tracing::instrument(skip(caller, payload), fields(task_id))
)]
async fn upsert_task(
    caller: &Caller,
    payload: Bson,
    worker_switch_timeout: Duration,
    idempotency_key: Option<String>,
    #[cfg(feature = "tracing")] trace_context: Option<TraceContext>,
    #[cfg(not(feature = "tracing"))]
    #[allow(unused_variables)]
    trace_context: Option<()>,
) -> Result<String, RequestError> {
    let task_id = idempotency_key.unwrap_or_else(|| Uuid::new_v4().to_string());
    #[cfg(feature = "tracing")]
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

    #[cfg(feature = "tracing")]
    let trace_bson = trace_context
        .and_then(|trace| {
            bson::serialize_to_bson(&trace)
                .map_err(|e| {
                    warn!(error=%e, "failed to serialize trace_context; proceeding without trace");
                    e
                })
                .ok()
        })
        .unwrap_or(Bson::Null);

    #[cfg(not(feature = "tracing"))]
    let trace_bson = Bson::Null;

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

    match caller
        .collection
        .update_one(filter, update)
        .upsert(true)
        .await
    {
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

pub async fn inspect_task<TOutput>(
    collection: &Collection<Document>,
    task_id: &str,
) -> Result<Option<Result<TOutput, RequestError>>, RequestError>
where
    TOutput: DeserializeOwned + Unpin,
{
    let doc = collection
        .find_one(doc! {"task_id": task_id})
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
    let mut stream = collection
        .watch()
        .pipeline(pipeline)
        .full_document(FullDocumentType::UpdateLookup)
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
            bson::deserialize_from_bson(output_bson).map_err(|_| RequestError::PayloadFormat {
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
