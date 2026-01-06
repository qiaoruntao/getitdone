## Implementation Notes

This document describes the minimal architecture behind the caller/worker flow. Version 0 keeps everything intentionally small so teams can try the API quickly and we can iterate on real feedback before adding advanced features.

---

### Goals

1. **Function-call feel:** Submitting work should look like `caller.send(params).await`.
2. **Two roles only:** A caller submits, a worker listens. No extra components.
3. **Typed payloads:** `TaskInput` and `TaskOutput` are user-defined structs with `serde` bounds.
4. **Mongo-backed mailbox:** Task metadata lives in MongoDB collections so multiple processes can communicate reliably.

### Non-goals for v0

- Retries / exponential backoff
- Fancy scaling controls (auto worker pools, sharding)
- Multi-stage workflows or fan-out graphs

---

### Core entities

| Entity | Responsibility |
| --- | --- |
| `TaskInput` | User payload that describes the work. Implements `Serialize + DeserializeOwned + Send + 'static`. |
| `TaskOutput` | Response payload returned by the worker. Same trait bounds as `TaskInput`. |
| `String` | Unique identifier assigned to each task so callers can re-fetch/await results later. |
| `TraceContext` | Optional struct with `trace_id`/`span_id`/`trace_flags` propagated from caller to worker (stored as a Mongo document). |
| `Config` | MongoDB connection info plus collection name and optional knobs (timeouts, visibility, worker switch delay). |
| `Caller` | API surface for sending a `TaskInput` and waiting for a `TaskOutput`. |
| `Worker` | Loop that polls tasks from MongoDB, runs a user-provided async function, and writes back the response. |
| `Mongo collection` | Durable storage for tasks/responses. Each collection corresponds to a single `(TaskInput, TaskOutput)` pair for compatibility. |

---

### Configuration

```rust
pub struct Config {
    pub mongo_uri: String,
    pub database: String,
    pub collection: String,
    pub request_timeout: Option<Duration>,
    pub worker_switch_timeout: Duration,
}
```

- The caller and worker must be created from the same `Config`.
- Each queue maps to a Mongo collection containing pending tasks plus worker state.
- Optional knobs let us decide how long a caller waits and how soon a worker can steal in-flight work (including per-task switch delays).
- Builders can call `reset_finished_tasks(true)` if they want to reuse task ids that already finished (handy when replaying work manually after a restart).

Workers rely on MongoDB change streams, so the deployment must be a replica set or sharded cluster. Standalone servers without change-stream support cause `Worker::connect` to error immediately.

We no longer auto-create indexes. Operators should provision them once per collection:

- `db.collection.createIndex({ task_id: 1 }, { unique: true })`
- `db.collection.createIndex({ status: 1, updated_at: 1 })`
- `db.collection.createIndex({ "worker_state.worker_id": 1 })`

Missing indexes only trigger warnings at runtime.

---

### Data flow

1. **Worker boot** – `Worker::spawn` polls Mongo for `pending` tasks, claims them via `find_one_and_update`, and spawns each job on its own Tokio task while reporting heartbeats.
2. **Send task** – `caller::send(input)` returns a builder that lets the caller set per-request options (timeout, worker switch timeout, idempotency key, trace override). Calling `.await` on the builder inserts the task document (payload stored as JSON plus metadata such as trace/idempotency key/worker switch timeout) and then waits for change-stream notifications.
3. **Dispatch only** – `caller::dispatch(input)` is the fire-and-forget variant that writes the document (still storing metadata for later tracing) and immediately returns the assigned `task_id` (String).
4. **Process task** – The worker executes the user code (string length in the prototype), emits periodic heartbeats, and updates the Mongo document only if it still owns the task. Future versions may add retry loops.
5. **Return value** – Callers awaiting inline rely on Mongo change streams (with a polling fallback) for notification. Callers that only have a `task_id` can later call `await_response(task_id)` to rehydrate the result, including failure details.

Workers automatically mark their in-flight tasks as failed (with a shutdown reason) before a graceful shutdown. If a worker disappears without updating state, the per-task worker switch timeout ensures the task is unlocked and becomes stealable only after the requested delay.

Tasks remain durable in Mongo even if no worker is running yet, so `TaskId` lookups keep working across restarts.

Trace propagation is automatic: `Caller::send` captures the current OpenTelemetry context, stores `{ trace_id, span_id, trace_flags }` inside the task document, and the worker rehydrates that struct so the `worker.handler` span can attach an explicit link to the caller span. Custom propagators can override the value via `.with_trace_context`.

---

### Module sketch

```
 src/
  ├─ lib.rs
  ├─ caller.rs       // send / dispatch APIs + Mongo bridge
  ├─ worker.rs       // background loop + Mongo bridge
  ├─ config.rs       // Config + builder
  └─ storage.rs      // shared Mongo client helpers
```

`lib.rs` re-exports `Config`, `Caller`, and `Worker` so applications can connect from different processes while sharing the same Mongo deployment.

---

### Mongo schema (tasks collection)

| Field | Type | Notes |
| --- | --- | --- |
| `_id` | `ObjectId` | Mongo-generated primary key (internal use only). |
| `task_id` | `Uuid` | Public identifier; equals the idempotency key, auto-generated if none provided. |
| `task_input` | `Bson` | Serialized `TaskInput`. |
| `task_output` | `Option<Bson>` | Serialized `TaskOutput` once available. |
| `status` | `String` | `pending`, `running`, `succeeded`, `failed`. |
| `created_at` | `DateTime` | Submission time. |
| `updated_at` | `DateTime` | Last mutation to the document (used for auditing). |
| `idempotency_key` | `String` | Same as `task_id`; unique index per collection. |
| `request_timeout` | `Option<i64>` | Caller-specific timeout in millis (for reference). |
| `worker_switch_timeout` | `i64` | Delay before another worker may steal the task. |
| `trace_context` | `Option<Document>` | Serialized `TraceContext` (`{ trace_id, span_id, trace_flags }`). |
| `worker_state` | `Document` | Contains `worker_id`, `started_at`, `heartbeat_at` (periodic ping), `finished_at`, `shutdown_reason`. |
| `error_reason` | `Option<String>` | Worker-provided failure reason or payload format error. |

Indexes:
- `{ idempotency_key: 1 }` unique (per collection); serves as public task id.
- `{ status: 1, updated_at: 1 }` to find stealable tasks.
- `{ worker_state.worker_id: 1 }` to fetch in-flight tasks during shutdown.

Responses can either live inside the same document (`task_output`) or separate collection if we need to store large payloads; for v0 we keep them together for simplicity.

---

### API sketch

```
pub struct Caller {
    config: Config,
    collection: Collection<Document>,
}

impl Caller {
    pub async fn connect(config: Config) -> Result<Self, RequestError>;
    pub fn send<TInput, TOutput>(&self, payload: TInput) -> SendBuilder<TOutput>;
    pub async fn dispatch<TInput>(&self, payload: TInput) -> Result<String, RequestError>;
    pub async fn await_response<TOutput>(&self, task_id: String) -> Result<TOutput, RequestError>;
}

pub struct SendBuilder<TOutput> { /* builder storing timeout/idempotency/trace overrides */ }

impl<TOutput> SendBuilder<TOutput> {
    pub fn with_timeout(self, timeout: Duration) -> Self;
    pub fn with_worker_switch_timeout(self, timeout: Duration) -> Self;
    pub fn with_idempotency_key(self, key: impl Into<String>) -> Self;
    pub fn with_trace_context(self, trace: TraceContext) -> Self;
}

impl<TOutput> Future for SendBuilder<TOutput> {
    type Output = Result<TOutput, RequestError>;
    // stores JSON payload in Mongo, then waits for change-stream events with a polling fallback
}

pub struct WorkerJob<TInput> {
    pub task_id: String,
    pub trace_context: Option<TraceContext>,
    pub payload: TInput,
}

pub struct Worker {
    config: Config,
    collection: Collection<Document>,
}

impl Worker {
    pub async fn connect(config: Config) -> Result<Self, RequestError>;
    pub fn run<TInput, TOutput, H, Fut>(self, handler: H) -> WorkerHandle
    where
        TInput: DeserializeOwned + Send + 'static,
        TOutput: Serialize + Send + Sync + 'static,
        H: Fn(WorkerJob<TInput>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<TOutput, RequestError>> + Send + 'static;
}
```

Both roles talk directly to the same Mongo collection; `storage.rs` centralizes the connection code so callers and workers can run in separate binaries.

### Network Compression

The crate supports `zstd`, `snappy`, and `zlib` compression. You can enable them via your MongoDB connection URI if the server supports them.
Example: `mongodb://localhost:27017/?compressors=zstd`

---

### Error handling

- `RequestError::Duplicate { task_id }` – caller reused an idempotency key; the new request is rejected and the original task/result stay untouched.
- `RequestError::Timeout` – caller waited longer than the effective timeout (per-request override or config default); the task continues running in Mongo even though this caller stopped waiting.
- `RequestError::Database` – Mongo returned an error while enqueuing or fetching the result.
- `RequestError::PayloadFormat { field }` – payload could not be serialized/deserialized into the expected JSON for `field` (e.g., incompatible types within the same collection).
- `RequestError::TaskFailed { reason }` – worker returned a domain error and the result is stored as failure.
- `RequestError::WorkerGone` – worker crashed or connection closed before writing a response; the task remains pending and becomes stealable after its configured worker switch timeout.
- `RequestError::WorkerTimeout` – unused in current implementation (callers rely on their own `request_timeout`).
- Trace propagation errors are swallowed; if a trace cannot be serialized we fall back to sending the task without trace metadata.

---

### Next iterations

Once the API feels right, we can layer in:

1. Additional storage adapters (Redis, Postgres)
2. Retries and backoff policies
3. Streaming responses / progress updates

For now the focus is the ergonomics of the caller/worker handshake powered by a simple Mongo mailbox.
