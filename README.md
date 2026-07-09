## getitdone

`getitdone` experiments with a dead simple way to call “a function that secretly runs somewhere else”. All you define are:

1. A **caller**: the code that submits work.
2. A **worker**: the code that receives that work and sends a reply.

They exchange a pair of plain structs:

- **TaskInput** – what the caller sends (e.g., image resize params).
- **TaskOutput** – what the worker returns (e.g., resized image info).

That’s the entire vocabulary. No extra jargon, just input → worker → output.

### How it feels

- From the caller’s perspective, you `await` a result exactly like a normal async function call.
- From the worker’s perspective, you implement “when I get `TaskInput`, run my logic and respond with `TaskOutput`”.
- Caller and worker can live in the same binary during development or in different services in production.

### Setup

Both the caller and the worker start from the same configuration. The config only needs to answer three things:

1. Which MongoDB cluster stores the tasks?
2. Which collection name should we use?
3. What optional knobs (timeout, visibility, default worker switch timeout) do we want?

```rust
let config = getitdone::Config::builder()
    .mongo_uri("mongodb://localhost:27017")
    .database("getitdone")
    .collection("image_resize")
    .request_timeout(None) // None => allow tasks to run indefinitely
    .worker_switch_timeout(Duration::from_secs(10))
    .reset_finished_tasks(true) // allow reusing ids for finished tasks
    .build();

let caller = getitdone::Caller::connect(config.clone()).await?;
let worker_handle = getitdone::Worker::connect(config).await?.spawn();
```

Once the config exists, the caller and worker can live in different binaries as long as they point to the same MongoDB deployment.

Workers rely on MongoDB change streams, so the deployment must be a replica set or sharded cluster. The crate will surface a `RequestError::Database` if change streams are disabled (e.g., standalone localhost instances).

### Example flow

```rust
#[derive(serde::Serialize)]
struct LengthRequest {
    text: String,
}

#[derive(serde::Deserialize)]
struct LengthResponse {
    length: usize,
}

use getitdone::{Worker, WorkerJob};

// 1. Start a worker (could run in another binary)
let worker_handle = getitdone::Worker::connect(config.clone())
    .await?
    .run(|job: WorkerJob<LengthRequest>| async move {
        Ok(LengthResponse {
            length: job.payload.text.chars().count(),
        })
    });

// 2. From the caller side, send work and await the reply
let request = LengthRequest {
    text: "hello".into(),
};
let result: LengthResponse = caller.send(request).await?;
assert_eq!(result.length, 5);

worker_handle.shutdown().await?;
```

Handlers receive a `WorkerJob<TInput>` that exposes the `task_id`, optional `TraceContext`, and the typed payload so they can log or open child spans without extra plumbing.

No queues, no events, no extra ceremonies exposed to the user—just a round trip with a tiny bit of scheduling under the hood.

Behind the scenes both sides read/write the same MongoDB collection defined by the config. Each task document stores the input payload, which worker claimed it, and the final output so any caller that knows the task id can fetch the response later.

#### Recommended indexes

`getitdone` no longer auto-creates indexes. Add them yourself once per collection to keep idempotency and worker steals fast:

```js
db.collection.createIndex({ task_id: 1 }, { unique: true })
db.collection.createIndex({ status: 1, updated_at: 1 })
db.collection.createIndex({ "worker_state.worker_id": 1 })
```

If they’re missing we’ll log a warning at startup, but the code will continue running.

### Per-request options

`caller.send(...)` returns a builder so each invocation can tweak behavior before awaiting the result:

```rust
let result = caller
    .send(ResizeImageInput { /* … */ })
    .with_timeout(Duration::from_secs(45))           // override global timeout
    .with_worker_switch_timeout(Duration::from_secs(5)) // override default steal delay
    .with_idempotency_key("img_42")                  // deduplicate repeated calls
    .await?;
```

The builder always yields a `Result<TaskOutput, RequestError>`. `Ok` means the worker finished successfully; `Err` indicates the task failed (see **Failure handling**).

- Each Mongo collection is bound to a single `TaskInput`/`TaskOutput` pair. Mixing payload types in the same collection will break compatibility.
- Task payloads are stored as JSON (we expect structs that implement `Serialize`/`Deserialize`). Strings become JSON strings and results are stored as JSON numbers/objects so Mongo stays type-safe.
- `.with_timeout` overrides how long this caller waits for a response.
- `.with_worker_switch_timeout` controls the earliest point when another worker may steal the task if the current worker disappears (defaults to the config value). Workers track running-task heartbeat expiry from startup scans and change-stream heartbeat updates, so dead-worker pickup does not depend on a frequent polling sweep.
- `.with_idempotency_key` deduplicates duplicate submissions.
- `.with_trace_context` takes a `TraceContext` so you can override the captured tracing metadata (optional).
- `.reset_finished_tasks(true)` lets you reuse task ids that already finished (helpful for manual retries after a restart).
- If a worker or caller tries to deserialize a task into the wrong type, the worker will emit `RequestError::PayloadFormat` and mark the task as failed. Each collection must stick to a single pair of Rust types to avoid these errors.

### Tracing and metadata

Each task can carry an optional trace or span identifier so observability tools can stitch caller and worker logs together. By default `caller.send(...)` captures the active tracing context (if any) and forwards it to the worker automatically:

```rust
tracing::info_span!("resize-request", request_id = %uuid).in_scope(|| async {
    // active span is captured internally; no need to pass anything
    let result = caller.send(ResizeImageInput { /* … */ }).await?;
    Ok::<_, Error>(result)
}).await?;
```

If you need to override the context (for example, bridging from a different tracing backend), call the builder-style API and provide the context explicitly:

```rust
use getitdone::TraceContext;

let external_trace = TraceContext::from_parts(trace_id_hex, span_id_hex);
let result = caller
    .send(ResizeImageInput { /* … */ })
    .with_trace_context(external_trace)
    .await?;
```

Need to override sampling? Construct it with `TraceContext::from_parts_with_flags` and pass an explicit `TraceFlags`.

The worker receives the same `TraceContext` alongside `TaskInput` via `WorkerJob::trace_context`, making it easy to start a child span or emit logs with the parent trace id. The identifiers are stored as a Mongo document (`{ trace_id, span_id, trace_flags }`) and the worker adds an explicit span link so tracing backends can stitch caller/worker activity without extra plumbing. Skip `.with_trace_context` if you want to rely on the automatically captured context.


### Long-running or fire-and-forget

- `request_timeout` is optional. Leaving it as `None` allows the caller to wait forever (useful for unbounded workloads).
- Not every caller needs the response immediately. Use the fire-and-forget variant to just enqueue work:

```rust
let task_id = caller.dispatch(ResizeImageInput { /* … */ }).await?; // equals the idempotency key (auto UUID if unset)
// later, maybe in another service:
let output: ResizeImageOutput = caller.await_response(task_id).await?;
```

This makes it easy to trigger tasks without blocking while still allowing any component to re-await the response when needed.

### Failure handling

We separate failures by origin so callers can react appropriately:

| Origin | Scenario | Result |
| --- | --- | --- |
| **Caller** | Duplicate idempotency key | `Err(RequestError::Duplicate)` – request rejected, existing task untouched |
|  | Per-call timeout fired | `Err(RequestError::Timeout)` – caller stopped waiting, task continues in Mongo |
| **Database** | Mongo unreachable or write/read error | `Err(RequestError::Database(_))` – nothing persisted |
|  | Payload could not be deserialized (mismatched types) | `Err(RequestError::PayloadFormat { field })` – task marked failed with serialization details |
| **Worker** | Handler returned `Ok(TaskOutput)` | `Ok(payload)` |
|  | Handler returned domain error | `Err(RequestError::TaskFailed { reason })` – failure stored in Mongo |
|  | Worker crashed/disconnected mid-task | `Err(RequestError::WorkerGone)` – task becomes stealable after its configured `worker_switch_timeout`, then can be picked up by the rare fallback detector |
|  | No worker ever claimed the task before worker_timeout | `Err(RequestError::WorkerTimeout)` – caller can retry later or inspect task status |

Workers automatically mark tasks as failed (with a shutdown reason) when they exit gracefully. If the process dies without updating state, the task is unlocked according to the per-task `worker_switch_timeout` (either the config default or the per-request override). Workers keep an in-memory expiry map for running tasks and wake when a tracked heartbeat expires. A rare periodic stale-claim path remains as a safety resync, not the normal scheduler.

Retries are intentionally left out of v0 because real retry policies are nuanced (backoff, jitter, partial progress). For now each claimed task runs exactly once. Use idempotency keys to avoid double work if you need to retrigger the same operation manually, and rely on `await_response(task_id)` to check the eventual outcome.

### Scope for the first version

- Mongo-backed delivery shared by caller & worker
- One output per input
- Optional timeout so callers aren’t stuck forever
- Fire-and-forget dispatch with later re-await by task id
- Trace context propagation so worker logs can link to caller requests
- Idempotency keys via the send builder
- Simple errors: either you got a reply, it timed out, or the worker crashed

### Repository layout

- `src/lib.rs` – crate entry point (implementation coming soon)
- `README.md` – this document
- `docs/implementation.md` – design & workflow details

👉 Check `docs/implementation.md` for the architecture behind the caller/worker pair.
