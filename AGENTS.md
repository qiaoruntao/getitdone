## Agent Notes

Quick reminders for anyone touching this repository:

1. **Language matters** – stick to the caller/worker vocabulary. Avoid resurrecting “handler/processor/producer/consumer/mscheduler” phrasing in user-facing docs.
2. **Mongo collections, not queues** – every explanation should describe work flowing through a MongoDB *collection*. Config fields follow that naming (`Config::collection`).
3. **Timeouts are optional** – `request_timeout` is `Option<Duration>`. Leaving it as `None` must keep the request alive indefinitely.
4. **Fire-and-forget exists** – always mention that callers can `dispatch()` a task, store the returned `TaskId`, and `await_response()` later.
5. **Trace context is automatic** – `Caller::send` captures the current tracing/span context. Examples should never require a manual `.clone()` unless overriding via `send_with_context`.
6. **Docs first** – README stays high-level and conceptual; `docs/implementation.md` captures the architecture. Keep them in sync whenever the API shape changes.
7. **Preserve docs** – don’t delete doc sections unless they conflict with new decisions or we explicitly need to clean up obsolete content. Prefer additive updates.
8. **Tests live under `tests/`** – keep unit/integration tests in the Cargo `tests` directory. Avoid embedding large test modules inside `src/` files so the crate code stays clean and library users aren’t forced to compile test-only logic.
9. **No TTL scheduling** – do not use MongoDB TTL indexes or TTL deletion behavior to schedule task pickup, lease expiry, or recovery.
10. **Prefer watched lease events** – periodic collection scanning is considered immature unless backed by concrete proof. Worker recovery should be driven by Mongo change streams plus local timers seeded from watched task state and startup reads.
11. **Library-wide changes** – every modification, fix, and implementation must preserve behavior for all task types, not just the caller/worker payload that exposed the issue.
12. **Agree before implementation** – see `/Users/qiaoruntao/projects/CLAUDE.md` → "Agree on approach before implementing".
