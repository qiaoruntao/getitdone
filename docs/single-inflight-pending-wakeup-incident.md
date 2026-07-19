# Single-inflight pending-task wakeup incident

## Temporary mitigation

The worker now makes one unconditional atomic ready-task claim probe whenever
a task completes and releases a semaphore permit. This removes the
`ExpiryTracker` pending-hint gate from the completion path; the tracker remains
responsible for change-stream and local-timer scheduling. The accompanying
integration regression test is
`test_completion_claims_pending_task_with_no_further_events`.

## Reported symptom

A worker configured with `with_max_inflight(1)` completed one long task and
then remained online without claiming any of 31 `pending` tasks in the same
Mongo collection. Restarting the worker immediately claimed the next task.

## Production evidence

Observed on Oracle on 2026-07-17 in the dedicated
`kuaishou_playback_download_v1` collection:

- The worker completed one task successfully at 22:07 UTC.
- PM2 kept the worker process online with no crash/restart before manual
  intervention.
- FFmpeg completed successfully; no task was still executing.
- The collection then contained one `succeeded` document and 31 ordinary
  `pending` documents. The pending documents had no delayed schedule or active
  worker lease.
- A manual PM2 restart caused the same worker to claim the next pending task
  immediately and begin its FFmpeg work.

This rules out an expired Kuaishou URL, FFmpeg failure, or a crashed worker as
the immediate explanation. It points to task pickup/wakeup state inside
`getitdone::Worker`.

## Relevant worker behavior

`worker_loop` performs a startup `refresh_pending_tasks(...)` followed by
`pump_available_tasks(..., ClaimMode::Ready, ...)`. With one permit, the first
claim consumes that permit. Tasks inserted while it is busy can remain pending.

After a job joins, the loop calls `pump_available_tasks` only when
`expiry_tracker.has_pending()` is true. If that tracker was cleared by an
earlier no-task probe or not armed by the relevant change-stream events, the
released permit remains unused despite ready documents in Mongo.

The current code comment near this completion path already identifies the
general risk: without a completion-triggered pump, a released permit can sit
idle until a later change-stream event or fallback tick. This incident shows
the tracker guard can still suppress that pump.

## Reproduction target

1. Start a worker with `max_inflight = 1` against an empty collection.
2. Insert/claim one task whose worker function waits long enough to create a
   window.
3. Insert one or more additional ready tasks while the first task is running.
4. Let the first task succeed.
5. Assert the worker claims a second task without a process restart or another
   task insertion.

Use a Mongo replica set so the test exercises the normal change-stream path.

## Fix direction to evaluate

Make a task completion an unconditional ready-work probe whenever a semaphore
permit is released. The atomic `claim_next_task` is safe when no task exists;
it returns `None`. Do not rely solely on `ExpiryTracker` as permission to make
that probe.

Keep recovery change-stream driven and local-timer based. This is not a reason
to introduce MongoDB TTL scheduling or broad periodic polling.

## Acceptance criteria

- A single-inflight worker claims the next pre-existing pending task after any
  task completion.
- The behavior works when pending tasks were inserted before worker startup and
  while another task was running.
- No duplicate concurrent claim of the same task occurs.
- Existing multi-inflight and stale-running-task recovery tests continue to
  pass.
- Add an integration test under `getitdone/tests/` that reproduces this exact
  completion-wakeup sequence.
