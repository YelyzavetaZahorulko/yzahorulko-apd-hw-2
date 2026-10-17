# Implementation Plan

Complete this before implementation and update it when the plan changes.

## Approach
`TaskProcessor` is an actor. `process` has three phases:
1. **Preflight.** Scan the IDs left to right and throw `.duplicateID(firstRepeatedID)`
   before any worker starts. An empty input returns `[]`.
2. **Bounded scheduling.** Use a task group that starts `min(maximumConcurrency, jobs.count)`
   child tasks. Each finished child frees a slot, and the next unstarted job (left to right) takes it.
3. **Collection.** Each child returns `(index, JobOutcome)`. Results are written into
   `[JobOutcome?]` by index, so the output order always matches the input order.

Outcome mapping inside the child:
- the worker returns a value → `.succeeded(value)`
- the worker throws `CancellationError` → `.cancelled`
- the worker throws any other error → `.failed`

Cancellation: before every scheduling decision the processor checks `Task.isCancelled`.
If it is set, the processor calls `group.cancelAll()`, starts nothing new, and marks every
never-started job `.cancelled`. Results that already completed keep their outcome.

The scheduler lives in a private core that takes `(Job) async -> JobOutcome`.

Retry extension: `process(_:retryPolicy:worker:)` reuses the same scheduler through an
internal `runWithExecutor`. Attempts for one job run sequentially inside one child task,
so a retry never takes an extra concurrency slot. `CancellationError` is never retried.

The optional retry extension can reuse it by wrapping the worker in an attempt loop.
## Milestones
- [x] Validate configuration and all job IDs before starting work.
- [x] Schedule jobs within the concurrency limit and preserve input order.
- [x] Map success, ordinary errors, and cancellation outcomes.
- [x] Handle caller cancellation and never-started jobs.
- [x] Add at least six distinct student-authored tests and run `swift test`.
- [x] Additional task

## Risks and checks

- **Exceeding the limit:** a slot is freed only after a child is collected.
  Check with a probe actor that records the peak number of active workers.
- **Wrong result order:** check with gated workers that finish in reverse order.
- **A worker starting before the duplicate check:** check with a counter that must stay 0.
- **Overwriting completed results on cancellation:** write an outcome only for indices
  that have no result yet.
- **Data races:** use only actors and `Sendable` types, with no locks or GCD.
- **Tests that depend on time:** use gates (continuations), not sleeps.
