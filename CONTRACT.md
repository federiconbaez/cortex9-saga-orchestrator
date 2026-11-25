# CONTRACT — cortex9-saga-orchestrator

Ref: CX9-6204

## Problem

A business transaction spans several services that cannot share a database
transaction (reserve stock → charge card → create shipment). If a later step
fails, earlier effects must be undone by **compensating actions**; if the
orchestrator crashes, the transaction must resume without duplicating
effects. This library is an orchestration-based saga engine with a pure,
event-sourced domain.

## Model

- A `SagaDefinition` is an ordered list of `StepDefinition`s. Each step has a
  kind (Garcia-Molina & Salem 1987, plus pivot terminology from
  microservices practice): `COMPENSATABLE*`, at most one `PIVOT`, then
  `RETRIABLE*`. Kinds must appear in that order.
- Each step has a timeout, a forward `RetryPolicy` and a compensation
  `RetryPolicy` (exponential backoff, capped, with jitter from a `Random` port).
- The `Saga` aggregate is rebuilt from its events (event sourcing) and acts
  as a decider: `next_command(now)` returns `Execute`, `Wait` or `Finish`;
  outcomes are recorded with `record_success` / `record_failure`.

## Invariants (tested, including property-based tests)

1. **Terminal states**: every run ends in `COMPLETED`, `COMPENSATED` or
   `FAILED` (operator attention), never anything else.
2. **Completed ⇒ all forward steps succeeded**, in definition order.
3. **Backward recovery in reverse order**: on failure before/at the pivot,
   every step whose effect *may* have happened (succeeded, timed out, or
   exhausted transient retries) is compensated, in reverse order of the
   forward sequence. A step that failed *permanently* is assumed to have had
   no effect and is not compensated. Steps never attempted are never compensated.
4. **No compensation after the pivot**: once the pivot succeeds, later steps
   use forward recovery (retry). Exhausting them ends in `FAILED`, never in
   compensation.
5. **Unknown pivot outcome is not guessed**: a pivot that times out or
   exhausts transient retries ends in `FAILED` rather than compensating
   something that may have committed.
6. **Stable idempotency keys**: `"{saga_id}/{step}/{phase}"` is identical for
   every attempt and across crashes, so idempotent participants apply each
   effect at most once.
7. **Write-ahead**: `StepStarted` is persisted before the handler runs.
   After a crash, the in-flight attempt is re-executed with the same key
   (at-least-once delivery + idempotency = effectively-once effect).
8. **Optimistic concurrency**: appending events with a stale expected
   version raises `ConcurrencyError`; two orchestrators cannot interleave
   decisions on one saga.
9. **Illegal transitions raise** `InvalidTransition` (e.g. recording an
   outcome for a step that is not in flight).

## Architecture (hexagonal)

| Layer | Path | Rules |
|-------|------|-------|
| Domain | `cortex9_saga/domain` | dataclasses + enums only; no asyncio, I/O, json, time, random. |
| Ports | `cortex9_saga/ports.py` | `Protocol`s: `SagaStore`, `Clock`, `Sleeper`, `Random`, `StepHandler`. |
| Application | `cortex9_saga/application` | `SagaOrchestrator` (asyncio), handler registry, error classification. |
| Adapters | `cortex9_saga/adapters` | in-memory and JSON-file stores (flock + atomic rename), system/virtual clocks, seeded random, idempotent participant helper. |

`scripts/check_architecture.py` fails CI if the domain imports anything
outside the standard-library allow-list or any outer layer.

## Acceptance criteria

- `ruff check`, `ruff format --check`, `mypy --strict`, `pytest` green on
  Python 3.11, 3.12 and 3.13.
- Property tests (Hypothesis) for invariants 1–4 and 6–7 under random
  failure scripts and random crash points.

## Out of scope

- Choreography (event-driven sagas without an orchestrator).
- Distributed locking / leader election between orchestrator processes
  beyond optimistic concurrency + file locks.
- Parallel (fan-out) steps; steps run sequentially.
- Message broker integration; handlers are async callables.
- Semantic locks / isolation anomalies between concurrent sagas (dirty reads
  of uncommitted saga effects are a known limitation of the pattern).
