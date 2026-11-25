# cortex9-saga-orchestrator

Orchestration-based **sagas** for Python 3.11+: declarative steps,
compensations in reverse order, pivot/retriable semantics, timeouts, retries
with backoff, stable idempotency keys and crash recovery from an event-sourced
history. Zero runtime dependencies; hexagonal layout with a pure domain.

> Ref: CX9-6204 · License: Apache-2.0 · Contract: [`CONTRACT.md`](CONTRACT.md)

## The problem it solves

`reserve stock → charge card → ship` touches three services with no shared
transaction. If the charge is declined the reservation must be released; if
the orchestrator dies mid-way it must resume **without charging twice**; if a
call times out you do not know whether it happened. This library encodes
those rules once, in a tested domain, instead of in ad-hoc `try/except`.

## Semantics

| Step kind | On give-up |
|-----------|------------|
| `COMPENSATABLE` | backward recovery: undo, newest first, every step that **may** have taken effect |
| `PIVOT` (≤ 1) | definitive rejection → backward recovery; **unknown outcome → `FAILED`** (never guesses commit vs rollback) |
| `RETRIABLE` (after pivot) | forward recovery only; exhausting retries → `FAILED`, never compensation |

- "May have taken effect" is sticky across attempts: a timeout on attempt 1
  followed by a permanent rejection on attempt 2 still compensates (the
  property tests found this case; see `test_permanent_after_unknown_attempt_still_compensates`).
- Handlers classify failures by raising `TransientStepError` (retry; effect
  unknown), `PermanentStepError` (guaranteed no effect) or timing out. Any
  other exception is treated as transient.
- Compensations are retried on every failure kind; exhausting them ends in
  `FAILED` with a reason for an operator.
- Idempotency key `"{saga_id}/{step}/{phase}"` is the same for every attempt
  and after crashes. `StepStarted` is persisted **before** the handler runs.

## Architecture

```
src/cortex9_saga/
├── domain/          # pure: definitions, events, Saga aggregate (State Machine + decider), recovery strategies
├── ports.py         # SagaStore (event store, optimistic concurrency), Clock, Sleeper, Random, StepHandler
├── application/     # SagaOrchestrator (asyncio), HandlerRegistry, error classification
└── adapters/        # InMemory/JsonFile stores (flock + atomic rename), codec, clocks, IdempotencyLedger
```

| Pattern | Where |
|---------|-------|
| Saga | the whole package |
| State Machine | `Saga` status/step transitions, `InvalidTransition` on illegal moves |
| Command | `Execute` / `Wait` / `Finish` returned by `Saga.next_command` |
| Strategy | `BackoffStrategy` (exponential, constant) and `RecoveryStrategy` (backward, forward, fail-stop) |
| Event Sourcing / Memento | `Saga.rehydrate(definition, events)` |
| Repository | `SagaStore` |

`scripts/check_architecture.py` (AST-based) fails CI if the domain imports
anything beyond `collections/dataclasses/enum/types/typing` or an outer layer.

## Quick start

```python
reg = HandlerRegistry()
reg.on("reserve-stock", reserve, compensate=release)
reg.on("charge-card", charge)  # PIVOT: no compensation
reg.on("ship", ship)  # RETRIABLE
o = SagaOrchestrator(
    [ORDER], reg, JsonFileSagaStore("sagas/"), SystemClock(), AsyncioSleeper(), SeededRandom(1)
)
o.start("place-order", "order-42", {"qty": 2, "card": "tok_..."})
saga = await o.run("order-42")  # COMPLETED | COMPENSATED | FAILED
await o.resume_incomplete()  # after a crash
```

Full runnable example: `python examples/order_saga.py`.

## Verification

```
python scripts/check_architecture.py
ruff check . && ruff format --check .
mypy                       # strict, package
pytest                     # 42 tests, incl. 2 Hypothesis suites × 300 examples
```

The property suites generate random definitions (0–4 compensatable steps,
optional pivot, 0–2 retriable), random per-attempt outcomes (ok, transient,
permanent, timeout, timeout-after-effect) and random crash points, and check:
terminal state, forward order, completed ⇒ all succeeded, reverse-order
compensation of exactly the steps that may have had effect, nothing
compensated after the pivot, stable keys, every effect applied exactly once,
and that a crash at any point resumes to the same outcome as a clean run.

## Honest limits (out of scope)

- Sequential steps only (no parallel fan-out).
- No choreography / message-broker integration; handlers are async callables.
- One orchestrator per saga at a time is enforced only by optimistic
  concurrency (+ `flock` for the JSON store), not by leases.
- `JsonFileSagaStore` rewrites the whole history per append: fine for
  thousands of events per saga, not for millions.
- Isolation between concurrent sagas (semantic locks, countermeasures) is the
  application's job.
- `IdempotencyLedger` is in-memory and illustrates the participant contract;
  real participants must store keys transactionally with their effect.
