"""Scripted participants and a crashing store for orchestrator tests."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from cortex9_saga import HandlerRegistry, PermanentStepError, StepContext, TransientStepError
from cortex9_saga.adapters.participants import IdempotencyLedger
from cortex9_saga.domain.definition import SagaDefinition, StepKind
from cortex9_saga.domain.events import Event
from cortex9_saga.ports import SagaStore

# Outcomes a scripted participant can produce on a given attempt.
OK = "ok"
TRANSIENT = "transient"  # rejected before acting
PERMANENT = "permanent"  # rejected, guaranteed no effect
TIMEOUT = "timeout"  # no answer, nothing done
TIMEOUT_AFTER = "timeout_after_effect"  # acted, then the answer was lost


class SimulatedCrash(BaseException):
    """Escapes every ``except Exception``: models the process dying."""


class Scripted:
    def __init__(self, definition: SagaDefinition, script: Mapping[str, Sequence[str]]) -> None:
        self.definition = definition
        self.script = script
        self.ledger = IdempotencyLedger()
        self.calls: list[tuple[str, str, int, str]] = []
        self.registry = HandlerRegistry()
        for s in definition.steps:
            comp = self._handler(s.name, "compensate") if s.kind is StepKind.COMPENSATABLE else None
            self.registry.on(s.name, self._handler(s.name, "forward"), comp)

    def _handler(self, step: str, phase: str) -> Any:
        async def effect(ctx: StepContext) -> Mapping[str, Any]:
            return {"step": step, "phase": phase}

        idempotent = self.ledger.wrap(effect)

        async def handler(ctx: StepContext) -> Mapping[str, Any]:
            self.calls.append((step, phase, ctx.attempt, ctx.idempotency_key))
            seq = self.script.get(f"{step}:{phase}", [OK])
            outcome = seq[min(ctx.attempt - 1, len(seq) - 1)]
            if outcome == OK:
                return await idempotent(ctx)
            if outcome == TRANSIENT:
                raise TransientStepError("try later")
            if outcome == PERMANENT:
                raise PermanentStepError("rejected")
            if outcome == TIMEOUT:
                raise TimeoutError
            if outcome == TIMEOUT_AFTER:
                await idempotent(ctx)
                raise TimeoutError
            raise AssertionError(outcome)

        return handler

    def applied(self, step: str, phase: str, saga_id: str = "s1") -> bool:
        return f"{saga_id}/{step}/{phase}" in self.ledger.effects


class CrashingStore:
    """Delegates to ``inner`` but dies on the N-th append (before writing)."""

    def __init__(self, inner: SagaStore, crash_at: int | None) -> None:
        self.inner = inner
        self.crash_at = crash_at
        self.appends = 0

    def append(self, saga_id: str, expected_version: int, events: Sequence[Event]) -> None:
        self.appends += 1
        if self.crash_at is not None and self.appends == self.crash_at:
            raise SimulatedCrash
        self.inner.append(saga_id, expected_version, events)

    def load(self, saga_id: str) -> Sequence[Event]:
        return self.inner.load(saga_id)

    def list_ids(self) -> Sequence[str]:
        return self.inner.list_ids()
