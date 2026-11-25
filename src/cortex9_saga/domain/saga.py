"""The ``Saga`` aggregate: an event-sourced state machine and decider.

The application asks ``next_command(now)`` what to do, announces the attempt
with ``begin`` (write-ahead), runs it, and reports the outcome with
``record_success`` / ``record_failure``. Every mutation is an event appended to
``pending_events``; ``Saga.rehydrate`` rebuilds identical state from history.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import Any

from cortex9_saga.domain.definition import SagaDefinition, StepDefinition
from cortex9_saga.domain.errors import InvalidTransition
from cortex9_saga.domain.events import (
    CompensationTriggered,
    Event,
    FailureKind,
    FinalStatus,
    Phase,
    SagaFinished,
    SagaStarted,
    StepFailed,
    StepStarted,
    StepSucceeded,
)
from cortex9_saga.domain.recovery import FailureContext, strategy_for


class SagaStatus(StrEnum):
    NEW = "new"
    RUNNING = "running"
    COMPENSATING = "compensating"
    COMPLETED = "completed"
    COMPENSATED = "compensated"
    FAILED = "failed"

    @property
    def terminal(self) -> bool:
        return self in (SagaStatus.COMPLETED, SagaStatus.COMPENSATED, SagaStatus.FAILED)


class StepStatus(StrEnum):
    PENDING = "pending"
    IN_FLIGHT = "in_flight"
    WAITING_RETRY = "waiting_retry"
    SUCCEEDED = "succeeded"
    FAILED_NO_EFFECT = "failed_no_effect"
    FAILED_UNKNOWN = "failed_unknown"


@dataclass(slots=True)
class PhaseState:
    status: StepStatus = StepStatus.PENDING
    attempts: int = 0
    retry_at: float | None = None
    result: Mapping[str, Any] | None = None
    last_error: str | None = None
    maybe_effect: bool = False
    """Sticky: some attempt ended without a definitive answer."""


# ----- commands (Command pattern) ------------------------------------------


@dataclass(frozen=True, slots=True)
class Execute:
    saga_id: str
    step: str
    phase: Phase
    attempt: int
    idempotency_key: str
    timeout: float
    input: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class Wait:
    until: float


@dataclass(frozen=True, slots=True)
class Finish:
    status: SagaStatus
    reason: str


Command = Execute | Wait | Finish

_FINAL = {
    FinalStatus.COMPLETED: SagaStatus.COMPLETED,
    FinalStatus.COMPENSATED: SagaStatus.COMPENSATED,
    FinalStatus.FAILED: SagaStatus.FAILED,
}


def idempotency_key(saga_id: str, step: str, phase: Phase) -> str:
    """Identical for every attempt and every process: the participant's dedupe key."""
    return f"{saga_id}/{step}/{phase.value}"


@dataclass
class Saga:
    definition: SagaDefinition
    saga_id: str = ""
    payload: Mapping[str, Any] = field(default_factory=dict)
    status: SagaStatus = SagaStatus.NEW
    reason: str = ""
    forward: dict[str, PhaseState] = field(default_factory=dict)
    compensation: dict[str, PhaseState] = field(default_factory=dict)
    cursor: int = 0
    to_compensate: tuple[str, ...] = ()
    comp_cursor: int = 0
    history: list[Event] = field(default_factory=list)
    pending_events: list[Event] = field(default_factory=list)

    # ----- construction ------------------------------------------------------

    @classmethod
    def start(
        cls, definition: SagaDefinition, saga_id: str, payload: Mapping[str, Any], now: float
    ) -> Saga:
        if not saga_id or "/" in saga_id:
            raise InvalidTransition(f"invalid saga id {saga_id!r}")
        saga = cls(definition)
        saga._record(SagaStarted(saga_id, definition.name, dict(payload), now))
        return saga

    @classmethod
    def rehydrate(cls, definition: SagaDefinition, events: Sequence[Event]) -> Saga:
        """Memento-style restore: fold the persisted history."""
        saga = cls(definition)
        for e in events:
            saga._apply(e)
            saga.history.append(e)
        return saga

    @property
    def version(self) -> int:
        """Number of events already persisted (optimistic-concurrency token)."""
        return len(self.history) - len(self.pending_events)

    def mark_committed(self) -> list[Event]:
        out, self.pending_events = self.pending_events, []
        return out

    # ----- decisions ---------------------------------------------------------

    def next_command(self, now: float) -> Command:
        if self.status is SagaStatus.NEW:
            raise InvalidTransition("saga not started")
        if self.status.terminal:
            return Finish(self.status, self.reason)
        if self.status is SagaStatus.RUNNING:
            step = self.definition.steps[self.cursor]
            return self._command_for(step, Phase.FORWARD, self.forward[step.name], now)
        name = self.to_compensate[self.comp_cursor]
        step = self.definition.step(name)
        return self._command_for(step, Phase.COMPENSATE, self.compensation[name], now)

    def _command_for(
        self, step: StepDefinition, phase: Phase, st: PhaseState, now: float
    ) -> Command:
        if st.status is StepStatus.WAITING_RETRY:
            assert st.retry_at is not None
            if now < st.retry_at:
                return Wait(st.retry_at)
            attempt = st.attempts + 1
        elif st.status is StepStatus.IN_FLIGHT:
            attempt = st.attempts  # crashed mid-attempt: same attempt, same key
        elif st.status is StepStatus.PENDING:
            attempt = 1
        else:  # pragma: no cover - guarded by cursor bookkeeping
            raise InvalidTransition(f"step {step.name} in unexpected state {st.status}")
        return Execute(
            saga_id=self.saga_id,
            step=step.name,
            phase=phase,
            attempt=attempt,
            idempotency_key=idempotency_key(self.saga_id, step.name, phase),
            timeout=step.timeout,
            input=self._input_for(step.name, phase),
        )

    def _input_for(self, step: str, phase: Phase) -> Mapping[str, Any]:
        results = {
            name: st.result
            for name, st in self.forward.items()
            if st.status is StepStatus.SUCCEEDED and st.result is not None
        }
        data: dict[str, Any] = {"payload": self.payload, "results": results}
        if phase is Phase.COMPENSATE:
            data["forward_result"] = self.forward[step].result
        return MappingProxyType(data)

    # ----- recording outcomes -----------------------------------------------

    def begin(self, cmd: Execute, now: float) -> None:
        expected = self.next_command(now)
        if not isinstance(expected, Execute) or (
            expected.step,
            expected.phase,
            expected.attempt,
        ) != (cmd.step, cmd.phase, cmd.attempt):
            raise InvalidTransition(f"cannot begin {cmd.step}/{cmd.phase} now: expected {expected}")
        self._record(StepStarted(cmd.step, cmd.phase, cmd.attempt, now))

    def record_success(
        self, step: str, phase: Phase, attempt: int, result: Mapping[str, Any], now: float
    ) -> None:
        self._require_in_flight(step, phase, attempt)
        self._record(StepSucceeded(step, phase, attempt, dict(result), now))
        if phase is Phase.FORWARD and self.cursor == len(self.definition.steps):
            self._record(SagaFinished(FinalStatus.COMPLETED, "all steps succeeded", now))
        elif phase is Phase.COMPENSATE and self.comp_cursor == len(self.to_compensate):
            self._record(SagaFinished(FinalStatus.COMPENSATED, self.reason, now))

    def record_failure(
        self,
        step: str,
        phase: Phase,
        attempt: int,
        kind: FailureKind,
        error: str,
        now: float,
        unit_random: float,
    ) -> None:
        self._require_in_flight(step, phase, attempt)
        sdef = self.definition.step(step)
        if phase is Phase.FORWARD:
            policy = sdef.retry
            retryable = kind is not FailureKind.PERMANENT and attempt < policy.max_attempts
        else:
            # Compensations must eventually succeed: retry every failure kind.
            policy = sdef.compensation_retry
            retryable = attempt < policy.max_attempts
        if retryable:
            delay = policy.backoff.delay(attempt, unit_random)
            self._record(StepFailed(step, phase, attempt, kind, error, now, now + delay))
            return
        self._record(StepFailed(step, phase, attempt, kind, error, now, None))
        if phase is Phase.COMPENSATE:
            self._record(
                SagaFinished(
                    FinalStatus.FAILED,
                    f"compensation of {step!r} exhausted ({kind.value}: {error}); "
                    "manual intervention required",
                    now,
                )
            )
            return
        succeeded = [s.name for s in self.definition.steps[: self.cursor]]
        maybe = self.forward[step].maybe_effect
        ctx = FailureContext(self.definition, step, kind, maybe, error, succeeded, now)
        event = strategy_for(sdef.kind, maybe).recover(ctx)
        self._record(event)
        if isinstance(event, CompensationTriggered) and not event.to_compensate:
            self._record(SagaFinished(FinalStatus.COMPENSATED, event.reason, now))

    def _require_in_flight(self, step: str, phase: Phase, attempt: int) -> None:
        table = self.forward if phase is Phase.FORWARD else self.compensation
        st = table.get(step)
        if st is None or st.status is not StepStatus.IN_FLIGHT or st.attempts != attempt:
            raise InvalidTransition(f"{step}/{phase} attempt {attempt} is not in flight")

    # ----- event application --------------------------------------------------

    def _record(self, e: Event) -> None:
        self._apply(e)
        self.history.append(e)
        self.pending_events.append(e)

    def _apply(self, e: Event) -> None:
        match e:
            case SagaStarted(saga_id=sid, payload=payload):
                if self.status is not SagaStatus.NEW:
                    raise InvalidTransition("saga already started")
                self.saga_id = sid
                self.payload = MappingProxyType(dict(payload))
                self.status = SagaStatus.RUNNING
                self.forward = {s.name: PhaseState() for s in self.definition.steps}
            case StepStarted(step=step, phase=phase, attempt=attempt):
                st = self._phase_state(step, phase)
                st.status, st.attempts, st.retry_at = StepStatus.IN_FLIGHT, attempt, None
            case StepSucceeded(step=step, phase=phase, result=result):
                st = self._phase_state(step, phase)
                st.status, st.result = StepStatus.SUCCEEDED, MappingProxyType(dict(result))
                if phase is Phase.FORWARD:
                    self.cursor += 1
                else:
                    self.comp_cursor += 1
            case StepFailed(step=step, phase=phase, kind=kind, error=err, retry_at=retry_at):
                st = self._phase_state(step, phase)
                st.last_error = err
                st.maybe_effect = st.maybe_effect or kind is not FailureKind.PERMANENT
                if retry_at is not None:
                    st.status, st.retry_at = StepStatus.WAITING_RETRY, retry_at
                elif not st.maybe_effect and phase is Phase.FORWARD:
                    st.status = StepStatus.FAILED_NO_EFFECT
                else:
                    st.status = StepStatus.FAILED_UNKNOWN
            case CompensationTriggered(to_compensate=steps, reason=reason):
                self.status = SagaStatus.COMPENSATING
                self.reason = reason
                self.to_compensate = tuple(steps)
                self.comp_cursor = 0
                self.compensation = {s: PhaseState() for s in steps}
            case SagaFinished(status=status, reason=reason):
                self.status = _FINAL[status]
                self.reason = reason

    def _phase_state(self, step: str, phase: Phase) -> PhaseState:
        table = self.forward if phase is Phase.FORWARD else self.compensation
        if step not in table:
            raise InvalidTransition(f"unknown {phase.value} step {step!r}")
        return table[step]
