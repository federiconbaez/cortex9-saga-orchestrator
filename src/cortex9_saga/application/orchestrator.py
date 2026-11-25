"""``SagaOrchestrator``: drives ``Saga`` aggregates against real handlers."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from cortex9_saga.application.errors import (
    MissingHandler,
    PermanentStepError,
    TransientStepError,
    UnknownSaga,
)
from cortex9_saga.domain.definition import SagaDefinition, StepKind
from cortex9_saga.domain.events import FailureKind, Phase, SagaStarted
from cortex9_saga.domain.saga import Execute, Finish, Saga, SagaStatus, Wait
from cortex9_saga.ports import Clock, Random, SagaStore, Sleeper, StepHandler


@dataclass(frozen=True, slots=True)
class StepContext:
    """What a handler receives. ``idempotency_key`` is stable across retries."""

    saga_id: str
    step: str
    phase: Phase
    attempt: int
    idempotency_key: str
    payload: Mapping[str, Any]
    results: Mapping[str, Mapping[str, Any]]
    forward_result: Mapping[str, Any] | None


class HandlerRegistry:
    """Maps step names to forward and compensation handlers."""

    def __init__(self) -> None:
        self._forward: dict[str, StepHandler] = {}
        self._compensate: dict[str, StepHandler] = {}

    def on(self, step: str, forward: StepHandler, compensate: StepHandler | None = None) -> None:
        self._forward[step] = forward
        if compensate is not None:
            self._compensate[step] = compensate

    def validate(self, definition: SagaDefinition) -> None:
        for s in definition.steps:
            if s.name not in self._forward:
                raise MissingHandler(f"{definition.name}: no forward handler for {s.name!r}")
            if s.kind is StepKind.COMPENSATABLE and s.name not in self._compensate:
                raise MissingHandler(f"{definition.name}: no compensation for {s.name!r}")

    def get(self, step: str, phase: Phase) -> StepHandler:
        table = self._forward if phase is Phase.FORWARD else self._compensate
        try:
            return table[step]
        except KeyError as e:
            raise MissingHandler(f"no {phase.value} handler for {step!r}") from e


class SagaOrchestrator:
    def __init__(
        self,
        definitions: Sequence[SagaDefinition],
        handlers: HandlerRegistry,
        store: SagaStore,
        clock: Clock,
        sleeper: Sleeper,
        random: Random,
    ) -> None:
        self._defs = {d.name: d for d in definitions}
        if len(self._defs) != len(definitions):
            raise ValueError("duplicate saga definition names")
        for d in definitions:
            handlers.validate(d)
        self._handlers = handlers
        self._store = store
        self._clock = clock
        self._sleeper = sleeper
        self._random = random

    def start(self, definition: str, saga_id: str, payload: Mapping[str, Any]) -> None:
        """Creates the saga. Idempotent for the same id, definition and payload."""
        existing = self._store.load(saga_id)
        if existing:
            first = existing[0]
            if (
                isinstance(first, SagaStarted)
                and first.definition == definition
                and dict(first.payload) == dict(payload)
            ):
                return
            raise ValueError(f"saga {saga_id!r} already exists with different parameters")
        saga = Saga.start(self._definition(definition), saga_id, payload, self._clock.now())
        self._commit(saga)

    def load(self, saga_id: str) -> Saga:
        events = self._store.load(saga_id)
        if not events or not isinstance(events[0], SagaStarted):
            raise UnknownSaga(saga_id)
        return Saga.rehydrate(self._definition(events[0].definition), events)

    async def run(self, saga_id: str) -> Saga:
        """Drives the saga until it reaches a terminal state."""
        saga = self.load(saga_id)
        while True:
            now = self._clock.now()
            cmd = saga.next_command(now)
            if isinstance(cmd, Finish):
                return saga
            if isinstance(cmd, Wait):
                await self._sleeper.sleep(max(0.0, cmd.until - now))
                continue
            saga.begin(cmd, now)
            self._commit(saga)  # write-ahead: the attempt is durable before it runs
            await self._execute(saga, cmd)
            self._commit(saga)

    async def resume_incomplete(self) -> dict[str, SagaStatus]:
        """Recovery after a crash: runs every non-terminal saga to completion."""
        out: dict[str, SagaStatus] = {}
        for sid in self._store.list_ids():
            saga = self.load(sid)
            if not saga.status.terminal:
                saga = await self.run(sid)
            out[sid] = saga.status
        return out

    async def _execute(self, saga: Saga, cmd: Execute) -> None:
        handler = self._handlers.get(cmd.step, cmd.phase)
        ctx = StepContext(
            saga_id=cmd.saga_id,
            step=cmd.step,
            phase=cmd.phase,
            attempt=cmd.attempt,
            idempotency_key=cmd.idempotency_key,
            payload=cmd.input["payload"],
            results=MappingProxyType(dict(cmd.input["results"])),
            forward_result=cmd.input.get("forward_result"),
        )
        kind: FailureKind
        try:
            result = await asyncio.wait_for(handler(ctx), timeout=cmd.timeout)
        except TimeoutError:
            kind, error = FailureKind.TIMEOUT, f"no response within {cmd.timeout}s"
        except PermanentStepError as e:
            kind, error = FailureKind.PERMANENT, str(e) or type(e).__name__
        except TransientStepError as e:
            kind, error = FailureKind.TRANSIENT, str(e) or type(e).__name__
        except Exception as e:
            kind, error = FailureKind.TRANSIENT, f"{type(e).__name__}: {e}"
        else:
            if not isinstance(result, Mapping):
                kind, error = FailureKind.TRANSIENT, "handler returned a non-mapping result"
            else:
                saga.record_success(cmd.step, cmd.phase, cmd.attempt, result, self._clock.now())
                return
        saga.record_failure(
            cmd.step, cmd.phase, cmd.attempt, kind, error, self._clock.now(), self._random.random()
        )

    def _commit(self, saga: Saga) -> None:
        if not saga.pending_events:
            return
        self._store.append(saga.saga_id, saga.version, list(saga.pending_events))
        saga.mark_committed()

    def _definition(self, name: str) -> SagaDefinition:
        try:
            return self._defs[name]
        except KeyError as e:
            raise UnknownSaga(f"unknown saga definition {name!r}") from e
