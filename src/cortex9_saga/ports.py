"""Ports (driven side). Implementations live in ``cortex9_saga.adapters``."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Protocol

from cortex9_saga.domain.events import Event

if TYPE_CHECKING:
    from cortex9_saga.application.orchestrator import StepContext


class ConcurrencyError(Exception):
    """The saga's history changed since it was loaded."""


class SagaStore(Protocol):
    """Event store (Repository) with optimistic concurrency."""

    def append(self, saga_id: str, expected_version: int, events: Sequence[Event]) -> None:
        """Atomically append ``events`` iff the stored history has ``expected_version`` events."""
        ...

    def load(self, saga_id: str) -> Sequence[Event]: ...

    def list_ids(self) -> Sequence[str]: ...


class Clock(Protocol):
    def now(self) -> float: ...


class Sleeper(Protocol):
    async def sleep(self, seconds: float) -> None: ...


class Random(Protocol):
    def random(self) -> float:
        """Uniform in [0, 1)."""
        ...


StepHandler = Callable[["StepContext"], Awaitable[Mapping[str, Any]]]
