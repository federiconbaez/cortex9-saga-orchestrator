"""Domain events. The saga's state is a left fold over these."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class Phase(StrEnum):
    FORWARD = "forward"
    COMPENSATE = "compensate"


class FailureKind(StrEnum):
    TRANSIENT = "transient"
    """May be retried; the effect may or may not have happened."""
    PERMANENT = "permanent"
    """Definitive rejection; the participant guarantees no effect."""
    TIMEOUT = "timeout"
    """No answer in time; the effect is unknown."""


class FinalStatus(StrEnum):
    COMPLETED = "completed"
    COMPENSATED = "compensated"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class SagaStarted:
    saga_id: str
    definition: str
    payload: Mapping[str, Any]
    at: float


@dataclass(frozen=True, slots=True)
class StepStarted:
    step: str
    phase: Phase
    attempt: int
    at: float


@dataclass(frozen=True, slots=True)
class StepSucceeded:
    step: str
    phase: Phase
    attempt: int
    result: Mapping[str, Any]
    at: float


@dataclass(frozen=True, slots=True)
class StepFailed:
    step: str
    phase: Phase
    attempt: int
    kind: FailureKind
    error: str
    at: float
    retry_at: float | None
    """Set when another attempt is scheduled; ``None`` means give up."""


@dataclass(frozen=True, slots=True)
class CompensationTriggered:
    failed_step: str
    reason: str
    to_compensate: tuple[str, ...]
    """Steps to undo, already in execution (reverse) order."""
    at: float


@dataclass(frozen=True, slots=True)
class SagaFinished:
    status: FinalStatus
    reason: str
    at: float


Event = (
    SagaStarted | StepStarted | StepSucceeded | StepFailed | CompensationTriggered | SagaFinished
)
