"""Declarative saga definitions and retry/backoff strategies."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum
from typing import Protocol

from cortex9_saga.domain.errors import InvalidDefinition


class StepKind(IntEnum):
    """Ordering matters: kinds must be non-decreasing along a definition."""

    COMPENSATABLE = 0
    PIVOT = 1
    RETRIABLE = 2


class BackoffStrategy(Protocol):
    """Strategy pattern: delay before retry number ``attempt + 1``."""

    def delay(self, attempt: int, unit_random: float) -> float: ...


@dataclass(frozen=True, slots=True)
class ExponentialBackoff:
    initial: float = 0.1
    multiplier: float = 2.0
    maximum: float = 30.0
    jitter: float = 0.2
    """Fraction of symmetric jitter, 0 disables it."""

    def __post_init__(self) -> None:
        if self.initial < 0 or self.multiplier < 1 or self.maximum < self.initial:
            raise InvalidDefinition("invalid exponential backoff parameters")
        if not 0 <= self.jitter <= 1:
            raise InvalidDefinition("jitter must be within [0, 1]")

    def delay(self, attempt: int, unit_random: float) -> float:
        base = min(self.maximum, self.initial * self.multiplier ** max(0, attempt - 1))
        spread = base * self.jitter * (2 * unit_random - 1)
        return max(0.0, min(self.maximum, base + spread))


@dataclass(frozen=True, slots=True)
class ConstantBackoff:
    seconds: float = 0.0

    def delay(self, attempt: int, unit_random: float) -> float:
        return self.seconds


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 3
    backoff: BackoffStrategy = field(default_factory=ExponentialBackoff)

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise InvalidDefinition("max_attempts must be >= 1")


@dataclass(frozen=True, slots=True)
class StepDefinition:
    name: str
    timeout: float = 30.0
    kind: StepKind = StepKind.COMPENSATABLE
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    compensation_retry: RetryPolicy = field(default_factory=lambda: RetryPolicy(max_attempts=10))

    def __post_init__(self) -> None:
        if not self.name or "/" in self.name:
            raise InvalidDefinition(f"invalid step name {self.name!r}")
        if self.timeout <= 0:
            raise InvalidDefinition(f"step {self.name!r}: timeout must be > 0")


@dataclass(frozen=True, slots=True)
class SagaDefinition:
    name: str
    steps: tuple[StepDefinition, ...]

    def __post_init__(self) -> None:
        if not self.name:
            raise InvalidDefinition("saga name required")
        if not self.steps:
            raise InvalidDefinition("a saga needs at least one step")
        names = [s.name for s in self.steps]
        if len(set(names)) != len(names):
            raise InvalidDefinition("step names must be unique")
        kinds = [s.kind for s in self.steps]
        if kinds != sorted(kinds):
            raise InvalidDefinition("step kinds must be ordered COMPENSATABLE* PIVOT? RETRIABLE*")
        if kinds.count(StepKind.PIVOT) > 1:
            raise InvalidDefinition("at most one pivot step")

    def step(self, name: str) -> StepDefinition:
        for s in self.steps:
            if s.name == name:
                return s
        raise KeyError(name)

    def index(self, name: str) -> int:
        return [s.name for s in self.steps].index(name)
