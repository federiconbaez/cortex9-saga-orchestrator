"""Recovery strategies applied when a forward step gives up (Strategy pattern).

* ``BackwardRecovery`` — undo every step that may have taken effect, newest first.
* ``ForwardRecovery`` — past the pivot nothing is undone; exhausting retries
  stops the saga for an operator.
* ``FailStop`` — the pivot's outcome is unknown; guessing either way could be
  wrong, so stop for an operator.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

from cortex9_saga.domain.definition import SagaDefinition, StepKind
from cortex9_saga.domain.events import (
    CompensationTriggered,
    Event,
    FailureKind,
    FinalStatus,
    SagaFinished,
)


@dataclass(frozen=True, slots=True)
class FailureContext:
    definition: SagaDefinition
    failed_step: str
    kind: FailureKind
    """Kind of the final failure."""
    may_have_effect: bool
    """True if *any* attempt ended without a definitive answer (timeout or
    transient): a later permanent rejection does not prove the earlier attempt
    did nothing."""
    error: str
    succeeded: Sequence[str]
    """Forward steps that succeeded, in execution order."""
    now: float


class RecoveryStrategy(Protocol):
    def recover(self, ctx: FailureContext) -> Event: ...


class BackwardRecovery:
    def recover(self, ctx: FailureContext) -> Event:
        failed_kind = ctx.definition.step(ctx.failed_step).kind
        undo = [s for s in ctx.succeeded if ctx.definition.step(s).kind is StepKind.COMPENSATABLE]
        if ctx.may_have_effect and failed_kind is StepKind.COMPENSATABLE:
            undo.append(ctx.failed_step)
        return CompensationTriggered(
            failed_step=ctx.failed_step,
            reason=f"{ctx.kind.value}: {ctx.error}",
            to_compensate=tuple(reversed(undo)),
            at=ctx.now,
        )


class ForwardRecovery:
    def recover(self, ctx: FailureContext) -> Event:
        return SagaFinished(
            status=FinalStatus.FAILED,
            reason=(
                f"retriable step {ctx.failed_step!r} gave up after the pivot "
                f"({ctx.kind.value}: {ctx.error}); manual completion required"
            ),
            at=ctx.now,
        )


class FailStop:
    def recover(self, ctx: FailureContext) -> Event:
        return SagaFinished(
            status=FinalStatus.FAILED,
            reason=(
                f"pivot {ctx.failed_step!r} outcome unknown ({ctx.kind.value}: {ctx.error}); "
                "refusing to guess between commit and rollback"
            ),
            at=ctx.now,
        )


def strategy_for(kind: StepKind, may_have_effect: bool) -> RecoveryStrategy:
    if kind is StepKind.RETRIABLE:
        return ForwardRecovery()
    if kind is StepKind.PIVOT and may_have_effect:
        return FailStop()
    return BackwardRecovery()
