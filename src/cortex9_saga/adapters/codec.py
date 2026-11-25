"""Event <-> JSON-compatible dict mapping (DTOs stay out of the domain)."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

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


class CodecError(ValueError):
    pass


def to_dict(e: Event) -> dict[str, Any]:
    match e:
        case SagaStarted():
            return {
                "type": "saga_started",
                "saga_id": e.saga_id,
                "definition": e.definition,
                "payload": dict(e.payload),
                "at": e.at,
            }
        case StepStarted():
            return {
                "type": "step_started",
                "step": e.step,
                "phase": e.phase.value,
                "attempt": e.attempt,
                "at": e.at,
            }
        case StepSucceeded():
            return {
                "type": "step_succeeded",
                "step": e.step,
                "phase": e.phase.value,
                "attempt": e.attempt,
                "result": dict(e.result),
                "at": e.at,
            }
        case StepFailed():
            return {
                "type": "step_failed",
                "step": e.step,
                "phase": e.phase.value,
                "attempt": e.attempt,
                "kind": e.kind.value,
                "error": e.error,
                "at": e.at,
                "retry_at": e.retry_at,
            }
        case CompensationTriggered():
            return {
                "type": "compensation_triggered",
                "failed_step": e.failed_step,
                "reason": e.reason,
                "to_compensate": list(e.to_compensate),
                "at": e.at,
            }
        case SagaFinished():
            return {
                "type": "saga_finished",
                "status": e.status.value,
                "reason": e.reason,
                "at": e.at,
            }
    raise CodecError(f"unknown event {e!r}")  # pragma: no cover


def from_dict(d: Mapping[str, Any]) -> Event:
    try:
        t = d["type"]
        if t == "saga_started":
            return SagaStarted(d["saga_id"], d["definition"], dict(d["payload"]), float(d["at"]))
        if t == "step_started":
            return StepStarted(d["step"], Phase(d["phase"]), int(d["attempt"]), float(d["at"]))
        if t == "step_succeeded":
            return StepSucceeded(
                d["step"], Phase(d["phase"]), int(d["attempt"]), dict(d["result"]), float(d["at"])
            )
        if t == "step_failed":
            r = d["retry_at"]
            return StepFailed(
                d["step"],
                Phase(d["phase"]),
                int(d["attempt"]),
                FailureKind(d["kind"]),
                str(d["error"]),
                float(d["at"]),
                None if r is None else float(r),
            )
        if t == "compensation_triggered":
            return CompensationTriggered(
                d["failed_step"], d["reason"], tuple(d["to_compensate"]), float(d["at"])
            )
        if t == "saga_finished":
            return SagaFinished(FinalStatus(d["status"]), d["reason"], float(d["at"]))
    except (KeyError, TypeError, ValueError) as e:
        raise CodecError(f"malformed event: {e}") from e
    raise CodecError(f"unknown event type {d.get('type')!r}")
