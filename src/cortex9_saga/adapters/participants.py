"""Helpers for writing idempotent saga participants."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from cortex9_saga.application.orchestrator import StepContext
from cortex9_saga.ports import StepHandler

Effect = Callable[[StepContext], Awaitable[Mapping[str, Any]]]


class IdempotencyLedger:
    """In-memory dedupe table keyed by ``StepContext.idempotency_key``.

    A real participant keeps this table in the same database transaction as
    its effect; this adapter shows the contract and backs the tests.
    """

    def __init__(self) -> None:
        self._done: dict[str, Mapping[str, Any]] = {}
        self.effects: dict[str, int] = {}

    def wrap(self, effect: Effect) -> StepHandler:
        async def handler(ctx: StepContext) -> Mapping[str, Any]:
            if ctx.idempotency_key in self._done:
                return self._done[ctx.idempotency_key]
            result = await effect(ctx)
            self._done[ctx.idempotency_key] = dict(result)
            self.effects[ctx.idempotency_key] = self.effects.get(ctx.idempotency_key, 0) + 1
            return result

        return handler
