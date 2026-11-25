"""Order placement: reserve stock -> charge card (pivot) -> ship (retriable).

Run: ``python examples/order_saga.py`` (no dependencies). The card is declined
on the first order, so stock is released; the second order completes.
"""

from __future__ import annotations

import asyncio
import tempfile
from collections.abc import Mapping
from typing import Any

from cortex9_saga import (
    HandlerRegistry,
    PermanentStepError,
    RetryPolicy,
    SagaDefinition,
    SagaOrchestrator,
    StepContext,
    StepDefinition,
    StepKind,
)
from cortex9_saga.adapters.participants import IdempotencyLedger
from cortex9_saga.adapters.runtime import AsyncioSleeper, SeededRandom, SystemClock
from cortex9_saga.adapters.stores import JsonFileSagaStore

ORDER = SagaDefinition(
    "place-order",
    (
        StepDefinition("reserve-stock", timeout=2),
        StepDefinition("charge-card", timeout=5, kind=StepKind.PIVOT),
        StepDefinition("ship", timeout=5, kind=StepKind.RETRIABLE, retry=RetryPolicy(5)),
    ),
)

stock = {"sku-1": 3}
ledger = IdempotencyLedger()


async def reserve(ctx: StepContext) -> Mapping[str, Any]:
    stock["sku-1"] -= ctx.payload["qty"]
    return {"reserved": ctx.payload["qty"]}


async def release(ctx: StepContext) -> Mapping[str, Any]:
    if ctx.forward_result:  # nothing to undo if the reservation never happened
        stock["sku-1"] += ctx.forward_result["reserved"]
    return {"released": True}


async def charge(ctx: StepContext) -> Mapping[str, Any]:
    if ctx.payload["card"] == "declined":
        raise PermanentStepError("card declined by issuer")
    return {"charge_id": f"ch_{ctx.saga_id}"}


async def ship(ctx: StepContext) -> Mapping[str, Any]:
    return {"tracking": f"TRK-{ctx.saga_id}"}


async def main() -> None:
    reg = HandlerRegistry()
    reg.on("reserve-stock", ledger.wrap(reserve), ledger.wrap(release))
    reg.on("charge-card", ledger.wrap(charge))
    reg.on("ship", ledger.wrap(ship))
    with tempfile.TemporaryDirectory() as d:
        o = SagaOrchestrator(
            [ORDER], reg, JsonFileSagaStore(d), SystemClock(), AsyncioSleeper(), SeededRandom(1)
        )
        o.start("place-order", "order-1", {"qty": 2, "card": "declined"})
        o.start("place-order", "order-2", {"qty": 1, "card": "ok"})
        for sid in ("order-1", "order-2"):
            saga = await o.run(sid)
            print(f"{sid}: {saga.status.value:12} {saga.reason}")
        print(f"stock left: {stock['sku-1']} (expected 2)")
        assert stock["sku-1"] == 2


if __name__ == "__main__":
    asyncio.run(main())
