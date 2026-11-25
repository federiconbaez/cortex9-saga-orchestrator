from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest

from cortex9_saga import (
    ConstantBackoff,
    HandlerRegistry,
    PermanentStepError,
    RetryPolicy,
    SagaDefinition,
    SagaOrchestrator,
    SagaStatus,
    StepContext,
    StepDefinition,
    StepKind,
)
from cortex9_saga.adapters.codec import CodecError, from_dict, to_dict
from cortex9_saga.adapters.runtime import SeededRandom, VirtualTime
from cortex9_saga.adapters.stores import InMemorySagaStore, JsonFileSagaStore
from cortex9_saga.application.errors import MissingHandler, UnknownSaga
from cortex9_saga.domain.events import Phase
from cortex9_saga.ports import ConcurrencyError, SagaStore

from conftest import run
from support import OK, PERMANENT, TIMEOUT_AFTER, TRANSIENT, CrashingStore, Scripted, SimulatedCrash

FAST = RetryPolicy(3, ConstantBackoff(1.0))


def travel() -> SagaDefinition:
    return SagaDefinition(
        "travel",
        (
            StepDefinition("flight", retry=FAST, compensation_retry=FAST),
            StepDefinition("hotel", retry=FAST, compensation_retry=FAST),
            StepDefinition("payment", kind=StepKind.PIVOT, retry=FAST),
            StepDefinition("email", kind=StepKind.RETRIABLE, retry=FAST),
        ),
    )


def orchestrator(sc: Scripted, store: SagaStore, vt: VirtualTime) -> SagaOrchestrator:
    return SagaOrchestrator([sc.definition], sc.registry, store, vt, vt, SeededRandom(1))


def test_happy_path_with_results_flowing_between_steps() -> None:
    seen: dict[str, Any] = {}

    async def flight(ctx: StepContext) -> Mapping[str, Any]:
        return {"pnr": "ABC123", "pax": ctx.payload["pax"]}

    async def hotel(ctx: StepContext) -> Mapping[str, Any]:
        seen["flight"] = ctx.results["flight"]
        return {"room": 12}

    async def ok(ctx: StepContext) -> Mapping[str, Any]:
        return {}

    reg = HandlerRegistry()
    reg.on("flight", flight, ok)
    reg.on("hotel", hotel, ok)
    reg.on("payment", ok)
    reg.on("email", ok)
    vt = VirtualTime()
    store = InMemorySagaStore()
    o = SagaOrchestrator([travel()], reg, store, vt, vt, SeededRandom(0))
    o.start("travel", "trip-1", {"pax": 2})
    saga = run(lambda: o.run("trip-1"))
    assert saga.status is SagaStatus.COMPLETED
    assert seen["flight"] == {"pnr": "ABC123", "pax": 2}


def test_failure_compensates_with_forward_result() -> None:
    got: dict[str, Any] = {}

    async def reserve(ctx: StepContext) -> Mapping[str, Any]:
        return {"reservation": "R-9"}

    async def cancel(ctx: StepContext) -> Mapping[str, Any]:
        got["fr"] = ctx.forward_result
        got["key"] = ctx.idempotency_key
        return {"cancelled": True}

    async def fail(ctx: StepContext) -> Mapping[str, Any]:
        raise PermanentStepError("card declined")

    async def ok(ctx: StepContext) -> Mapping[str, Any]:
        return {}

    reg = HandlerRegistry()
    reg.on("flight", reserve, cancel)
    reg.on("hotel", fail, ok)
    reg.on("payment", ok)
    reg.on("email", ok)
    vt = VirtualTime()
    o = SagaOrchestrator([travel()], reg, InMemorySagaStore(), vt, vt, SeededRandom(0))
    o.start("travel", "t", {})
    saga = run(lambda: o.run("t"))
    assert saga.status is SagaStatus.COMPENSATED
    assert got == {"fr": {"reservation": "R-9"}, "key": "t/flight/compensate"}
    assert "card declined" in saga.reason


def test_real_timeout_is_enforced() -> None:
    async def hang(ctx: StepContext) -> Mapping[str, Any]:
        await asyncio.sleep(10)
        return {}

    async def ok(ctx: StepContext) -> Mapping[str, Any]:
        return {}

    d = SagaDefinition(
        "t",
        (
            StepDefinition("a", retry=FAST, compensation_retry=FAST),
            StepDefinition("b", timeout=0.02, retry=RetryPolicy(2, ConstantBackoff(0))),
        ),
    )
    reg = HandlerRegistry()
    reg.on("a", ok, ok)
    reg.on("b", hang, ok)
    vt = VirtualTime()
    o = SagaOrchestrator([d], reg, InMemorySagaStore(), vt, vt, SeededRandom(0))
    o.start("t", "x", {})
    saga = run(lambda: o.run("x"))
    # b timed out (unknown effect) so both b and a are compensated, newest first.
    assert saga.to_compensate == ("b", "a")
    assert saga.status is SagaStatus.COMPENSATED


def test_retries_use_backoff_and_stable_key() -> None:
    sc = Scripted(travel(), {"hotel:forward": [TRANSIENT, TRANSIENT, OK]})
    vt = VirtualTime()
    o = orchestrator(sc, InMemorySagaStore(), vt)
    o.start("travel", "s1", {})
    saga = run(lambda: o.run("s1"))
    assert saga.status is SagaStatus.COMPLETED
    hotel_keys = {k for s, p, _, k in sc.calls if s == "hotel"}
    assert hotel_keys == {"s1/hotel/forward"}
    assert vt.slept == [1.0, 1.0]


def test_lost_response_is_deduplicated_by_participant() -> None:
    sc = Scripted(travel(), {"flight:forward": [TIMEOUT_AFTER, OK]})
    vt = VirtualTime()
    o = orchestrator(sc, InMemorySagaStore(), vt)
    o.start("travel", "s1", {})
    saga = run(lambda: o.run("s1"))
    assert saga.status is SagaStatus.COMPLETED
    assert sc.ledger.effects["s1/flight/forward"] == 1


def test_unknown_exception_counts_as_transient() -> None:
    calls = {"n": 0}

    async def flaky(ctx: StepContext) -> Mapping[str, Any]:
        calls["n"] += 1
        if calls["n"] == 1:
            raise ConnectionError("reset by peer")
        return {}

    async def ok(ctx: StepContext) -> Mapping[str, Any]:
        return {}

    reg = HandlerRegistry()
    for s in ("flight", "hotel"):
        reg.on(s, ok, ok)
    reg.on("payment", flaky)
    reg.on("email", ok)
    vt = VirtualTime()
    o = SagaOrchestrator([travel()], reg, InMemorySagaStore(), vt, vt, SeededRandom(0))
    o.start("travel", "s", {})
    assert run(lambda: o.run("s")).status is SagaStatus.COMPLETED
    assert calls["n"] == 2


def test_non_mapping_result_is_a_failure() -> None:
    async def bad(ctx: StepContext) -> Any:
        return 42

    async def ok(ctx: StepContext) -> Mapping[str, Any]:
        return {}

    d = SagaDefinition("d", (StepDefinition("a", kind=StepKind.PIVOT, retry=RetryPolicy(1)),))
    reg = HandlerRegistry()
    reg.on("a", bad)
    vt = VirtualTime()
    o = SagaOrchestrator([d], reg, InMemorySagaStore(), vt, vt, SeededRandom(0))
    o.start("d", "s", {})
    saga = run(lambda: o.run("s"))
    assert saga.status is SagaStatus.FAILED  # pivot outcome unknown
    _ = ok


def test_crash_and_resume_from_json_store(tmp_path: Path) -> None:
    sc = Scripted(travel(), {"payment:forward": [PERMANENT]})
    vt = VirtualTime()
    for crash_at in range(2, 14):
        sid = f"s{crash_at}"
        store = CrashingStore(JsonFileSagaStore(tmp_path), crash_at)
        o = orchestrator(sc, store, vt)
        o.start("travel", sid, {"n": crash_at})  # append #1, never crashes here
        with contextlib.suppress(SimulatedCrash):
            asyncio.run(o.run(sid))
        fresh = orchestrator(sc, JsonFileSagaStore(tmp_path), vt)
        statuses = run(fresh.resume_incomplete)
        assert statuses[sid] is SagaStatus.COMPENSATED
        for step in ("flight", "hotel"):
            assert sc.ledger.effects[f"{sid}/{step}/forward"] == 1
            assert sc.ledger.effects[f"{sid}/{step}/compensate"] == 1


def test_optimistic_concurrency() -> None:
    sc = Scripted(travel(), {})
    vt = VirtualTime()
    store = InMemorySagaStore()
    o = orchestrator(sc, store, vt)
    o.start("travel", "s1", {})
    a = o.load("s1")
    b = o.load("s1")
    for saga in (a, b):
        cmd = saga.next_command(vt.now())
        saga.begin(cmd, vt.now())  # type: ignore[arg-type]
    store.append("s1", a.version, a.pending_events)
    with pytest.raises(ConcurrencyError):
        store.append("s1", b.version, b.pending_events)


def test_json_store_concurrency_and_validation(tmp_path: Path) -> None:
    store = JsonFileSagaStore(tmp_path)
    sc = Scripted(travel(), {})
    vt = VirtualTime()
    o = orchestrator(sc, store, vt)
    o.start("travel", "s1", {"a": [1, 2]})
    with pytest.raises(ConcurrencyError):
        store.append("s1", 0, list(store.load("s1")))
    with pytest.raises(ValueError, match="not safe"):
        store.load("../etc/passwd")
    assert store.list_ids() == ["s1"]


def test_start_is_idempotent_but_rejects_conflicts() -> None:
    sc = Scripted(travel(), {})
    vt = VirtualTime()
    o = orchestrator(sc, InMemorySagaStore(), vt)
    o.start("travel", "s1", {"a": 1})
    o.start("travel", "s1", {"a": 1})
    with pytest.raises(ValueError, match="different parameters"):
        o.start("travel", "s1", {"a": 2})
    with pytest.raises(UnknownSaga):
        o.load("missing")
    with pytest.raises(UnknownSaga):
        o.start("nope", "s2", {})


def test_registry_validation() -> None:
    async def ok(ctx: StepContext) -> Mapping[str, Any]:
        return {}

    reg = HandlerRegistry()
    reg.on("flight", ok)  # compensatable step without compensation
    vt = VirtualTime()
    with pytest.raises(MissingHandler):
        SagaOrchestrator([travel()], reg, InMemorySagaStore(), vt, vt, SeededRandom(0))
    with pytest.raises(MissingHandler):
        reg.get("payment", Phase.COMPENSATE)


def test_codec_roundtrip_and_errors() -> None:
    sc = Scripted(travel(), {"hotel:forward": [TRANSIENT, PERMANENT]})
    vt = VirtualTime()
    store = InMemorySagaStore()
    o = orchestrator(sc, store, vt)
    o.start("travel", "s1", {"nested": {"x": [1, "y"]}})
    run(lambda: o.run("s1"))
    events = store.load("s1")
    assert [from_dict(to_dict(e)) for e in events] == list(events)
    with pytest.raises(CodecError):
        from_dict({"type": "nope"})
    with pytest.raises(CodecError):
        from_dict({"type": "step_started", "step": "a"})
