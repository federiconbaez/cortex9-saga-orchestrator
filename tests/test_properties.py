"""Property-based tests of the CONTRACT invariants under random failure
scripts and random crash points."""

from __future__ import annotations

import asyncio
import contextlib

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from cortex9_saga import (
    ConstantBackoff,
    RetryPolicy,
    SagaDefinition,
    SagaOrchestrator,
    SagaStatus,
    StepDefinition,
    StepKind,
)
from cortex9_saga.adapters.runtime import SeededRandom, VirtualTime
from cortex9_saga.adapters.stores import InMemorySagaStore
from cortex9_saga.domain.events import CompensationTriggered, Phase, StepStarted, StepSucceeded
from cortex9_saga.domain.saga import Saga

from support import OK, PERMANENT, TIMEOUT, TIMEOUT_AFTER, TRANSIENT, CrashingStore, Scripted
from support import SimulatedCrash as Crash

FORWARD_OUTCOMES = st.sampled_from([OK, OK, OK, TRANSIENT, PERMANENT, TIMEOUT, TIMEOUT_AFTER])
COMP_OUTCOMES = st.sampled_from([OK, OK, OK, TRANSIENT, TIMEOUT, TIMEOUT_AFTER])


@st.composite
def scenarios(draw: st.DrawFn) -> tuple[SagaDefinition, dict[str, list[str]]]:
    n_comp = draw(st.integers(0, 4))
    has_pivot = draw(st.booleans())
    n_retr = draw(st.integers(0, 2)) if has_pivot else 0
    if n_comp == 0 and not has_pivot:
        n_comp = 1
    attempts = draw(st.integers(1, 3))
    pol = RetryPolicy(attempts, ConstantBackoff(draw(st.sampled_from([0.0, 0.5, 2.0]))))
    steps: list[StepDefinition] = []
    steps += [
        StepDefinition(f"c{i}", retry=pol, compensation_retry=RetryPolicy(4, ConstantBackoff(1)))
        for i in range(n_comp)
    ]
    if has_pivot:
        steps.append(StepDefinition("pivot", kind=StepKind.PIVOT, retry=pol))
    steps += [StepDefinition(f"r{i}", kind=StepKind.RETRIABLE, retry=pol) for i in range(n_retr)]
    script: dict[str, list[str]] = {}
    for s in steps:
        script[f"{s.name}:forward"] = draw(st.lists(FORWARD_OUTCOMES, min_size=1, max_size=4))
        script[f"{s.name}:compensate"] = draw(st.lists(COMP_OUTCOMES, min_size=1, max_size=5))
    return SagaDefinition("prop", tuple(steps)), script


def execute(
    d: SagaDefinition, script: dict[str, list[str]], crash_at: int | None
) -> tuple[Saga, Scripted]:
    sc = Scripted(d, script)
    vt = VirtualTime()
    inner = InMemorySagaStore()
    o = SagaOrchestrator([d], sc.registry, CrashingStore(inner, crash_at), vt, vt, SeededRandom(7))
    o.start("prop", "s1", {"k": 1})
    with contextlib.suppress(Crash):
        asyncio.run(o.run("s1"))
    survivor = SagaOrchestrator([d], sc.registry, inner, vt, vt, SeededRandom(7))
    saga = asyncio.run(survivor.run("s1"))
    return saga, sc


def check_invariants(d: SagaDefinition, saga: Saga, sc: Scripted) -> None:
    names = [s.name for s in d.steps]
    kind = {s.name: s.kind for s in d.steps}
    # 1. terminal
    assert saga.status.terminal
    # Forward steps start in definition order.
    fwd = [e.step for e in saga.history if isinstance(e, StepStarted) and e.phase is Phase.FORWARD]
    idx = [names.index(s) for s in fwd]
    assert idx == sorted(idx)
    succeeded = [
        e.step for e in saga.history if isinstance(e, StepSucceeded) and e.phase is Phase.FORWARD
    ]
    # 2. completed => everything succeeded
    if saga.status is SagaStatus.COMPLETED:
        assert succeeded == names
    triggered = [e for e in saga.history if isinstance(e, CompensationTriggered)]
    assert len(triggered) <= 1
    if triggered:
        plan = triggered[0].to_compensate
        # 4. never after the pivot, never compensate pivot/retriable steps
        assert all(kind[s] is StepKind.COMPENSATABLE for s in plan)
        assert not any(kind[s] is StepKind.RETRIABLE for s in fwd)
        # 3. reverse order, only attempted steps
        order = [names.index(s) for s in plan]
        assert order == sorted(order, reverse=True)
        assert set(plan) <= set(fwd)
        # Every compensatable step whose forward effect happened is in the plan.
        for s in names:
            if kind[s] is StepKind.COMPENSATABLE and sc.applied(s, "forward"):
                assert s in plan, f"effect of {s} would leak"
        if saga.status is SagaStatus.COMPENSATED:
            for s in plan:
                comp = [
                    e
                    for e in saga.history
                    if isinstance(e, StepSucceeded) and e.step == s and e.phase is Phase.COMPENSATE
                ]
                assert comp, f"{s} not compensated"
    # 6. stable keys
    for step, phase, _, key in sc.calls:
        assert key == f"s1/{step}/{phase}"
    # 7. effectively-once effects despite retries/crashes
    assert all(n == 1 for n in sc.ledger.effects.values())


@settings(max_examples=300, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(scenarios())
def test_invariants_without_crash(sc_def: tuple[SagaDefinition, dict[str, list[str]]]) -> None:
    d, script = sc_def
    saga, sc = execute(d, script, None)
    check_invariants(d, saga, sc)


@settings(max_examples=300, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(scenarios(), st.integers(2, 40))
def test_crash_anywhere_resumes_to_same_outcome(
    sc_def: tuple[SagaDefinition, dict[str, list[str]]], crash_at: int
) -> None:
    d, script = sc_def
    clean, _ = execute(d, script, None)
    saga, sc = execute(d, script, crash_at)
    check_invariants(d, saga, sc)
    assert saga.status is clean.status
    assert saga.to_compensate == clean.to_compensate
