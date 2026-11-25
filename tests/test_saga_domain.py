"""Pure aggregate tests: no asyncio, no I/O."""

import pytest

from cortex9_saga import ConstantBackoff, RetryPolicy, SagaDefinition, StepDefinition, StepKind
from cortex9_saga.domain.errors import InvalidTransition
from cortex9_saga.domain.events import CompensationTriggered, FailureKind, Phase
from cortex9_saga.domain.saga import Execute, Finish, Saga, SagaStatus, StepStatus, Wait

T0 = 1000.0
NOW = ConstantBackoff(0)


def step(name: str, kind: StepKind = StepKind.COMPENSATABLE, attempts: int = 1) -> StepDefinition:
    return StepDefinition(
        name,
        kind=kind,
        retry=RetryPolicy(attempts, NOW),
        compensation_retry=RetryPolicy(2, NOW),
    )


def booking() -> SagaDefinition:
    return SagaDefinition(
        "booking",
        (
            step("reserve"),
            step("charge"),
            step("confirm", StepKind.PIVOT),
            step("notify", StepKind.RETRIABLE, attempts=2),
        ),
    )


def drive(saga: Saga, outcomes: dict[str, list[FailureKind | None]]) -> list[tuple[str, Phase]]:
    """Runs the aggregate with scripted outcomes; None means success."""
    trace: list[tuple[str, Phase]] = []
    now = T0
    while True:
        cmd = saga.next_command(now)
        if isinstance(cmd, Finish):
            return trace
        if isinstance(cmd, Wait):
            now = cmd.until
            continue
        saga.begin(cmd, now)
        trace.append((cmd.step, cmd.phase))
        key = f"{cmd.step}:{cmd.phase.value}"
        script = outcomes.get(key, [None])
        outcome = script[min(cmd.attempt - 1, len(script) - 1)]
        if outcome is None:
            saga.record_success(cmd.step, cmd.phase, cmd.attempt, {"by": cmd.step}, now)
        else:
            saga.record_failure(cmd.step, cmd.phase, cmd.attempt, outcome, "boom", now, 0.5)
        now += 1


F = Phase.FORWARD
B = Phase.COMPENSATE


def test_happy_path_completes_in_order() -> None:
    s = Saga.start(booking(), "s1", {"order": 7}, T0)
    trace = drive(s, {})
    assert trace == [("reserve", F), ("charge", F), ("confirm", F), ("notify", F)]
    assert s.status is SagaStatus.COMPLETED


def test_permanent_failure_compensates_previous_steps_only() -> None:
    s = Saga.start(booking(), "s1", {}, T0)
    trace = drive(s, {"charge:forward": [FailureKind.PERMANENT]})
    assert trace[-1] == ("reserve", B)
    assert ("charge", B) not in trace
    assert s.status is SagaStatus.COMPENSATED
    assert s.forward["charge"].status is StepStatus.FAILED_NO_EFFECT


def test_timeout_compensates_the_failed_step_too_in_reverse_order() -> None:
    s = Saga.start(booking(), "s1", {}, T0)
    trace = drive(s, {"charge:forward": [FailureKind.TIMEOUT]})
    assert trace[-2:] == [("charge", B), ("reserve", B)]
    assert s.status is SagaStatus.COMPENSATED


def test_first_step_permanent_failure_has_nothing_to_undo() -> None:
    s = Saga.start(booking(), "s1", {}, T0)
    drive(s, {"reserve:forward": [FailureKind.PERMANENT]})
    assert s.status is SagaStatus.COMPENSATED
    assert s.to_compensate == ()


def test_pivot_permanent_failure_compensates_prior_steps_but_not_pivot() -> None:
    s = Saga.start(booking(), "s1", {}, T0)
    trace = drive(s, {"confirm:forward": [FailureKind.PERMANENT]})
    assert trace[-2:] == [("charge", B), ("reserve", B)]
    assert ("confirm", B) not in trace


def test_pivot_unknown_outcome_fails_stop() -> None:
    s = Saga.start(booking(), "s1", {}, T0)
    trace = drive(s, {"confirm:forward": [FailureKind.TIMEOUT]})
    assert s.status is SagaStatus.FAILED
    assert "outcome unknown" in s.reason
    assert all(phase is F for _, phase in trace)


def test_after_pivot_only_forward_recovery() -> None:
    s = Saga.start(booking(), "s1", {}, T0)
    trace = drive(s, {"notify:forward": [FailureKind.TRANSIENT, FailureKind.TRANSIENT]})
    assert trace.count(("notify", F)) == 2
    assert s.status is SagaStatus.FAILED
    assert not any(isinstance(e, CompensationTriggered) for e in s.history)


def test_transient_failures_are_retried_with_backoff_wait() -> None:
    d = SagaDefinition(
        "r",
        (StepDefinition("x", retry=RetryPolicy(3, ConstantBackoff(5))),),
    )
    s = Saga.start(d, "s1", {}, T0)
    cmd = s.next_command(T0)
    assert isinstance(cmd, Execute)
    s.begin(cmd, T0)
    s.record_failure("x", F, 1, FailureKind.TRANSIENT, "flaky", T0, 0.1)
    assert s.next_command(T0 + 1) == Wait(T0 + 5)
    retry = s.next_command(T0 + 5)
    assert isinstance(retry, Execute)
    assert retry.attempt == 2
    assert retry.idempotency_key == cmd.idempotency_key == "s1/x/forward"


def test_compensation_exhaustion_fails_for_operator() -> None:
    s = Saga.start(booking(), "s1", {}, T0)
    drive(
        s,
        {
            "charge:forward": [FailureKind.PERMANENT],
            "reserve:compensate": [FailureKind.TRANSIENT, FailureKind.PERMANENT],
        },
    )
    assert s.status is SagaStatus.FAILED
    assert "compensation of 'reserve' exhausted" in s.reason


def test_compensation_receives_forward_result() -> None:
    s = Saga.start(booking(), "s1", {"x": 1}, T0)
    drive(s, {"charge:forward": [FailureKind.PERMANENT]})
    s2 = Saga.start(booking(), "s2", {"x": 1}, T0)
    now = T0
    while True:
        cmd = s2.next_command(now)
        assert isinstance(cmd, Execute)
        s2.begin(cmd, now)
        if cmd.phase is B:
            assert cmd.input["forward_result"] == {"by": "reserve"}
            assert cmd.input["payload"] == {"x": 1}
            break
        if cmd.step == "charge":
            s2.record_failure(cmd.step, F, 1, FailureKind.PERMANENT, "no", now, 0)
        else:
            s2.record_success(cmd.step, F, cmd.attempt, {"by": cmd.step}, now)


def test_rehydrate_reproduces_state_exactly() -> None:
    s = Saga.start(booking(), "s1", {"k": "v"}, T0)
    drive(s, {"charge:forward": [FailureKind.TIMEOUT]})
    r = Saga.rehydrate(booking(), s.history)
    assert (r.status, r.reason, r.to_compensate, r.cursor, r.comp_cursor) == (
        s.status,
        s.reason,
        s.to_compensate,
        s.cursor,
        s.comp_cursor,
    )
    assert r.forward == s.forward
    assert r.compensation == s.compensation
    assert r.version == len(s.history)


def test_crash_mid_attempt_reissues_same_attempt() -> None:
    s = Saga.start(booking(), "s1", {}, T0)
    cmd = s.next_command(T0)
    assert isinstance(cmd, Execute)
    s.begin(cmd, T0)
    restored = Saga.rehydrate(booking(), s.history)
    again = restored.next_command(T0 + 100)
    assert again == cmd


def test_illegal_transitions_raise() -> None:
    s = Saga.start(booking(), "s1", {}, T0)
    with pytest.raises(InvalidTransition):
        s.record_success("reserve", F, 1, {}, T0)  # not in flight
    cmd = s.next_command(T0)
    assert isinstance(cmd, Execute)
    with pytest.raises(InvalidTransition):
        s.begin(Execute("s1", "charge", F, 1, "k", 1, {}), T0)  # out of order
    s.begin(cmd, T0)
    with pytest.raises(InvalidTransition):
        s.record_success("reserve", F, 2, {}, T0)  # wrong attempt
    s.record_success("reserve", F, 1, {}, T0)
    with pytest.raises(InvalidTransition):
        s.record_success("reserve", F, 1, {}, T0)  # duplicate outcome
    with pytest.raises(InvalidTransition):
        Saga.start(booking(), "bad/id", {}, T0)
    with pytest.raises(InvalidTransition):
        Saga(booking()).next_command(T0)


def test_pending_events_and_version() -> None:
    s = Saga.start(booking(), "s1", {}, T0)
    assert s.version == 0
    assert len(s.mark_committed()) == 1
    assert s.version == 1


def test_permanent_after_unknown_attempt_still_compensates() -> None:
    """Regression found by the property tests: attempt 1 timed out (effect
    unknown), attempt 2 was rejected permanently. The rejection does not prove
    attempt 1 did nothing, so the step must still be compensated."""
    d = SagaDefinition(
        "r",
        (
            step("a"),
            StepDefinition("b", retry=RetryPolicy(2, NOW), compensation_retry=RetryPolicy(2, NOW)),
        ),
    )
    s = Saga.start(d, "s1", {}, T0)
    trace = drive(s, {"b:forward": [FailureKind.TIMEOUT, FailureKind.PERMANENT]})
    assert s.to_compensate == ("b", "a")
    assert trace[-2:] == [("b", B), ("a", B)]
    assert s.forward["b"].status is StepStatus.FAILED_UNKNOWN


def test_pivot_permanent_after_timeout_is_unknown() -> None:
    d = SagaDefinition(
        "p",
        (step("a"), StepDefinition("p", kind=StepKind.PIVOT, retry=RetryPolicy(2, NOW))),
    )
    s = Saga.start(d, "s1", {}, T0)
    drive(s, {"p:forward": [FailureKind.TIMEOUT, FailureKind.PERMANENT]})
    assert s.status is SagaStatus.FAILED
    assert "outcome unknown" in s.reason
