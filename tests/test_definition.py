import pytest

from cortex9_saga import (
    ConstantBackoff,
    ExponentialBackoff,
    RetryPolicy,
    SagaDefinition,
    StepDefinition,
    StepKind,
)
from cortex9_saga.domain.errors import InvalidDefinition

C, P, R = StepKind.COMPENSATABLE, StepKind.PIVOT, StepKind.RETRIABLE


def sd(name: str, kind: StepKind = C) -> StepDefinition:
    return StepDefinition(name, kind=kind)


def test_valid_orderings() -> None:
    SagaDefinition("a", (sd("x"), sd("y", P), sd("z", R)))
    SagaDefinition("b", (sd("x"),))
    SagaDefinition("c", (sd("x", P),))
    SagaDefinition("d", (sd("x", R), sd("y", R)))


@pytest.mark.parametrize(
    "steps",
    [
        (),
        (sd("x"), sd("x")),
        (sd("x", P), sd("y")),
        (sd("x", R), sd("y", P)),
        (sd("x", P), sd("y", P)),
    ],
)
def test_invalid_definitions(steps: tuple[StepDefinition, ...]) -> None:
    with pytest.raises(InvalidDefinition):
        SagaDefinition("bad", steps)


@pytest.mark.parametrize("name", ["", "a/b"])
def test_invalid_step_names(name: str) -> None:
    with pytest.raises(InvalidDefinition):
        StepDefinition(name)


def test_invalid_numbers() -> None:
    with pytest.raises(InvalidDefinition):
        StepDefinition("x", timeout=0)
    with pytest.raises(InvalidDefinition):
        RetryPolicy(max_attempts=0)
    with pytest.raises(InvalidDefinition):
        ExponentialBackoff(jitter=1.5)
    with pytest.raises(InvalidDefinition):
        ExponentialBackoff(initial=5, maximum=1)


def test_exponential_backoff_growth_cap_and_jitter_bounds() -> None:
    b = ExponentialBackoff(initial=1, multiplier=2, maximum=10, jitter=0)
    assert [b.delay(n, 0.5) for n in range(1, 6)] == [1, 2, 4, 8, 10]
    j = ExponentialBackoff(initial=4, multiplier=1, maximum=10, jitter=0.25)
    assert j.delay(1, 0.0) == pytest.approx(3.0)
    assert j.delay(1, 0.999999) == pytest.approx(5.0, rel=1e-5)
    assert ConstantBackoff(2.5).delay(7, 0.3) == 2.5


def test_lookup_helpers() -> None:
    d = SagaDefinition("a", (sd("x"), sd("y")))
    assert d.index("y") == 1
    assert d.step("x").name == "x"
    with pytest.raises(KeyError):
        d.step("nope")
