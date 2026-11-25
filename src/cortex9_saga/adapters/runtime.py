"""Clocks, sleepers and randomness."""

from __future__ import annotations

import asyncio
import random
import time


class SystemClock:
    def now(self) -> float:
        return time.time()


class AsyncioSleeper:
    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)


class VirtualTime:
    """Clock + Sleeper for tests: sleeping advances time instantly."""

    def __init__(self, start: float = 1_800_000_000.0) -> None:
        self._now = start
        self.slept: list[float] = []

    def now(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += seconds

    async def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self._now += seconds
        await asyncio.sleep(0)


class SeededRandom:
    def __init__(self, seed: int) -> None:
        self._r = random.Random(seed)  # noqa: S311 - jitter, not security

    def random(self) -> float:
        return self._r.random()
