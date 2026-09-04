"""Tests for the token-bucket write rate limiter.

A fake clock makes pacing deterministic: virtual time only advances when the
limiter sleeps, so no wall-clock time is spent and assertions are exact.
"""

import pytest


class FakeClock:
    """Deterministic monotonic clock; ``sleep`` advances virtual time."""

    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        assert seconds >= 0, "limiter must never request a negative sleep"
        self.sleeps.append(seconds)
        self.now += seconds


def _limiter(rate, capacity=None):
    from dynamodb_data_source.rate_limiter import TokenBucketRateLimiter

    clock = FakeClock()
    limiter = TokenBucketRateLimiter(
        rate=rate, capacity=capacity, monotonic=clock.monotonic, sleep=clock.sleep
    )
    return limiter, clock


def test_burst_up_to_capacity_does_not_sleep():
    """A full bucket absorbs a burst up to its capacity without pacing."""
    limiter, clock = _limiter(rate=10, capacity=10)

    for _ in range(10):
        limiter.acquire(1)

    assert clock.sleeps == []


def test_next_permit_after_burst_waits():
    """Once drained, the next permit waits 1/rate seconds."""
    limiter, clock = _limiter(rate=10, capacity=10)

    for _ in range(10):
        limiter.acquire(1)
    limiter.acquire(1)

    assert clock.sleeps == [pytest.approx(0.1)]


def test_sustained_rate_is_paced():
    """With capacity=1 (no burst), N permits take (N-1)/rate seconds."""
    limiter, clock = _limiter(rate=5, capacity=1)

    for _ in range(6):
        limiter.acquire(1)

    # First permit is free (bucket starts full); the next five pace at 0.2s each.
    assert sum(clock.sleeps) == pytest.approx(1.0)
    assert clock.now == pytest.approx(1.0)


def test_tokens_refill_over_time():
    """Tokens accrue while idle, so a later burst need not sleep."""
    limiter, clock = _limiter(rate=10, capacity=10)

    for _ in range(10):
        limiter.acquire(1)  # drain
    clock.now += 0.5  # 0.5s * 10/s = 5 tokens refilled
    for _ in range(5):
        limiter.acquire(1)

    assert clock.sleeps == []


def test_refill_capped_at_capacity():
    """Idle time cannot bank more than capacity tokens."""
    limiter, clock = _limiter(rate=10, capacity=10)

    clock.now += 100  # way more than enough to overflow the bucket
    for _ in range(10):
        limiter.acquire(1)
    limiter.acquire(1)  # 11th must still wait — bucket capped at 10

    assert clock.sleeps == [pytest.approx(0.1)]


def test_rate_must_be_positive():
    from dynamodb_data_source.rate_limiter import TokenBucketRateLimiter

    with pytest.raises(ValueError):
        TokenBucketRateLimiter(rate=0)
    with pytest.raises(ValueError):
        TokenBucketRateLimiter(rate=-1)


def test_permits_cannot_exceed_capacity():
    limiter, _ = _limiter(rate=10, capacity=5)

    with pytest.raises(ValueError):
        limiter.acquire(6)


def test_capacity_below_one_rejected():
    from dynamodb_data_source.rate_limiter import TokenBucketRateLimiter

    with pytest.raises(ValueError):
        TokenBucketRateLimiter(rate=10, capacity=0.5)


def test_acquire_zero_permits_is_noop():
    limiter, clock = _limiter(rate=10, capacity=10)

    limiter.acquire(0)

    assert clock.sleeps == []
