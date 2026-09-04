"""Token-bucket rate limiter for pacing DynamoDB writes.

Spark runs the writer's ``write()`` callback independently per partition with
no cross-executor coordination, so a limiter instance paces a single task. The
effective global write rate is therefore approximately
``max_writes_per_second * numPartitions``. See the README for guidance.

Two consequences of the token-bucket model worth knowing:

- The bucket starts full, so the first ``capacity`` (defaults to ``rate``)
  items on each task pass without pacing — an intentional burst that is
  amortized once the bucket drains.
- In Structured Streaming the writer's ``write()`` runs once per microbatch per
  partition, and each call constructs a fresh limiter. The limit therefore
  applies *per microbatch*, resetting (and permitting another burst) each
  microbatch rather than enforcing a rate across the whole stream.
"""

import time

# Tokens accumulate via repeated float additions, so a permit's worth of tokens
# can land a hair below the target (e.g. 0.9999999999999998). Comparing with a
# small tolerance avoids a spin loop of sub-epsilon sleeps that never advance the
# clock, at the cost of at most EPSILON tokens of drift per grant (negligible).
_EPSILON = 1e-9


class TokenBucketRateLimiter:
    """Paces work to at most ``rate`` permits per second.

    Tokens accrue continuously at ``rate`` per second up to ``capacity`` (the
    maximum burst). :meth:`acquire` blocks until enough tokens are available,
    then consumes them.

    The ``monotonic`` clock and ``sleep`` functions are injectable so the
    limiter can be tested deterministically without real time passing.
    """

    def __init__(self, rate, *, capacity=None, monotonic=time.monotonic, sleep=time.sleep):
        if rate <= 0:
            raise ValueError(f"rate must be positive, got {rate}")

        self._rate = float(rate)
        self._capacity = float(capacity) if capacity is not None else max(1.0, float(rate))
        if self._capacity < 1:
            raise ValueError(f"capacity must be >= 1, got {capacity}")

        self._monotonic = monotonic
        self._sleep = sleep
        self._tokens = self._capacity
        self._last = monotonic()

    def _refill(self):
        """Add tokens accrued since the last check, capped at capacity."""
        now = self._monotonic()
        elapsed = now - self._last
        if elapsed > 0:
            self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
            self._last = now

    def acquire(self, permits=1):
        """Block until ``permits`` tokens are available, then consume them."""
        if permits <= 0:
            return
        if permits > self._capacity:
            raise ValueError(
                f"permits ({permits}) cannot exceed capacity ({self._capacity})"
            )

        while True:
            self._refill()
            if self._tokens + _EPSILON >= permits:
                self._tokens -= permits
                return
            deficit = permits - self._tokens
            self._sleep(deficit / self._rate)
