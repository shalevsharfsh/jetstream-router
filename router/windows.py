"""Rolling counters over a sliding window, kept in Redis.

Two of the four paths (engagement traction, follow bursts) are the same shape:
count occurrences of a key over the last N seconds and fire once when a
threshold is crossed. That shared shape lives here so the handlers stay about
their domain rather than about Redis.

Why bucketed counters rather than a sorted set of timestamps: a ZSET per subject
gives exact windows but costs memory proportional to event volume and needs
periodic ZREMRANGEBYSCORE trimming. Bucketing trades a little precision at the
window edge (resolution is one bucket) for O(1) memory per key and TTL-based
cleanup we never have to run ourselves. At firehose volume that trade is
obviously right; if we later needed exact windows, this is the one module that
would change.

State lives in Redis rather than in the worker process for a specific reason:
these workers scale to zero and back under KEDA. In-process counters would be
destroyed on every scale-down, and would be wrong the moment there were two
replicas. Externalising the state is what makes the workers genuinely
disposable.
"""

from __future__ import annotations

import time
from typing import Any

#: Window resolution. Smaller means more Redis keys and a sharper window edge.
BUCKET_S = 10


def _buckets(window_s: int, now: float, bucket_s: int = BUCKET_S) -> list[int]:
    """The bucket timestamps covering ``window_s`` back from ``now``."""
    n = max(1, window_s // bucket_s)
    current = int(now) - (int(now) % bucket_s)
    return [current - i * bucket_s for i in range(n)]


async def bump_and_total(
    redis: Any,
    namespace: str,
    member: str,
    window_s: int,
    now: float | None = None,
    bucket_s: int = BUCKET_S,
) -> int:
    """Record one occurrence of ``member`` and return its total over the window.

    ``now`` is injectable purely so tests can drive the clock instead of sleeping.
    """
    now = time.time() if now is None else now
    buckets = _buckets(window_s, now, bucket_s)
    current_key = f"{namespace}:{member}:{buckets[0]}"

    pipe = redis.pipeline()
    pipe.incr(current_key)
    # TTL slightly beyond the window so a bucket survives exactly as long as it
    # can still be counted, then evicts itself. No sweeper process to operate.
    pipe.expire(current_key, window_s + bucket_s)
    await pipe.execute()

    older = [f"{namespace}:{member}:{b}" for b in buckets[1:]]
    if not older:
        values: list[Any] = []
    else:
        values = await redis.mget(older)

    total = 0
    for raw in values:
        if raw:
            total += int(raw)
    # Re-read of the current bucket is avoided: the INCR above already told us
    # its value, but pipeline results are cheap to ignore and re-reading keeps
    # the code obvious. Fetch it once here.
    current = await redis.get(current_key)
    return total + (int(current) if current else 0)


async def claim_alert(redis: Any, namespace: str, member: str, cooldown_s: int) -> bool:
    """Return True at most once per ``cooldown_s`` for a given member.

    Without this, an account sitting above the threshold would emit an alert per
    event for as long as it stayed there — thousands of pages for one incident.
    SET NX EX makes the claim atomic, so it holds even with several replicas of
    the same worker racing on the same key.
    """
    return bool(await redis.set(f"{namespace}:alerted:{member}", "1", nx=True, ex=cooldown_s))
