"""The tap: the single stateful component in the system.

It holds one long-lived WebSocket to Jetstream, classifies every event, and
publishes it onto a per-destination subject. Everything downstream is stateless
and elastic; this is not, and that asymmetry is the central design fact of the
service (see DESIGN.md).

Three things here are worth reading carefully, because they are the decisions:

1. **The server-side filter.** ``wantedCollections`` is derived from the routing
   table, so we never pay to receive events we have no path for.

2. **The bounded queue and its policy.** Reader and publisher are decoupled by a
   bounded queue. When the broker is slower than the firehose, something has to
   give, and the choice is explicit rather than emergent:

   - ``shed``  — drop the oldest queued event and keep reading. Latency stays
     bounded, completeness does not. Right for aggregate/statistical paths.
   - ``block`` — stop reading the socket. TCP backpressure propagates to
     Jetstream, nothing is lost, but we fall behind and may be disconnected —
     at which point the cursor gets us back with a gap-free replay.

   Neither is universally correct, which is exactly why it is configuration and
   why the shed counter is a first-class metric instead of a debug log.

3. **The cursor.** ``time_us`` of the last *published* event (not the last read
   one) is checkpointed on an interval, and we resume a few seconds behind it.
   That deliberately reprocesses a small window rather than risking a hole.
"""

from __future__ import annotations

import asyncio
import contextlib
import random
import signal
import time
from typing import Any

import nats
import orjson
import redis.asyncio as aioredis
import websockets
from nats.js.api import DiscardPolicy, RetentionPolicy, StorageType, StreamConfig

from .config import Settings, load_settings
from .observability import (
    EVENTS_CLASSIFIED,
    EVENTS_MALFORMED,
    EVENTS_PUBLISHED,
    EVENTS_RECEIVED,
    EVENTS_SHED,
    TAP_CONNECTED,
    TAP_CURSOR_LAG_S,
    TAP_CURSOR_US,
    TAP_QUEUE_DEPTH,
    TAP_RECONNECTS,
    Health,
    get_logger,
    setup_logging,
    start_http_server,
)
from .routing import Route, classify, wanted_collections

log = get_logger("tap")

#: Jetstream cursors are microseconds since epoch.
US_PER_S = 1_000_000


def build_url(settings: Settings, cursor_us: int | None) -> str:
    """Compose the subscribe URL, including the server-side type filter."""
    params = [f"wantedCollections={c}" for c in wanted_collections(settings.collection_routes)]
    if cursor_us is not None:
        params.append(f"cursor={cursor_us}")
    return f"{settings.jetstream_url}?{'&'.join(params)}"


async def ensure_stream(js: Any, settings: Settings) -> None:
    """Create the stream if absent, leave it alone if present.

    LIMITS retention with a short max_age (rather than WorkQueue, which discards
    on ack) keeps a replay window available: an operator can rewind a consumer
    after a bad deploy. The cost is disk we must bound, hence max_bytes and
    DiscardPolicy.OLD — when full we drop the oldest rather than start rejecting
    writes and stalling the tap.
    """
    config = StreamConfig(
        name=settings.stream_name,
        subjects=["bsky.>"],
        retention=RetentionPolicy.LIMITS,
        storage=StorageType.FILE,
        discard=DiscardPolicy.OLD,
        max_age=15 * 60,  # seconds of replay window
        max_bytes=512 * 1024 * 1024,
    )
    try:
        await js.stream_info(settings.stream_name)
        log.info("stream already exists", extra={"context": {"stream": settings.stream_name}})
    except Exception:
        await js.add_stream(config)
        log.info("stream created", extra={"context": {"stream": settings.stream_name}})


class Tap:
    def __init__(self, settings: Settings, health: Health) -> None:
        self.settings = settings
        self.health = health
        self.queue: asyncio.Queue[tuple[Route, bytes]] = asyncio.Queue(
            maxsize=settings.queue_maxsize
        )
        self.stop = asyncio.Event()
        #: time_us of the newest event we have durably handed to the broker.
        #: Only this advances the cursor — advancing on read would let a crash
        #: between read and publish silently lose events.
        self.published_time_us: int | None = None
        self.redis: aioredis.Redis | None = None
        self.js: Any = None
        self.nc: Any = None

    # ------------------------------------------------------------ cursor --

    async def load_cursor(self) -> int | None:
        """Where to resume from.

        Prefer what this process has actually published over what Redis last
        saw: on a mid-flight reconnect the in-memory value is newer than the
        checkpoint, and rewinding to the checkpoint would replay more than we
        need to. Redis is the cold-start path — a fresh pod, or one that lost
        the race between publish and checkpoint.
        """
        assert self.redis is not None
        if self.published_time_us is not None:
            cursor = self.published_time_us
        else:
            raw = await self.redis.get(self.settings.cursor_key)
            if raw is None:
                log.info("no stored cursor; starting from live")
                return None
            cursor = int(raw)
        rewound = cursor - int(self.settings.cursor_replay_s * US_PER_S)
        log.info(
            "resuming from cursor",
            extra={"context": {"cursor_us": cursor, "resume_us": rewound,
                               "replay_s": self.settings.cursor_replay_s}},
        )
        return max(rewound, 0)

    async def checkpoint_cursor_loop(self) -> None:
        """Persist the cursor and publish the lag signal on a fixed interval.

        Lag gates readiness as well as being a metric. A tap that holds a
        healthy socket but is ninety seconds behind is not doing its job, and a
        readiness probe that only checked socket state would report it as fine —
        which is precisely the failure you most want surfaced.
        """
        assert self.redis is not None
        while not self.stop.is_set():
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self.stop.wait(), timeout=self.settings.cursor_interval_s)
            if self.published_time_us is None:
                continue

            await self.redis.set(self.settings.cursor_key, self.published_time_us)
            TAP_CURSOR_US.set(self.published_time_us)

            # time_us is Jetstream's own clock, so this compares two machines'
            # clocks and is only meaningful to within their skew. Fine for a
            # "tens of seconds behind" signal; it would not be fine as a
            # precise measurement, and is not used as one.
            lag = max(0.0, time.time() - (self.published_time_us / US_PER_S))
            TAP_CURSOR_LAG_S.set(lag)
            if TAP_CONNECTED._value.get() == 1:
                if lag > self.settings.max_lag_s:
                    self.health.set_ready(False, f"lagging {lag:.0f}s behind")
                else:
                    self.health.set_ready(True, f"lag {lag:.1f}s")

    # ------------------------------------------------------------ reader --

    def _enqueue(self, route: Route, payload: bytes) -> None:
        """Apply the backpressure policy. Only called for the ``shed`` path."""
        try:
            self.queue.put_nowait((route, payload))
        except asyncio.QueueFull:
            # Drop the oldest: under sustained overload the freshest events are
            # the useful ones. Losing the head is also what keeps the delay
            # through this queue bounded.
            try:
                dropped_route, _ = self.queue.get_nowait()
                EVENTS_SHED.labels(destination=dropped_route.destination.value).inc()
            except asyncio.QueueEmpty:  # pragma: no cover - racy, harmless
                pass
            with contextlib.suppress(asyncio.QueueFull):
                self.queue.put_nowait((route, payload))

    async def read_loop(self) -> None:
        """Connect, read, classify, enqueue. Reconnects forever with backoff."""
        attempt = 0
        while not self.stop.is_set():
            cursor = await self.load_cursor()
            url = build_url(self.settings, cursor)
            try:
                async with websockets.connect(url, max_size=None, ping_interval=20) as ws:
                    attempt = 0
                    TAP_CONNECTED.set(1)
                    self.health.set_ready(True, "connected")
                    log.info("jetstream connected", extra={"context": {"cursor_us": cursor}})
                    await self._consume(ws)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                TAP_CONNECTED.set(0)
                self.health.set_ready(False, f"disconnected: {type(exc).__name__}")
                TAP_RECONNECTS.inc()
                attempt += 1
                # Exponential backoff with full jitter. Jitter matters: without
                # it, every replica of a fleet reconnects in lockstep and
                # hammers the upstream at exactly the moment it is struggling.
                delay = min(self.settings.reconnect_max_s, 2 ** min(attempt, 6))
                delay = random.uniform(0, delay)
                log.warning(
                    "jetstream disconnected; backing off",
                    extra={"context": {"attempt": attempt, "delay_s": round(delay, 2),
                                       "error": str(exc)}},
                )
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self.stop.wait(), timeout=delay)

    async def _consume(self, ws: Any) -> None:
        blocking = self.settings.backpressure_policy == "block"
        while not self.stop.is_set():
            raw = await ws.recv()
            EVENTS_RECEIVED.inc()

            try:
                event = orjson.loads(raw)
            except orjson.JSONDecodeError:
                EVENTS_MALFORMED.inc()
                continue

            route = classify(event, self.settings.collection_routes)
            if route is None:
                EVENTS_MALFORMED.inc()
                continue

            EVENTS_CLASSIFIED.labels(
                destination=route.destination.value, reason=route.reason
            ).inc()

            payload = raw if isinstance(raw, bytes) else raw.encode()
            if blocking:
                await self.queue.put((route, payload))
            else:
                self._enqueue(route, payload)
            TAP_QUEUE_DEPTH.set(self.queue.qsize())

    # --------------------------------------------------------- publisher --

    async def publish_loop(self) -> None:
        """Drain the queue to the broker in pipelined batches.

        Publishing one-at-a-time would serialise a round trip per event and cap
        us well below the firehose rate. Issuing a batch concurrently and
        awaiting them together keeps the acks (we do want acks — an unacked
        publish is a lie) without paying latency per message.
        """
        assert self.js is not None
        while not self.stop.is_set() or not self.queue.empty():
            try:
                first = await asyncio.wait_for(self.queue.get(), timeout=0.5)
            except TimeoutError:
                continue

            batch = [first]
            while len(batch) < self.settings.publish_batch:
                try:
                    batch.append(self.queue.get_nowait())
                except asyncio.QueueEmpty:
                    break

            results = await asyncio.gather(
                *(
                    self.js.publish(route.subject, payload, headers=route.headers())
                    for route, payload in batch
                ),
                return_exceptions=True,
            )

            highest = self.published_time_us
            for (route, _), result in zip(batch, results, strict=True):
                if isinstance(result, Exception):
                    # Do not advance the cursor past a failed publish, and do not
                    # crash the loop: a broker blip should degrade throughput,
                    # not kill ingest.
                    log.error(
                        "publish failed",
                        extra={"context": {"destination": route.destination.value,
                                           "error": str(result)}},
                    )
                    continue
                EVENTS_PUBLISHED.labels(destination=route.destination.value).inc()
                if route.time_us is not None and (highest is None or route.time_us > highest):
                    highest = route.time_us
            self.published_time_us = highest
            TAP_QUEUE_DEPTH.set(self.queue.qsize())

    # ------------------------------------------------------------- run ----

    async def run(self) -> None:
        s = self.settings
        self.redis = aioredis.from_url(s.redis_url, decode_responses=True)
        self.nc = await nats.connect(
            s.nats_url,
            max_reconnect_attempts=-1,  # never give up; the pod is the retry budget
            reconnect_time_wait=1,
        )
        self.js = self.nc.jetstream()
        await ensure_stream(self.js, s)

        log.info(
            "tap starting",
            extra={"context": {
                "policy": s.backpressure_policy,
                "queue_maxsize": s.queue_maxsize,
                "wanted_collections": wanted_collections(s.collection_routes),
            }},
        )

        tasks = [
            asyncio.create_task(self.read_loop(), name="read"),
            asyncio.create_task(self.publish_loop(), name="publish"),
            asyncio.create_task(self.checkpoint_cursor_loop(), name="cursor"),
        ]
        await self.stop.wait()

        # Graceful drain: stop reading, let the publisher flush what is already
        # queued, then checkpoint one last time. Without this, a rolling deploy
        # loses everything in flight.
        log.info("draining", extra={"context": {"queued": self.queue.qsize()}})
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if self.published_time_us is not None and self.redis is not None:
            await self.redis.set(s.cursor_key, self.published_time_us)
        if self.nc is not None:
            await self.nc.drain()
        if self.redis is not None:
            await self.redis.aclose()
        log.info("tap stopped")


async def main() -> None:
    settings = load_settings()
    setup_logging(settings.log_level)
    health = Health()
    await start_http_server(health, settings.metrics_port)

    tap = Tap(settings, health)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, tap.stop.set)
    await tap.run()


if __name__ == "__main__":
    asyncio.run(main())
