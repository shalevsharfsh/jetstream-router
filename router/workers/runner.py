"""The worker harness shared by every downstream path.

One durable pull consumer per destination, each with its own name, its own
filter subject, its own redelivery budget and its own DLQ. That is where the
"paths that don't interfere with each other" property actually comes from: a
worker that crashes, wedges, or falls behind affects exactly one consumer's
pending count. Nothing about the content path can slow down the graph path,
because they share no process, no queue and no ack window.

A handler is a plain object with ``name`` and ``async def handle(event, route)``.
It never touches NATS. That keeps the interesting logic unit-testable without a
broker, and means the retry/DLQ/drain semantics are implemented once here rather
than four times with three subtly different bugs.
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
from typing import Any, Protocol

import nats
import orjson
import redis.asyncio as aioredis
from nats.js.api import AckPolicy, ConsumerConfig

from ..config import Settings, load_settings
from ..observability import (
    ALERTS_RAISED,
    DLQ_MESSAGES,
    HANDLER_SECONDS,
    MESSAGES_HANDLED,
    Health,
    get_logger,
    setup_logging,
    start_http_server,
)
from ..routing import Destination
from ..windows import RedisWindowStore

log = get_logger("worker")


class Handler(Protocol):
    """What every downstream path must implement."""

    name: str

    async def setup(self, ctx: WorkerContext) -> None: ...

    async def handle(self, event: dict[str, Any], headers: dict[str, str]) -> None: ...


class WorkerContext:
    """Shared dependencies handed to handlers.

    ``store`` is an interface, not a Redis client: on Kubernetes it is
    ``RedisWindowStore``, on Lambda it is ``DynamoWindowStore``. Handlers are
    written against the interface, which is what makes them genuinely portable
    between the two deployment targets rather than nominally so.

    It also means a test can pass a fakeredis-backed store and a tweaked
    Settings and exercise the real handler logic with no infrastructure at all.
    """

    def __init__(self, settings: Settings, store: Any) -> None:
        self.settings = settings
        self.store = store

    def alert(self, worker: str, alert: str, **context: Any) -> None:
        """Emit downstream 'work'.

        In this prototype an alert is a structured log line plus a counter. In
        production this is the seam where a webhook, an SNS publish or a
        PagerDuty event goes — deliberately one function so there is exactly one
        place to change.
        """
        ALERTS_RAISED.labels(worker=worker, alert=alert).inc()
        log.info("ALERT", extra={"context": {"worker": worker, "alert": alert, **context}})


async def _consumer_config(settings: Settings, destination: Destination) -> ConsumerConfig:
    return ConsumerConfig(
        durable_name=f"{destination.value}-worker",
        filter_subject=destination.subject,
        ack_policy=AckPolicy.EXPLICIT,
        # Redelivery budget. Past this we stop retrying and move the message
        # aside — a poison message must never be able to block a path forever.
        max_deliver=settings.worker_max_deliver,
        ack_wait=settings.worker_ack_wait_s,
        max_ack_pending=settings.worker_batch * 4,
    )


class Worker:
    def __init__(
        self,
        destination: Destination,
        handler: Handler,
        settings: Settings,
        health: Health,
    ) -> None:
        self.destination = destination
        self.handler = handler
        self.settings = settings
        self.health = health
        self.stop = asyncio.Event()
        self.nc: Any = None
        self.js: Any = None
        self.redis: aioredis.Redis | None = None

    async def _process(self, msg: Any) -> None:
        """Handle one message. Owns the ack/nak/DLQ decision, nothing else."""
        worker = self.handler.name
        headers = dict(msg.headers or {})

        try:
            event = orjson.loads(msg.data)
        except orjson.JSONDecodeError:
            # Unparseable payload will never parse on retry. Terminate
            # immediately rather than burning the whole redelivery budget.
            MESSAGES_HANDLED.labels(worker=worker, outcome="malformed").inc()
            await self._to_dlq(msg, headers, reason="malformed-json")
            return

        try:
            with HANDLER_SECONDS.labels(worker=worker).time():
                await self.handler.handle(event, headers)
        except Exception as exc:
            delivered = getattr(getattr(msg, "metadata", None), "num_delivered", 1)
            if delivered >= self.settings.worker_max_deliver:
                log.error(
                    "handler failed permanently; routing to DLQ",
                    extra={"context": {"worker": worker, "delivered": delivered,
                                       "error": str(exc)}},
                    exc_info=True,
                )
                MESSAGES_HANDLED.labels(worker=worker, outcome="dlq").inc()
                await self._to_dlq(msg, headers, reason=f"handler-error: {exc}")
            else:
                log.warning(
                    "handler failed; will retry",
                    extra={"context": {"worker": worker, "delivered": delivered,
                                       "error": str(exc)}},
                )
                MESSAGES_HANDLED.labels(worker=worker, outcome="retry").inc()
                # Negative-ack with a delay so a struggling dependency gets a
                # breather instead of an instant redelivery storm.
                await msg.nak(delay=min(30, 2**delivered))
            return

        MESSAGES_HANDLED.labels(worker=worker, outcome="ok").inc()
        await msg.ack()

    async def _to_dlq(self, msg: Any, headers: dict[str, str], reason: str) -> None:
        """Park a message we will not retry, then ack it off the main consumer.

        Ack order matters: publish first, ack second. If we crash between them
        the message is redelivered and lands in the DLQ twice, which is
        recoverable. Acking first would lose it outright.
        """
        DLQ_MESSAGES.labels(worker=self.handler.name).inc()
        with contextlib.suppress(Exception):
            await self.js.publish(
                self.destination.dlq_subject,
                msg.data,
                headers={**headers, "dlq_reason": reason[:256]},
            )
        await msg.ack()

    async def run(self) -> None:
        s = self.settings
        self.redis = aioredis.from_url(s.redis_url, decode_responses=True)
        self.nc = await nats.connect(s.nats_url, max_reconnect_attempts=-1, reconnect_time_wait=1)
        self.js = self.nc.jetstream()

        await self.handler.setup(WorkerContext(s, RedisWindowStore(self.redis)))

        sub = await self.js.pull_subscribe(
            subject=self.destination.subject,
            durable=f"{self.destination.value}-worker",
            config=await _consumer_config(s, self.destination),
        )
        self.health.set_ready(True, "subscribed")
        log.info(
            "worker started",
            extra={"context": {"worker": self.handler.name,
                               "subject": self.destination.subject}},
        )

        while not self.stop.is_set():
            try:
                msgs = await sub.fetch(batch=s.worker_batch, timeout=2)
            except (TimeoutError, nats.errors.TimeoutError):
                continue  # an idle path is normal, not an error
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.error("fetch failed", extra={"context": {"error": str(exc)}})
                await asyncio.sleep(1)
                continue

            # Messages within a batch are independent, so process them
            # concurrently; ordering across a destination is not something this
            # design promises (see DESIGN.md on ordering).
            await asyncio.gather(*(self._process(m) for m in msgs), return_exceptions=True)

        log.info("worker stopping", extra={"context": {"worker": self.handler.name}})
        self.health.set_ready(False, "shutting down")
        if self.nc is not None:
            await self.nc.drain()
        if self.redis is not None:
            await self.redis.aclose()


async def run_worker(destination: Destination, handler: Handler) -> None:
    settings = load_settings()
    setup_logging(settings.log_level)
    health = Health()
    await start_http_server(health, settings.metrics_port)

    worker = Worker(destination, handler, settings, health)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        # SIGTERM arrives on every rolling deploy and every KEDA scale-down.
        # Finishing the in-flight batch before exiting is what makes those
        # events invisible downstream instead of a burst of redeliveries.
        loop.add_signal_handler(sig, worker.stop.set)
    await worker.run()
