"""Logging, metrics and the health endpoint.

Structured JSON logs on stdout (the container convention — the collector owns
shipping, not us) and a Prometheus endpoint on the same port as the health
probes so a pod needs one port and one Service.

Metric naming follows Prometheus convention: ``_total`` for counters, base units
for everything else. Labels are strictly bounded — ``destination`` and ``reason``
are small closed sets. Nothing user-controlled (a DID, a URI, a collection name
from an unknown lexicon) is ever a label; that is how you cardinality-bomb a
Prometheus and take out your own monitoring during the incident you needed it for.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from typing import Any

from aiohttp import web
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

# ---------------------------------------------------------------- logging ----


class JsonFormatter(logging.Formatter):
    """Minimal structured formatter.

    Extra context is passed as ``logger.info("msg", extra={"context": {...}})``
    so the message stays a stable, greppable constant and the varying data lives
    in fields — the thing that makes logs queryable instead of merely readable.
    """

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
            + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        context = getattr(record, "context", None)
        if isinstance(context, dict):
            payload.update(context)
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def setup_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    # nats-py logs every reconnect attempt at INFO; we already emit our own.
    logging.getLogger("nats").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


# ---------------------------------------------------------------- metrics ----

ROLE = os.environ.get("ROLE", "unknown")

EVENTS_RECEIVED = Counter(
    "jsr_events_received_total",
    "Events read off the Jetstream WebSocket.",
)
EVENTS_MALFORMED = Counter(
    "jsr_events_malformed_total",
    "Events that could not be decoded or classified. A rise here means upstream "
    "schema drift, which is a different problem from an unrouted event.",
)
EVENTS_CLASSIFIED = Counter(
    "jsr_events_classified_total",
    "Events classified, by destination and the rule that decided it.",
    ["destination", "reason"],
)
EVENTS_PUBLISHED = Counter(
    "jsr_events_published_total",
    "Events successfully published to the broker.",
    ["destination"],
)
EVENTS_SHED = Counter(
    "jsr_events_shed_total",
    "Events dropped by the tap's backpressure policy. Non-zero means we are "
    "deliberately trading completeness for freshness — never silent.",
    ["destination"],
)
TAP_QUEUE_DEPTH = Gauge(
    "jsr_tap_queue_depth",
    "In-flight events between the socket reader and the publisher.",
)
TAP_CONNECTED = Gauge(
    "jsr_tap_connected",
    "1 while the Jetstream WebSocket is established, 0 otherwise.",
)
TAP_RECONNECTS = Counter(
    "jsr_tap_reconnects_total",
    "WebSocket reconnect attempts.",
)
TAP_CURSOR_US = Gauge(
    "jsr_tap_cursor_time_us",
    "Last checkpointed Jetstream cursor (time_us).",
)
TAP_CURSOR_LAG_S = Gauge(
    "jsr_tap_cursor_lag_seconds",
    "Wall-clock seconds between now and the last event we published. This is "
    "the single most useful number in the service: it is 'are we keeping up?' "
    "expressed directly, rather than inferred from CPU or queue depth. It is "
    "also what gates readiness.",
)

MESSAGES_HANDLED = Counter(
    "jsr_messages_handled_total",
    "Messages processed by a worker, by outcome.",
    ["worker", "outcome"],
)
HANDLER_SECONDS = Histogram(
    "jsr_handler_seconds",
    "Per-message handler latency.",
    ["worker"],
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0),
)
ALERTS_RAISED = Counter(
    "jsr_alerts_raised_total",
    "Downstream 'work' actually triggered — the point of the whole service.",
    ["worker", "alert"],
)
RETRACTIONS = Counter(
    "jsr_retractions_total",
    "Deletions processed, by collection and whether the aggregate impact could "
    "be resolved. A high 'no-index' rate is the cost of not maintaining a "
    "record->subject index; see workers/retraction.py.",
    ["collection", "resolution"],
)
DLQ_MESSAGES = Counter(
    "jsr_dlq_messages_total",
    "Messages that exhausted redelivery and were routed to the DLQ subject.",
    ["worker"],
)


# ----------------------------------------------------------------- health ----


class Health:
    """Liveness and readiness, kept separate because they mean different things.

    Liveness answers "is this process wedged" — a failing liveness probe gets the
    pod killed. Readiness answers "should traffic/work reach me right now"; a tap
    that has lost its upstream WebSocket is un-ready but perfectly alive, and
    restarting it would only throw away the connection backoff it has earned.
    """

    def __init__(self) -> None:
        self.live = True
        self.ready = False
        self.detail: str = "starting"

    def set_ready(self, ready: bool, detail: str = "") -> None:
        self.ready = ready
        self.detail = detail or ("ok" if ready else "not ready")


async def start_http_server(health: Health, port: int) -> web.AppRunner:
    """Serve /healthz, /readyz and /metrics."""

    async def healthz(_: web.Request) -> web.Response:
        return web.json_response({"status": "ok" if health.live else "dead", "role": ROLE},
                                 status=200 if health.live else 500)

    async def readyz(_: web.Request) -> web.Response:
        return web.json_response(
            {"status": "ready" if health.ready else "not-ready", "detail": health.detail},
            status=200 if health.ready else 503,
        )

    async def metrics(_: web.Request) -> web.Response:
        return web.Response(body=generate_latest(), content_type=CONTENT_TYPE_LATEST.split(";")[0])

    app = web.Application()
    app.router.add_get("/healthz", healthz)
    app.router.add_get("/readyz", readyz)
    app.router.add_get("/metrics", metrics)

    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()
    return runner
