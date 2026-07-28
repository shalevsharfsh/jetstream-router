"""Configuration.

Env-var driven, resolved once at import of the process entrypoint. In Kubernetes
these come from a ConfigMap; on AWS they would be Lambda environment variables
populated by CDK. Deliberately no config framework — the surface is small enough
that a dataclass and ``os.environ`` beat a dependency.

Anything an operator might plausibly want to change during an incident (queue
bounds, shed policy, thresholds, the routing table itself) is here rather than
hardcoded.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field

from .routing import DEFAULT_COLLECTION_ROUTES, Destination

JETSTREAM_DEFAULT = "wss://jetstream2.us-east.bsky.network/subscribe"


def _env_str(name: str, default: str) -> str:
    return os.environ.get(name, default).strip() or default


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number, got {raw!r}") from exc


def _env_list(name: str, default: list[str]) -> list[str]:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return list(default)
    return [item.strip() for item in raw.split(",") if item.strip()]


def _collection_routes() -> dict[str, Destination]:
    """Routing table, optionally overridden by a JSON ConfigMap value.

    Shape: ``{"app.bsky.feed.post": "content", ...}``. An unknown destination
    name is a hard failure at startup — a typo here would silently blackhole an
    event type, and failing the pod is far louder than that.
    """
    raw = os.environ.get("ROUTER_COLLECTION_ROUTES", "").strip()
    if not raw:
        return dict(DEFAULT_COLLECTION_ROUTES)

    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError("ROUTER_COLLECTION_ROUTES must be a JSON object")

    valid = {d.value for d in Destination}
    table: dict[str, Destination] = {}
    for collection, dest in parsed.items():
        if dest not in valid:
            raise ValueError(
                f"ROUTER_COLLECTION_ROUTES[{collection!r}] = {dest!r} is not a known "
                f"destination (expected one of {sorted(valid)})"
            )
        table[collection] = Destination(dest)
    return table


@dataclass(frozen=True)
class Settings:
    # --- infrastructure -------------------------------------------------
    nats_url: str = field(default_factory=lambda: _env_str("NATS_URL", "nats://nats:4222"))
    redis_url: str = field(default_factory=lambda: _env_str("REDIS_URL", "redis://redis:6379/0"))
    stream_name: str = field(default_factory=lambda: _env_str("STREAM_NAME", "BSKY"))
    metrics_port: int = field(default_factory=lambda: _env_int("METRICS_PORT", 9090))
    log_level: str = field(default_factory=lambda: _env_str("LOG_LEVEL", "INFO"))

    # --- tap ------------------------------------------------------------
    jetstream_url: str = field(default_factory=lambda: _env_str("JETSTREAM_URL", JETSTREAM_DEFAULT))
    #: Bound on in-flight events between the socket reader and the publisher.
    #: This is the knob that decides how much latency we absorb before the
    #: backpressure policy kicks in.
    queue_maxsize: int = field(default_factory=lambda: _env_int("TAP_QUEUE_MAXSIZE", 10_000))
    #: "shed" (drop oldest, stay current, lose events) or "block" (stop reading
    #: the socket, lose nothing, risk being disconnected). See DESIGN.md — this
    #: is the most consequential single setting in the service.
    backpressure_policy: str = field(
        default_factory=lambda: _env_str("TAP_BACKPRESSURE_POLICY", "shed")
    )
    publish_batch: int = field(default_factory=lambda: _env_int("TAP_PUBLISH_BATCH", 200))
    cursor_key: str = field(default_factory=lambda: _env_str("TAP_CURSOR_KEY", "jetstream:cursor"))
    cursor_interval_s: float = field(
        default_factory=lambda: _env_float("TAP_CURSOR_INTERVAL_S", 2.0)
    )
    #: How far behind the last published event we rewind on resume. Cheap
    #: insurance for at-least-once: we would rather reprocess a few seconds of
    #: events (handlers are idempotent-ish) than punch a hole in the stream.
    cursor_replay_s: float = field(default_factory=lambda: _env_float("TAP_CURSOR_REPLAY_S", 5.0))
    reconnect_max_s: float = field(default_factory=lambda: _env_float("TAP_RECONNECT_MAX_S", 30.0))
    #: Cursor lag beyond which the tap reports itself un-ready. Connected but
    #: 90 seconds behind is not "working" in any sense a downstream consumer
    #: cares about, and a readiness signal that only tracks socket state would
    #: hide exactly that failure.
    max_lag_s: float = field(default_factory=lambda: _env_float("TAP_MAX_LAG_S", 60.0))

    # --- workers --------------------------------------------------------
    worker_batch: int = field(default_factory=lambda: _env_int("WORKER_BATCH", 100))
    worker_max_deliver: int = field(default_factory=lambda: _env_int("WORKER_MAX_DELIVER", 5))
    worker_ack_wait_s: int = field(default_factory=lambda: _env_int("WORKER_ACK_WAIT_S", 30))

    # --- content path ---------------------------------------------------
    keywords: list[str] = field(
        default_factory=lambda: [
            k.lower() for k in _env_list("CONTENT_KEYWORDS", ["kubernetes", "zenity", "bluesky"])
        ]
    )
    languages: list[str] = field(default_factory=lambda: _env_list("CONTENT_LANGUAGES", []))

    # --- engagement path ------------------------------------------------
    engagement_window_s: int = field(default_factory=lambda: _env_int("ENGAGEMENT_WINDOW_S", 60))
    engagement_threshold: int = field(default_factory=lambda: _env_int("ENGAGEMENT_THRESHOLD", 25))

    # --- graph path -----------------------------------------------------
    follow_window_s: int = field(default_factory=lambda: _env_int("FOLLOW_WINDOW_S", 60))
    follow_threshold: int = field(default_factory=lambda: _env_int("FOLLOW_THRESHOLD", 10))

    # --- routing table --------------------------------------------------
    collection_routes: dict[str, Destination] = field(default_factory=_collection_routes)

    def __post_init__(self) -> None:
        if self.backpressure_policy not in ("shed", "block"):
            raise ValueError(
                f"TAP_BACKPRESSURE_POLICY must be 'shed' or 'block', "
                f"got {self.backpressure_policy!r}"
            )
        if self.queue_maxsize < 1:
            raise ValueError("TAP_QUEUE_MAXSIZE must be >= 1")


def load_settings() -> Settings:
    return Settings()
