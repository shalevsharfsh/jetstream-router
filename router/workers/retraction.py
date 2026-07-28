"""Retraction path — every ``operation = delete``, regardless of collection.

This path exists because deletions are a genuinely different kind of work from
creates: they are cleanup and compliance, not analysis. Keeping them on their
own consumer means a backlog of deletions cannot delay notifications, and a bug
in content matching cannot stop retractions from being processed.

**A real constraint, found by reading the wire rather than the docs.** A delete
commit carries no ``record``::

    {"did": "...", "kind": "commit",
     "commit": {"operation": "delete", "collection": "app.bsky.feed.like",
                "rkey": "3mqmrv6uselv2", "rev": "..."}}

There is no ``subject``. So when a like is retracted we know *who* retracted it
and *which record of theirs* it was, but not *which post it pointed at* — which
is exactly what we would need to decrement the engagement counter for that post.

Referential cleanup is therefore impossible without an index the create path
writes: ``(did, collection, rkey) -> subject_uri``. That is a real cost — at
firehose volume it is roughly a write per engagement event, with a TTL, purely
to make deletes resolvable. Whether that is worth paying depends on whether
counts need to be exact or merely indicative, and for a traction *signal* the
answer is usually no.

So this handler does the part that is honest without the index: it records the
retraction, and reports the unresolved ones as a metric rather than pretending
the cleanup happened. Building the index is described in DESIGN.md, not built.
"""

from __future__ import annotations

from typing import Any

from ..observability import RETRACTIONS, get_logger
from ..routing import Destination
from .runner import WorkerContext

log = get_logger("retraction")

NAMESPACE = "retraction"

#: Collections whose deletion *would* require a counter decrement if we had an
#: index to resolve the target. Tracked so the cost of not having one is visible.
COUNTED_COLLECTIONS = {
    "app.bsky.feed.like": Destination.ENGAGEMENT,
    "app.bsky.feed.repost": Destination.ENGAGEMENT,
    "app.bsky.graph.follow": Destination.GRAPH,
}

#: Closed set of values allowed to appear as a metric label. See handle().
KNOWN_COLLECTIONS = set(COUNTED_COLLECTIONS) | {"app.bsky.feed.post"}


class RetractionHandler:
    name = "retraction"

    def __init__(self) -> None:
        self.ctx: WorkerContext | None = None

    async def setup(self, ctx: WorkerContext) -> None:
        self.ctx = ctx
        log.info("retraction handler ready")

    async def handle(self, event: dict[str, Any], headers: dict[str, str]) -> None:
        assert self.ctx is not None
        commit = event.get("commit") or {}
        collection = commit.get("collection") or "unknown"
        did = event.get("did")
        rkey = commit.get("rkey")

        # Durable record of the retraction itself. This is the part that is
        # genuinely useful without an index: "was this record deleted?" is
        # answerable, which is what a compliance or moderation path needs.
        await self.ctx.redis.setex(
            f"{NAMESPACE}:{did}:{collection}:{rkey}",
            self.ctx.settings.engagement_window_s * 4,
            "1",
        )

        unresolvable = collection in COUNTED_COLLECTIONS
        resolution = "no-index" if unresolvable else "not-applicable"

        # Collections are user-extensible in the AT Protocol: anyone can publish
        # records under a lexicon they invented. Using the raw value as a metric
        # label would let a stranger mint unbounded Prometheus time series from
        # the public internet. Bound it to the set we actually know.
        label = collection if collection in KNOWN_COLLECTIONS else "other"

        # A metric, not an alert. Roughly one like-deletion every 150ms arrives
        # on the live firehose; paging a human on each would be indefensible.
        # The rate is the interesting thing, and a counter is what measures a
        # rate. Alerts in this service are reserved for things a person should
        # actually look at.
        RETRACTIONS.labels(collection=label, resolution=resolution).inc()

        log.debug(
            "retraction",
            extra={"context": {
                "did": did,
                "collection": collection,
                "rkey": rkey,
                # Surfaces the limitation instead of hiding it: these are the
                # events whose aggregate impact we cannot undo today.
                "counter_decrement": resolution,
            }},
        )
