"""Social-graph path — detecting a burst of follows for a single account.

Counts are keyed on the account being *followed* (``commit.record.subject``,
which for follows is a bare DID string), not the follower. A sudden spike of
distinct accounts following one target inside a short window is the shape of
both organic virality and coordinated inauthentic behaviour; distinguishing
those two is a modelling problem well outside this exercise, so the handler
raises the signal and says so rather than pretending to judge.

Lowest-volume real path (~5.6% of routed events), and entirely tolerant of a
cold start — which is why this one scales from zero under KEDA.
"""

from __future__ import annotations

from typing import Any

from ..observability import get_logger
from .runner import WorkerContext

log = get_logger("graph")

NAMESPACE = "follow"


def followee_did(event: dict[str, Any]) -> str | None:
    """For ``app.bsky.graph.follow``, ``record.subject`` is a plain DID string."""
    record = (event.get("commit") or {}).get("record") or {}
    subject = record.get("subject")
    if isinstance(subject, str) and subject.startswith("did:"):
        return subject
    return None


class GraphHandler:
    name = "graph"

    def __init__(self) -> None:
        self.ctx: WorkerContext | None = None

    async def setup(self, ctx: WorkerContext) -> None:
        self.ctx = ctx
        log.info(
            "graph handler ready",
            extra={"context": {"window_s": ctx.settings.follow_window_s,
                               "threshold": ctx.settings.follow_threshold}},
        )

    async def handle(self, event: dict[str, Any], headers: dict[str, str]) -> None:
        assert self.ctx is not None
        target = followee_did(event)
        if target is None:
            return

        settings = self.ctx.settings
        total = await self.ctx.store.bump_and_total(
            NAMESPACE, target, settings.follow_window_s
        )
        if total < settings.follow_threshold:
            return

        if not await self.ctx.store.claim_alert(NAMESPACE, target, settings.follow_window_s):
            return

        self.ctx.alert(
            self.name,
            "follow-burst",
            target_did=target,
            count=total,
            window_s=settings.follow_window_s,
            threshold=settings.follow_threshold,
            # Counted follow events, not distinct followers: deduplicating
            # actors would need a per-target set (HyperLogLog would do it
            # cheaply). Called out rather than silently conflated.
            metric="follow_events",
        )
