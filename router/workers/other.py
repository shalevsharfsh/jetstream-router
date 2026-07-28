"""The 'everything else' path — deliberately boring, deliberately present.

Two kinds of event land here: ``identity`` and ``account`` events (which have no
collection and so can never match the routing table), and commits under a
collection we have no handler for.

It would have been less code to drop these at the tap. That would also mean the
day Bluesky ships a new lexicon, or the day someone edits the routing ConfigMap
with a typo, the events would vanish with no trace and the first symptom would
be a business question nobody could answer. Counting them makes "we are
receiving something we don't understand" a graph instead of an outage.

This is also the seam where new paths get born: a rising ``unmapped-collection``
rate is the evidence that justifies building a real worker for it.
"""

from __future__ import annotations

from typing import Any

from ..observability import get_logger
from .runner import WorkerContext

log = get_logger("other")


class OtherHandler:
    name = "other"

    def __init__(self) -> None:
        self.ctx: WorkerContext | None = None

    async def setup(self, ctx: WorkerContext) -> None:
        self.ctx = ctx
        log.info("other handler ready (observability only)")

    async def handle(self, event: dict[str, Any], headers: dict[str, str]) -> None:
        # The classification counter in the tap already records these by
        # destination and reason; there is no work to do beyond acknowledging
        # them so they don't accumulate in the stream. Account deactivations and
        # deletions are the one sub-case worth a line, since they are the events
        # a retention or compliance path would eventually need.
        if event.get("kind") == "account":
            account = event.get("account") or {}
            if account.get("status") in ("deleted", "deactivated", "suspended"):
                log.info(
                    "account status change",
                    extra={"context": {"did": event.get("did"),
                                       "status": account.get("status")}},
                )
