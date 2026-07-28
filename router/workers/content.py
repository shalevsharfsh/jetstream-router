"""Content path — keyword and language matching on posts, raising notifications.

Latency-sensitive by nature: a notification that arrives ten minutes late is
worth much less than one that arrives now. That is why this is the one worker
pinned at ``minReplicas: 1`` instead of scaling from zero — a cold start on the
notification path is a user-visible delay, whereas a cold start on an
aggregation path costs nothing but a slightly later alert.

**On handling firehose content.** The brief warns that raw post text can contain
anything. This handler therefore treats post text as something to match against,
never something to emit: alerts carry the DID, the record key, and *which*
keyword matched — never the text itself. Logs are the easiest place to
accidentally build a permanent, widely-readable copy of exactly the content you
were told to be careful with.
"""

from __future__ import annotations

from typing import Any

from ..observability import get_logger
from ..routing import OP_DELETE
from .runner import WorkerContext

log = get_logger("content")


class ContentHandler:
    name = "content"

    def __init__(self) -> None:
        self.ctx: WorkerContext | None = None
        self.keywords: list[str] = []
        self.languages: set[str] = set()

    async def setup(self, ctx: WorkerContext) -> None:
        self.ctx = ctx
        self.keywords = [k.lower() for k in ctx.settings.keywords]
        self.languages = {lang.lower() for lang in ctx.settings.languages}
        log.info(
            "content handler ready",
            extra={"context": {"keywords": len(self.keywords),
                               "languages": sorted(self.languages) or "any"}},
        )

    async def handle(self, event: dict[str, Any], headers: dict[str, str]) -> None:
        assert self.ctx is not None
        commit = event.get("commit") or {}

        # Deletes are routed to the retraction path, so seeing one here means the
        # routing table and this worker disagree. Loud, not silent.
        if commit.get("operation") == OP_DELETE:
            log.warning("delete reached content path; check routing")
            return

        record = commit.get("record") or {}
        text = record.get("text")
        if not isinstance(text, str) or not text:
            return

        langs = {str(x).lower() for x in (record.get("langs") or []) if x}
        if self.languages and not (langs & self.languages):
            return

        haystack = text.lower()
        matched = [kw for kw in self.keywords if kw in haystack]
        if not matched:
            return

        self.ctx.alert(
            self.name,
            "keyword-match",
            did=event.get("did"),
            rkey=commit.get("rkey"),
            collection=commit.get("collection"),
            matched=matched,
            langs=sorted(langs),
            # Deliberately: the length of the post, not the post.
            text_len=len(text),
        )
