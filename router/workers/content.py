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

import re
from typing import Any

from ..observability import get_logger
from ..routing import OP_DELETE
from .runner import WorkerContext

log = get_logger("content")


def compile_keywords(keywords: list[str]) -> list[tuple[str, re.Pattern[str]]]:
    """Compile keywords to word-boundary patterns.

    Naive substring matching is the obvious implementation and it is wrong in a
    way that only shows up against real data: with ``ai`` in the keyword list,
    ``"ai" in text`` fires on *said*, *again*, *email* and *chair*. Running this
    against the live firehose produced a stream of matches that were almost all
    false positives — which would be worse than useless on a notification path,
    because it trains whoever receives them to ignore them.

    The boundary is expressed as ``(?<!\\w) ... (?!\\w)`` rather than ``\\b``.
    ``\\b`` is a *transition* between a word and non-word character, so it can
    never match next to a keyword that itself ends in punctuation: ``\\bc\\+\\+\\b``
    matches nothing at all, because the character after ``+`` would have to be a
    word character. The lookaround form asks the question we actually mean —
    "not glued to a word" — and handles ``c++``, ``.NET`` and ``ai`` alike.

    Known limitation: word characters are undefined for scripts that do not
    delimit words with spaces (Japanese, Chinese, Thai), where this will not
    match mid-string. Doing that properly needs segmentation, which is well
    outside this exercise — so the language filter is the honest mitigation,
    and the limitation is stated rather than hidden.
    """
    compiled = []
    for keyword in keywords:
        pattern = re.compile(rf"(?<!\w){re.escape(keyword)}(?!\w)", re.IGNORECASE)
        compiled.append((keyword, pattern))
    return compiled


class ContentHandler:
    name = "content"

    def __init__(self) -> None:
        self.ctx: WorkerContext | None = None
        self.keywords: list[tuple[str, re.Pattern[str]]] = []
        self.languages: set[str] = set()

    async def setup(self, ctx: WorkerContext) -> None:
        self.ctx = ctx
        # Compiled once at startup, not per event: this runs on every post on
        # the firehose, so re-compiling would be the hot loop.
        self.keywords = compile_keywords(ctx.settings.keywords)
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

        matched = [keyword for keyword, pattern in self.keywords if pattern.search(text)]
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
