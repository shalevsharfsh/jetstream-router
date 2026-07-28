"""Worker logic tests.

These exercise the handlers against a fake Redis rather than a real one, which
is the payoff of keeping handlers free of broker concerns: the interesting
behaviour (windowing, threshold crossing, alert de-duplication) is testable with
no infrastructure at all.
"""

from __future__ import annotations

import fakeredis.aioredis
import pytest

from router.config import Settings
from router.observability import ALERTS_RAISED
from router.windows import bump_and_total, claim_alert
from router.workers.content import ContentHandler
from router.workers.engagement import EngagementHandler, subject_uri
from router.workers.graph import GraphHandler, followee_did
from router.workers.runner import WorkerContext


@pytest.fixture
def redis():
    return fakeredis.aioredis.FakeRedis(decode_responses=True)


def ctx(redis, **overrides) -> WorkerContext:
    settings = Settings(**overrides)
    return WorkerContext(settings, redis)


def like(uri: str, did: str = "did:plc:actor") -> dict:
    return {
        "did": did,
        "time_us": 1785266126673819,
        "kind": "commit",
        "commit": {
            "operation": "create",
            "collection": "app.bsky.feed.like",
            "rkey": "3mrpyre2sga2y",
            "record": {"subject": {"uri": uri, "cid": "bafy..."}},
        },
    }


def follow(target: str, did: str = "did:plc:actor") -> dict:
    return {
        "did": did,
        "time_us": 1785266126680550,
        "kind": "commit",
        "commit": {
            "operation": "create",
            "collection": "app.bsky.graph.follow",
            "rkey": "3mrpyref6452f",
            "record": {"subject": target},
        },
    }


def post(text: str, langs: list[str] | None = None) -> dict:
    record: dict = {"text": text}
    if langs is not None:
        record["langs"] = langs
    return {
        "did": "did:plc:author",
        "time_us": 1785266126687031,
        "kind": "commit",
        "commit": {
            "operation": "create",
            "collection": "app.bsky.feed.post",
            "rkey": "3mrpyre7fdk25",
            "record": record,
        },
    }


def alert_count(worker: str, alert: str) -> float:
    return ALERTS_RAISED.labels(worker=worker, alert=alert)._value.get()


# ------------------------------------------------------- subject extraction --


def test_like_and_follow_subjects_have_different_shapes() -> None:
    """The reason there are two extractors instead of one shared helper.

    Likes/reposts nest the target under ``subject.uri``; follows put a bare DID
    string in ``subject``. A single "clever" extractor would have to guess.
    """
    assert subject_uri(like("at://did:plc:x/app.bsky.feed.post/abc")) == (
        "at://did:plc:x/app.bsky.feed.post/abc"
    )
    assert followee_did(follow("did:plc:target")) == "did:plc:target"

    # And crucially, neither accepts the other's shape.
    assert subject_uri(follow("did:plc:target")) is None
    assert followee_did(like("at://did:plc:x/app.bsky.feed.post/abc")) is None


# ------------------------------------------------------------- engagement --


async def test_engagement_alerts_once_when_threshold_is_crossed(redis) -> None:
    handler = EngagementHandler()
    await handler.setup(ctx(redis, engagement_threshold=5, engagement_window_s=60))
    uri = "at://did:plc:x/app.bsky.feed.post/hot"

    before = alert_count("engagement", "unusual-traction")

    # Four likes: below threshold, no alert.
    for _ in range(4):
        await handler.handle(like(uri), {})
    assert alert_count("engagement", "unusual-traction") == before

    # Fifth crosses it.
    await handler.handle(like(uri), {})
    assert alert_count("engagement", "unusual-traction") == before + 1

    # Ten more while still above threshold must NOT produce ten more alerts.
    # Without the claim, a popular post pages you once per like.
    for _ in range(10):
        await handler.handle(like(uri), {})
    assert alert_count("engagement", "unusual-traction") == before + 1


async def test_engagement_counts_are_per_target_not_global(redis) -> None:
    handler = EngagementHandler()
    await handler.setup(ctx(redis, engagement_threshold=3))

    before = alert_count("engagement", "unusual-traction")
    for i in range(6):
        await handler.handle(like(f"at://did:plc:x/app.bsky.feed.post/{i}"), {})
    # Six likes spread over six posts must not look like one viral post.
    assert alert_count("engagement", "unusual-traction") == before


async def test_engagement_ignores_events_without_a_subject(redis) -> None:
    handler = EngagementHandler()
    await handler.setup(ctx(redis, engagement_threshold=1))
    broken = like("x")
    broken["commit"]["record"] = {}
    await handler.handle(broken, {})  # must not raise -> must not trigger a retry


# ------------------------------------------------------------------ graph --


async def test_follow_burst_keys_on_the_followee_not_the_follower(redis) -> None:
    handler = GraphHandler()
    await handler.setup(ctx(redis, follow_threshold=3, follow_window_s=60))
    before = alert_count("graph", "follow-burst")

    # Three different accounts following one target is a burst...
    for i in range(3):
        await handler.handle(follow("did:plc:target", did=f"did:plc:follower{i}"), {})
    assert alert_count("graph", "follow-burst") == before + 1

    # ...whereas one account following three different targets is not.
    before = alert_count("graph", "follow-burst")
    for i in range(3):
        await handler.handle(follow(f"did:plc:other{i}", did="did:plc:busy"), {})
    assert alert_count("graph", "follow-burst") == before


# ---------------------------------------------------------------- content --


async def test_content_matches_keywords_case_insensitively(redis) -> None:
    handler = ContentHandler()
    await handler.setup(ctx(redis, keywords=["kubernetes"], languages=[]))
    before = alert_count("content", "keyword-match")
    await handler.handle(post("Deploying KUBERNETES today"), {})
    assert alert_count("content", "keyword-match") == before + 1


async def test_content_language_filter_excludes_non_matching_langs(redis) -> None:
    handler = ContentHandler()
    await handler.setup(ctx(redis, keywords=["hello"], languages=["en"]))
    before = alert_count("content", "keyword-match")

    await handler.handle(post("hello world", langs=["ja"]), {})
    assert alert_count("content", "keyword-match") == before

    await handler.handle(post("hello world", langs=["en"]), {})
    assert alert_count("content", "keyword-match") == before + 1


async def test_content_ignores_posts_without_text(redis) -> None:
    handler = ContentHandler()
    await handler.setup(ctx(redis, keywords=["anything"]))
    empty = post("")
    del empty["commit"]["record"]["text"]
    await handler.handle(empty, {})


async def test_content_never_needs_the_delete_path(redis) -> None:
    """A delete reaching content means routing broke; it must not crash."""
    handler = ContentHandler()
    await handler.setup(ctx(redis, keywords=["x"]))
    deletion = {"did": "did:plc:a", "kind": "commit",
                "commit": {"operation": "delete", "collection": "app.bsky.feed.post",
                           "rkey": "r"}}
    await handler.handle(deletion, {})


# ---------------------------------------------------------------- windows --


async def test_window_expires_old_buckets(redis) -> None:
    """Counts must decay: a burst an hour ago is not a burst now."""
    now = 1_000_000.0
    for _ in range(5):
        total = await bump_and_total(redis, "t", "target", window_s=60, now=now)
    assert total == 5

    # Same key, 10 minutes later — every bucket in the window has rolled over.
    later = await bump_and_total(redis, "t", "target", window_s=60, now=now + 600)
    assert later == 1


async def test_claim_alert_is_single_shot_within_cooldown(redis) -> None:
    assert await claim_alert(redis, "ns", "member", cooldown_s=60) is True
    assert await claim_alert(redis, "ns", "member", cooldown_s=60) is False
    # A different member is unaffected.
    assert await claim_alert(redis, "ns", "other", cooldown_s=60) is True
