"""Routing tests.

The brief asks for "a couple of meaningful tests around your routing and core
logic" rather than broad coverage, so these target the decisions that would
actually hurt if they regressed: delete precedence, the malformed/unroutable
distinction, and the fact that the server-side filter is derived from the
routing table rather than maintained alongside it.

Fixtures are trimmed from events captured off the live firehose, not invented,
so the shapes are real — including the fact that deletes carry no record.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from router.routing import (
    DEFAULT_COLLECTION_ROUTES,
    Destination,
    Route,
    classify,
    wanted_collections,
)


def commit(collection: str, operation: str = "create", record: dict | None = None) -> dict:
    body: dict = {"rev": "3mrpyre337i2y", "operation": operation,
                  "collection": collection, "rkey": "3mrpyre2sga2y"}
    if record is not None:
        body["record"] = record
    return {"did": "did:plc:abc", "time_us": 1785266126673819, "kind": "commit", "commit": body}


# --------------------------------------------------------------- happy path --


@pytest.mark.parametrize(
    "collection,expected",
    [
        ("app.bsky.feed.post", Destination.CONTENT),
        ("app.bsky.feed.like", Destination.ENGAGEMENT),
        ("app.bsky.feed.repost", Destination.ENGAGEMENT),
        ("app.bsky.graph.follow", Destination.GRAPH),
    ],
)
def test_creates_route_by_collection(collection: str, expected: Destination) -> None:
    route = classify(commit(collection, "create", {}))
    assert route is not None
    assert route.destination is expected
    assert route.reason == "collection-map"
    assert route.subject == f"bsky.{expected.value}"


def test_post_update_goes_to_content_like_a_create() -> None:
    """Updates are still content to be matched; only deletes are special."""
    route = classify(commit("app.bsky.feed.post", "update", {"text": "edited"}))
    assert route is not None
    assert route.destination is Destination.CONTENT


# ------------------------------------------------------- delete precedence --


@pytest.mark.parametrize("collection", sorted(DEFAULT_COLLECTION_ROUTES))
def test_delete_beats_collection_for_every_routed_collection(collection: str) -> None:
    """The load-bearing rule: a delete never reaches the create-side worker.

    Parametrised across the whole table rather than one example, because the
    failure mode is someone adding a collection later and quietly reintroducing
    the bug for that one type only.
    """
    route = classify(commit(collection, "delete"))
    assert route is not None
    assert route.destination is Destination.RETRACTION
    assert route.reason == "delete-precedence"


def test_delete_of_unmapped_collection_still_routes_to_retraction() -> None:
    """Retraction is defined by the operation, not by the collection table."""
    route = classify(commit("app.bsky.graph.block", "delete"))
    assert route is not None
    assert route.destination is Destination.RETRACTION


def test_real_delete_payload_has_no_record() -> None:
    """Guards the assumption the retraction handler is built on.

    If Jetstream ever started including records on deletes this test would keep
    passing, but the handler's documented limitation could be lifted. It exists
    to pin the shape we observed so the constraint is asserted, not remembered.
    """
    event = commit("app.bsky.feed.like", "delete")
    assert "record" not in event["commit"]
    route = classify(event)
    assert route is not None and route.destination is Destination.RETRACTION


# ------------------------------------------------------------------ other --


def test_non_commit_kinds_are_routed_not_dropped() -> None:
    for kind in ("identity", "account"):
        route = classify({"did": "did:plc:x", "time_us": 1, "kind": kind, kind: {}})
        assert route is not None
        assert route.destination is Destination.OTHER
        assert route.reason == "non-commit-kind"
        assert route.collection is None


def test_unknown_collection_is_counted_as_other_with_a_distinct_reason() -> None:
    """OTHER must distinguish 'no collection' from 'collection we don't handle'.

    They imply different responses: the first is expected forever, the second is
    the signal that a new lexicon is worth a worker.
    """
    route = classify(commit("app.bsky.graph.block", "create", {}))
    assert route is not None
    assert route.destination is Destination.OTHER
    assert route.reason == "unmapped-collection"
    assert route.collection == "app.bsky.graph.block"


# -------------------------------------------------------------- malformed --


@pytest.mark.parametrize(
    "event",
    [
        pytest.param("not a mapping", id="not-a-mapping"),
        pytest.param(None, id="none"),
        pytest.param([], id="list"),
        pytest.param({}, id="empty"),
        pytest.param({"kind": ""}, id="empty-kind"),
        pytest.param({"kind": 7}, id="non-string-kind"),
        pytest.param({"kind": "commit"}, id="commit-without-body"),
        pytest.param({"kind": "commit", "commit": "nope"}, id="commit-not-a-mapping"),
        pytest.param({"kind": "commit", "commit": {"operation": "create"}}, id="no-collection"),
        pytest.param(
            {"kind": "commit", "commit": {"collection": "app.bsky.feed.post"}}, id="no-operation"
        ),
    ],
)
def test_malformed_events_return_none(event) -> None:
    """None means "unusable", which the caller counts separately from unrouted.

    Conflating the two would hide upstream schema drift inside a metric that is
    expected to be non-zero anyway.
    """
    assert classify(event) is None


def test_malformed_is_distinct_from_unroutable() -> None:
    assert classify({"kind": "commit", "commit": {}}) is None
    assert classify(commit("app.bsky.unknown.thing")) is not None


# ---------------------------------------------------------------- headers --


def test_headers_are_all_strings_and_carry_the_verdict() -> None:
    """NATS headers are string-valued; a stray int would fail at publish time."""
    route = classify(commit("app.bsky.feed.like", "create", {}))
    assert route is not None
    headers = route.headers()
    assert all(isinstance(k, str) and isinstance(v, str) for k, v in headers.items())
    assert headers["destination"] == "engagement"
    assert headers["reason"] == "collection-map"
    assert headers["time_us"] == "1785266126673819"


def test_route_is_immutable() -> None:
    """A classification verdict should not be editable downstream."""
    route = classify(commit("app.bsky.feed.post", "create", {}))
    assert isinstance(route, Route)
    with pytest.raises(FrozenInstanceError):
        route.destination = Destination.OTHER  # type: ignore[misc]


# ------------------------------------------------------- configurable table --


def test_custom_routing_table_overrides_defaults() -> None:
    """Onboarding a collection onto an existing path is config, not a deploy."""
    table = {**DEFAULT_COLLECTION_ROUTES, "app.bsky.graph.block": Destination.GRAPH}
    route = classify(commit("app.bsky.graph.block", "create", {}), table)
    assert route is not None
    assert route.destination is Destination.GRAPH


def test_wanted_collections_tracks_the_routing_table() -> None:
    """The server-side filter must be derived, never maintained in parallel.

    If these could drift, the tap would either pay for events it cannot route or
    silently stop receiving ones it can.
    """
    assert wanted_collections() == sorted(DEFAULT_COLLECTION_ROUTES)

    table = {**DEFAULT_COLLECTION_ROUTES, "app.bsky.graph.block": Destination.GRAPH}
    assert "app.bsky.graph.block" in wanted_collections(table)
