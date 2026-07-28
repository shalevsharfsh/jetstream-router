"""Event classification — the core of the service.

Everything else in this repo is plumbing around this module. It is deliberately
pure: no I/O, no clock, no network. Given a decoded Jetstream event it returns
the destination that event belongs to, or ``None`` if the event is malformed.
That purity is what makes the routing table exhaustively testable without
standing up a broker.

Jetstream event shapes (verified against the live firehose, not the docs)::

    # commit / create — carries a record
    {"did": "...", "time_us": 1785266126673819, "kind": "commit",
     "commit": {"rev": "...", "operation": "create", "rkey": "...",
                "collection": "app.bsky.feed.like",
                "record": {"subject": {"uri": "at://...", "cid": "..."}}}}

    # commit / delete — NOTE: no "record" key at all
    {"did": "...", "time_us": ..., "kind": "commit",
     "commit": {"rev": "...", "operation": "delete", "rkey": "...",
                "collection": "app.bsky.feed.post"}}

    # account (and the analogous "identity") — no "commit" key at all
    {"did": "...", "time_us": ..., "kind": "account",
     "account": {"active": false, "status": "deleted", "seq": ...}}

The absence of ``record`` on deletes is a real constraint on what the retraction
path can do; see ``workers/retraction.py`` and DESIGN.md.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

SUBJECT_PREFIX = "bsky"

# Jetstream's own vocabulary, spelled out so typos surface as NameErrors.
KIND_COMMIT = "commit"
OP_CREATE = "create"
OP_UPDATE = "update"
OP_DELETE = "delete"


class Destination(StrEnum):
    """One destination == one independently deployed, independently scaled worker.

    Adding a value here is a deliberate act: it implies a new Deployment, a new
    durable consumer, its own DLQ and its own scaling policy. Adding a *collection*
    to an existing destination is by contrast just a config change — see
    ``DEFAULT_COLLECTION_ROUTES`` and ``Settings.collection_routes``.
    """

    CONTENT = "content"
    ENGAGEMENT = "engagement"
    GRAPH = "graph"
    RETRACTION = "retraction"
    OTHER = "other"

    @property
    def subject(self) -> str:
        """NATS subject this destination's worker consumes from."""
        return f"{SUBJECT_PREFIX}.{self.value}"

    @property
    def dlq_subject(self) -> str:
        return f"{SUBJECT_PREFIX}.dlq.{self.value}"


# Which collection feeds which worker. Overridable at runtime from a ConfigMap
# (JSON in ROUTER_COLLECTION_ROUTES), so onboarding a new lexicon onto an
# existing path needs no image rebuild.
DEFAULT_COLLECTION_ROUTES: dict[str, Destination] = {
    "app.bsky.feed.post": Destination.CONTENT,
    "app.bsky.feed.like": Destination.ENGAGEMENT,
    "app.bsky.feed.repost": Destination.ENGAGEMENT,
    "app.bsky.graph.follow": Destination.GRAPH,
}


@dataclass(frozen=True, slots=True)
class Route:
    """The classification verdict for a single event."""

    destination: Destination
    kind: str
    collection: str | None
    operation: str | None
    did: str | None
    time_us: int | None
    #: Why this destination was chosen. Emitted as a metric label and a message
    #: header so a surprising routing decision is greppable in production
    #: rather than requiring a debugger.
    reason: str

    @property
    def subject(self) -> str:
        return self.destination.subject

    def headers(self) -> dict[str, str]:
        """Classification carried alongside the payload.

        Workers get the verdict for free instead of re-deriving it, and the
        headers are what an operator sees when inspecting a stuck message.
        """
        out = {
            "destination": self.destination.value,
            "kind": self.kind,
            "reason": self.reason,
        }
        if self.collection:
            out["collection"] = self.collection
        if self.operation:
            out["operation"] = self.operation
        if self.did:
            out["did"] = self.did
        if self.time_us is not None:
            out["time_us"] = str(self.time_us)
        return out


def classify(
    event: Any,
    collection_routes: Mapping[str, Destination] | None = None,
) -> Route | None:
    """Map a decoded Jetstream event to its destination.

    Returns ``None`` for anything structurally unusable. Callers count those
    separately: a malformed event is an upstream-schema signal, not just
    another unrouted event, and conflating the two hides schema drift.

    Precedence is deliberate and load-bearing:

    1. Non-commit kinds (``identity`` / ``account``) have no collection at all,
       so they can never match the collection table.
    2. **Deletes win over collection.** A deleted post goes to the retraction
       worker, not the content worker. The brief asks for retraction to be "a
       distinct path handled separately from creates", and a delete carries no
       record — so the content worker has nothing to match against anyway.
       Getting this backwards is the single easiest way to break the routing.
    3. Otherwise the collection table decides.
    4. Anything left over is OTHER, which is counted rather than dropped.
    """
    if not isinstance(event, Mapping):
        return None

    kind = event.get("kind")
    if not isinstance(kind, str) or not kind:
        return None

    did = event.get("did") if isinstance(event.get("did"), str) else None
    time_us = event.get("time_us") if isinstance(event.get("time_us"), int) else None

    # (1) identity / account — real events, no downstream work wired up yet.
    if kind != KIND_COMMIT:
        return Route(
            destination=Destination.OTHER,
            kind=kind,
            collection=None,
            operation=None,
            did=did,
            time_us=time_us,
            reason="non-commit-kind",
        )

    commit = event.get("commit")
    if not isinstance(commit, Mapping):
        return None

    collection = commit.get("collection")
    operation = commit.get("operation")
    if not isinstance(collection, str) or not collection:
        return None
    if not isinstance(operation, str) or not operation:
        return None

    # (2) Deletes take precedence over the collection table.
    if operation == OP_DELETE:
        return Route(
            destination=Destination.RETRACTION,
            kind=kind,
            collection=collection,
            operation=operation,
            did=did,
            time_us=time_us,
            reason="delete-precedence",
        )

    routes = DEFAULT_COLLECTION_ROUTES if collection_routes is None else collection_routes

    # (3) Collection table.
    destination = routes.get(collection)
    if destination is not None:
        return Route(
            destination=destination,
            kind=kind,
            collection=collection,
            operation=operation,
            did=did,
            time_us=time_us,
            reason="collection-map",
        )

    # (4) Known-shape event we have no path for. Counted, never silently dropped —
    # a rising OTHER rate is how we learn the network shipped a new lexicon.
    return Route(
        destination=Destination.OTHER,
        kind=kind,
        collection=collection,
        operation=operation,
        did=did,
        time_us=time_us,
        reason="unmapped-collection",
    )


def wanted_collections(
    collection_routes: Mapping[str, Destination] | None = None,
) -> list[str]:
    """Collections to request server-side via Jetstream's ``wantedCollections``.

    Derived from the routing table rather than configured separately, so the
    filter can never drift out of sync with what we actually know how to route.
    This is the cheapest backpressure in the system: events filtered here cost
    us no bandwidth, no parse and no broker write. Measured against the live
    firehose it is roughly an order of magnitude less traffic.

    Note this filter only constrains ``commit`` events — Jetstream still
    delivers ``identity`` and ``account`` regardless, which is why OTHER exists.
    """
    routes = DEFAULT_COLLECTION_ROUTES if collection_routes is None else collection_routes
    return sorted(routes)
