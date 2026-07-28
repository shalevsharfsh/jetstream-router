"""Tap tests, driven by a fake Jetstream server.

Running a real WebSocket server in-process is what makes the tap's genuinely
hard behaviour testable: reconnection, cursor resume, and the backpressure
policy. Those are the parts most likely to be wrong and the parts least likely
to be exercised by pointing the service at the live firehose and eyeballing
logs — you cannot make the real Jetstream drop your connection on cue.

The fake deliberately supports misbehaviour (abrupt close, malformed frames)
because the interesting assertions are about failure, not the happy path.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from urllib.parse import parse_qs, urlparse

import pytest
import websockets

from router.config import Settings
from router.observability import Health
from router.routing import Destination, classify
from router.tap import Tap, build_url


class FakeJetstream:
    """A minimal stand-in for the real endpoint.

    Records the query strings it was called with, so tests can assert on what
    the tap *asked for* (its collection filter and resume cursor) rather than
    only on what it did with the response.
    """

    def __init__(self, events: list[dict], drop_on: asyncio.Event | None = None) -> None:
        self.events = events
        #: When set by the test, the *first* connection is aborted. Gating the
        #: drop on an explicit signal rather than an event count or a sleep is
        #: what makes the reconnect test deterministic: the test decides exactly
        #: when the connection dies, so there is no race between the tap
        #: recording its cursor and the socket going away.
        self.drop_on = drop_on
        self.connections: list[dict[str, list[str]]] = []
        self.server: websockets.Server | None = None
        self.port = 0

    async def _handler(self, ws) -> None:
        first_connection = not self.connections
        self.connections.append(parse_qs(urlparse(ws.request.path).query))

        for event in self.events:
            await ws.send(json.dumps(event) if isinstance(event, dict) else event)

        if self.drop_on is not None and first_connection:
            await self.drop_on.wait()
            # Abort the transport rather than sending a close frame. 1006 is a
            # reserved code that may never appear on the wire — it is what the
            # *client* synthesises when a connection dies without a closing
            # handshake, which is exactly the failure we want to reproduce.
            ws.transport.abort()
            return

        # Hold the connection open until one side closes it. Sleeping for a
        # fixed period here would make server shutdown block on the handler.
        await ws.wait_closed()

    async def __aenter__(self) -> FakeJetstream:
        self.server = await websockets.serve(self._handler, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *_) -> None:
        if self.server is not None:
            self.server.close()
            # Bounded: a hung handler should fail the test quickly, not wedge
            # the whole suite.
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self.server.wait_closed(), timeout=5)

    @property
    def url(self) -> str:
        return f"ws://127.0.0.1:{self.port}/subscribe"


def post_event(time_us: int, text: str = "hello kubernetes") -> dict:
    return {
        "did": "did:plc:author",
        "time_us": time_us,
        "kind": "commit",
        "commit": {"operation": "create", "collection": "app.bsky.feed.post",
                   "rkey": "abc", "record": {"text": text, "langs": ["en"]}},
    }


class StubRedis:
    """Just enough Redis for the cursor. Keeps these tests dependency-free."""

    def __init__(self, initial: dict[str, str] | None = None) -> None:
        self.store: dict[str, str] = dict(initial or {})

    async def get(self, key: str):
        return self.store.get(key)

    async def set(self, key: str, value) -> None:
        self.store[key] = str(value)

    async def aclose(self) -> None:  # pragma: no cover
        pass


# ------------------------------------------------------------------- URL ----


def test_url_requests_only_collections_we_can_route() -> None:
    """The cheapest backpressure in the system is not receiving the event."""
    url = build_url(Settings(jetstream_url="wss://example/subscribe"), None)
    query = parse_qs(urlparse(url).query)
    assert sorted(query["wantedCollections"]) == [
        "app.bsky.feed.like",
        "app.bsky.feed.post",
        "app.bsky.feed.repost",
        "app.bsky.graph.follow",
    ]
    assert "cursor" not in query


def test_url_includes_cursor_when_resuming() -> None:
    url = build_url(Settings(jetstream_url="wss://example/subscribe"), 1785266126673819)
    assert parse_qs(urlparse(url).query)["cursor"] == ["1785266126673819"]


# ------------------------------------------------------------- reconnect ----


async def test_tap_reconnects_and_resumes_from_its_cursor() -> None:
    """The behaviour the public reference implementations most often miss.

    Asserts two things at once: that a mid-stream disconnect is survived at all,
    and that the second connection asks to resume rather than silently
    restarting from the live tip and punching a hole in the stream.
    """
    base = 1_000_000_000_000_000
    events = [post_event(base + i) for i in range(4)]
    drop = asyncio.Event()

    async def drain(tap: Tap) -> None:
        """Stand in for the publisher: mark events as durably handled."""
        while True:
            route, _ = await tap.queue.get()
            tap.published_time_us = route.time_us

    async with FakeJetstream(events, drop_on=drop) as fake:
        settings = Settings(
            jetstream_url=fake.url,
            cursor_replay_s=5.0,
            reconnect_max_s=0.1,  # keep the test fast
        )
        tap = Tap(settings, Health())
        tap.redis = StubRedis()

        reader = asyncio.create_task(tap.read_loop())
        publisher = asyncio.create_task(drain(tap))

        # Wait until all four events have been read AND "published", so the tap
        # provably has a cursor before we take the connection away.
        for _ in range(100):
            await asyncio.sleep(0.02)
            if tap.published_time_us == base + 3:
                break
        assert tap.published_time_us == base + 3, "events never reached the publisher"

        drop.set()  # now kill the connection

        for _ in range(100):
            await asyncio.sleep(0.02)
            if len(fake.connections) >= 2:
                break

        tap.stop.set()
        for task in (reader, publisher):
            task.cancel()
        await asyncio.gather(reader, publisher, return_exceptions=True)

    assert len(fake.connections) >= 2, "tap did not reconnect after being dropped"
    # First connect is cold (no cursor); the reconnect must carry one.
    assert "cursor" not in fake.connections[0]
    assert "cursor" in fake.connections[1], "reconnect did not resume from a cursor"

    # Resumed from the last published event, rewound by the replay margin —
    # deliberately reprocessing a little rather than risking a gap.
    assert int(fake.connections[1]["cursor"][0]) == (base + 3) - int(5.0 * 1_000_000)


async def test_cold_start_uses_the_stored_cursor() -> None:
    """A fresh pod must pick up where the previous one left off."""
    stored = 1_785_266_126_673_819
    async with FakeJetstream([]) as fake:
        settings = Settings(jetstream_url=fake.url, cursor_replay_s=5.0)
        tap = Tap(settings, Health())
        tap.redis = StubRedis({settings.cursor_key: str(stored)})

        reader = asyncio.create_task(tap.read_loop())
        for _ in range(40):
            await asyncio.sleep(0.05)
            if fake.connections:
                break
        tap.stop.set()
        reader.cancel()
        await asyncio.gather(reader, return_exceptions=True)

    assert fake.connections
    resumed = int(fake.connections[0]["cursor"][0])
    # Rewound by replay_s, and never past zero.
    assert resumed == stored - int(5.0 * 1_000_000)


# ---------------------------------------------------------- backpressure ----


async def test_shed_policy_drops_oldest_and_keeps_the_queue_bounded() -> None:
    """Under overload the queue must not grow without limit."""
    settings = Settings(queue_maxsize=10, backpressure_policy="shed")
    tap = Tap(settings, Health())

    for i in range(50):
        route = classify(post_event(1_000 + i))
        assert route is not None
        tap._enqueue(route, b"{}")

    assert tap.queue.qsize() == 10, "shed policy failed to bound the queue"

    # The survivors must be the newest events, not the oldest: under sustained
    # overload, fresh data is the useful data.
    newest = []
    while not tap.queue.empty():
        route, _ = tap.queue.get_nowait()
        newest.append(route.time_us)
    assert newest == list(range(1_040, 1_050))


async def test_block_policy_applies_real_backpressure() -> None:
    """The lossless alternative: stop reading rather than drop."""
    settings = Settings(queue_maxsize=2, backpressure_policy="block")
    tap = Tap(settings, Health())

    await tap.queue.put((classify(post_event(1)), b"{}"))
    await tap.queue.put((classify(post_event(2)), b"{}"))

    # A third put must not complete while the queue is full.
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(tap.queue.put((classify(post_event(3)), b"{}")), timeout=0.2)
    assert tap.queue.qsize() == 2


async def test_settings_reject_an_unknown_backpressure_policy() -> None:
    """Fail at startup, not at 3am under load."""
    with pytest.raises(ValueError, match="shed"):
        Settings(backpressure_policy="whatever")


# ------------------------------------------------------------ malformed -----


async def test_malformed_frames_do_not_kill_the_reader() -> None:
    """One bad frame from upstream must not take down ingest."""
    async with FakeJetstream(["{not json", json.dumps(post_event(2_000))]) as fake:
        settings = Settings(jetstream_url=fake.url)
        tap = Tap(settings, Health())
        tap.redis = StubRedis()

        reader = asyncio.create_task(tap.read_loop())
        for _ in range(40):
            await asyncio.sleep(0.05)
            if not tap.queue.empty():
                break
        tap.stop.set()
        reader.cancel()
        await asyncio.gather(reader, return_exceptions=True)

    # The good event that followed the bad one still made it through.
    assert tap.queue.qsize() == 1
    route, _ = tap.queue.get_nowait()
    assert route.destination is Destination.CONTENT
