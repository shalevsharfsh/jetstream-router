"""Single entrypoint for every role.

One image, one command, ``ROLE`` selects behaviour. The alternative — an image
per worker — buys stronger isolation of dependencies at the cost of five builds,
five pushes and five things to keep in step. At this size the single image is
clearly right; the moment two paths needed genuinely different dependencies
(say one pulled in a model runtime) it would stop being right, and the split is
mechanical because each role is already a separate module.

Note that "one image" does not weaken runtime isolation at all: each role still
runs as its own Deployment, its own pod, its own consumer, its own scaling
policy. Sharing a build artifact is not sharing a failure domain.
"""

from __future__ import annotations

import asyncio
import os
import sys

from .routing import Destination


def _run_worker(destination: Destination, handler_factory) -> None:
    from .workers.runner import run_worker

    asyncio.run(run_worker(destination, handler_factory()))


def main() -> None:
    role = os.environ.get("ROLE", "").strip().lower()

    if role == "tap":
        from .tap import main as tap_main

        asyncio.run(tap_main())
        return

    if role == "content":
        from .workers.content import ContentHandler

        _run_worker(Destination.CONTENT, ContentHandler)
        return

    if role == "engagement":
        from .workers.engagement import EngagementHandler

        _run_worker(Destination.ENGAGEMENT, EngagementHandler)
        return

    if role == "graph":
        from .workers.graph import GraphHandler

        _run_worker(Destination.GRAPH, GraphHandler)
        return

    if role == "retraction":
        from .workers.retraction import RetractionHandler

        _run_worker(Destination.RETRACTION, RetractionHandler)
        return

    if role == "other":
        from .workers.other import OtherHandler

        _run_worker(Destination.OTHER, OtherHandler)
        return

    print(
        f"ROLE must be one of: tap, content, engagement, graph, retraction, other "
        f"(got {role!r})",
        file=sys.stderr,
    )
    raise SystemExit(2)


if __name__ == "__main__":
    main()
