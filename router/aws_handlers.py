"""Lambda entrypoints — the same handlers, a different transport.

The point of this module is to demonstrate that the fan-out design is not
coupled to NATS or to Kubernetes. The classifier in ``routing.py`` and the four
handlers in ``workers/`` are unchanged; only the plumbing that feeds them and the
store behind them differ. If that were not true, the claim that this is a
portable, broker-agnostic shape would be marketing rather than architecture.

Two differences from the Kubernetes runner are worth understanding, because
they are consequences of the execution model rather than style choices:

**Partial batch failure replaces per-message ack.** A NATS pull consumer acks
each message individually. Lambda's SQS integration takes a whole batch and, by
default, retries the *entire* batch if the invocation fails — so one poison
message forces 499 good ones to be reprocessed. Returning
``batchItemFailures`` narrows that to the records that actually failed. It is
the direct analogue of per-message ack, and forgetting it is one of the most
common and most expensive Lambda-with-SQS mistakes.

**State moves to DynamoDB.** Same bucketed-window algorithm as
``windows.py``, but expressed as an atomic ``ADD`` with a TTL attribute instead
of ``INCR`` with ``EXPIRE``. The algorithm was chosen partly because it ports
cleanly to any store with atomic increment and expiry, which is most of them.
"""

from __future__ import annotations

import os
import time
from typing import Any

import boto3
import orjson

from .config import Settings
from .observability import get_logger, setup_logging
from .windows import BUCKET_S, _buckets

setup_logging(os.environ.get("LOG_LEVEL", "INFO"))
log = get_logger("lambda")

# Module scope: created once per execution environment and reused across
# invocations. Building a client per invocation is a well-known way to add
# tens of milliseconds and a pile of TLS handshakes to every call.
_dynamodb = boto3.resource("dynamodb")
_table = _dynamodb.Table(os.environ["STATE_TABLE"]) if os.environ.get("STATE_TABLE") else None


class DynamoWindowStore:
    """The ``windows.py`` interface, backed by DynamoDB instead of Redis.

    Deliberately mirrors the Redis method names so the handlers do not know or
    care which one they are talking to.
    """

    def __init__(self, table: Any) -> None:
        self.table = table

    async def bump_and_total(
        self, namespace: str, member: str, window_s: int, now: float | None = None
    ) -> int:
        now = time.time() if now is None else now
        buckets = _buckets(window_s, now)

        # Atomic increment. DynamoDB's ADD is the equivalent of Redis INCR, and
        # like INCR it is safe under concurrent writers — which matters because
        # Lambda will happily run many of these at once.
        self.table.update_item(
            Key={"pk": f"{namespace}#{member}", "sk": str(buckets[0])},
            UpdateExpression="ADD #c :one SET #t = :ttl",
            ExpressionAttributeNames={"#c": "count", "#t": "ttl"},
            ExpressionAttributeValues={
                ":one": 1,
                ":ttl": int(now) + window_s + BUCKET_S,
            },
        )

        # A Query over the partition beats N GetItems: one round trip, and the
        # sort key range is exactly the window we want.
        response = self.table.query(
            KeyConditionExpression=(
                boto3.dynamodb.conditions.Key("pk").eq(f"{namespace}#{member}")
                & boto3.dynamodb.conditions.Key("sk").between(
                    str(buckets[-1]), str(buckets[0])
                )
            )
        )
        return sum(int(item.get("count", 0)) for item in response.get("Items", []))

    async def claim_alert(self, namespace: str, member: str, cooldown_s: int) -> bool:
        """Conditional write as the equivalent of Redis SET NX."""
        try:
            self.table.put_item(
                Item={
                    "pk": f"{namespace}#alerted",
                    "sk": member,
                    "ttl": int(time.time()) + cooldown_s,
                },
                ConditionExpression="attribute_not_exists(pk)",
            )
            return True
        except _dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
            return False


def _make_handler(handler_factory):
    """Wrap a handler in the SQS batch protocol."""

    def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
        import asyncio

        from .workers.runner import WorkerContext

        settings = Settings()
        ctx = WorkerContext(settings, DynamoWindowStore(_table))
        handler = handler_factory()

        async def run() -> list[dict[str, str]]:
            await handler.setup(ctx)
            failures: list[dict[str, str]] = []
            for record in event.get("Records", []):
                try:
                    # EventBridge wraps the event; the original Jetstream
                    # payload is under detail.event.
                    body = orjson.loads(record["body"])
                    payload = body.get("detail", {}).get("event", body)
                    await handler.handle(payload, body.get("detail", {}))
                except Exception as exc:
                    # Report only this record as failed. Without this, the whole
                    # batch is retried and 499 innocent messages pay for one.
                    log.error(
                        "record failed",
                        extra={"context": {"messageId": record.get("messageId"),
                                           "error": str(exc)}},
                    )
                    failures.append({"itemIdentifier": record["messageId"]})
            return failures

        return {"batchItemFailures": asyncio.run(run())}

    return lambda_handler


def _content():
    from .workers.content import ContentHandler

    return ContentHandler()


def _engagement():
    from .workers.engagement import EngagementHandler

    return EngagementHandler()


def _graph():
    from .workers.graph import GraphHandler

    return GraphHandler()


def _retraction():
    from .workers.retraction import RetractionHandler

    return RetractionHandler()


def _other():
    from .workers.other import OtherHandler

    return OtherHandler()


content_handler = _make_handler(_content)
engagement_handler = _make_handler(_engagement)
graph_handler = _make_handler(_graph)
retraction_handler = _make_handler(_retraction)
other_handler = _make_handler(_other)
