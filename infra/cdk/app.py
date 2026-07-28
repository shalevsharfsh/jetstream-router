#!/usr/bin/env python3
"""The same topology, expressed as AWS managed services.

This exists so the claim "this design is serverless-shaped" is checkable rather
than rhetorical. It synthesises (``cdk synth``, gated in CI) but has **not** been
deployed to a live account — there is no point pretending otherwise, and the
README says so too.

The mapping is one-to-one with what runs on kind:

    kind / NATS                         AWS
    ---------------------------------   ------------------------------------
    tap Deployment (replicas: 1)        ECS Fargate service (desiredCount: 1)
    NATS subject per destination        EventBridge bus + one rule per type
    durable pull consumer               SQS queue (+ redrive policy)
    worker Deployment                   Lambda, SQS event source
    KEDA ScaledObject (queue depth)     Lambda's native SQS scaling
    max_deliver -> DLQ subject          maxReceiveCount -> DLQ queue
    Redis                               DynamoDB (TTL on the window items)

Two things are worth noticing about that table.

**The tap is Fargate, not Lambda.** It is the one component that cannot be a
function: it owns a long-lived WebSocket and a cursor. A 15-minute execution
ceiling and no connection affinity make FaaS the wrong tool, and pretending
otherwise would be the tell that someone had not thought about it. One always-on
task, everything downstream elastic.

**Routing moves from the tap into EventBridge.** On Kubernetes the tap
classifies and publishes to a per-destination subject. Here it publishes raw
events with the classification as event *detail*, and the rules do the routing.
That is a genuine trade, not a cosmetic one: rules are declarative and let a new
consumer subscribe without redeploying ingest, at the cost of routing logic
living in IaC where it is harder to unit-test. The classifier in
``router/routing.py`` still decides — it just annotates instead of addressing.
"""

from aws_cdk import App, CfnOutput, Duration, RemovalPolicy, Stack
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_ecs as ecs
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_lambda_event_sources as sources
from aws_cdk import aws_logs as logs
from aws_cdk import aws_sqs as sqs
from constructs import Construct

# Mirrors router/routing.py. Each entry becomes a rule, a queue, a DLQ and a
# function — one independent path, exactly as on Kubernetes.
DESTINATIONS = ["content", "engagement", "graph", "retraction", "other"]

# Kept out of both the Lambda bundle and the image build context. Without this
# the asset copy recurses into its own output directory.
ASSET_EXCLUDE = [
    ".venv", ".git", ".github", "infra/cdk/cdk.out", "cdk.out",
    "__pycache__", "*.pyc", ".pytest_cache", ".ruff_cache", "tests", "docs", "k8s",
]

# Per-path tuning. Uniform values would have been less code and worse thinking:
# these differ for the same reasons the Kubernetes replica counts differ.
PATH_CONFIG = {
    # Notification path: latency matters, so keep concurrency available and
    # batch small. Reserved concurrency also stops the noisy engagement path
    # from consuming the whole account's Lambda budget and starving this one.
    "content": {"batch": 10, "window": 0, "reserved": 20, "timeout": 30},
    # ~70% of firehose volume. Large batches amortise invocation cost, and a
    # batching window trades a little latency for far fewer invocations —
    # the single biggest cost lever in this design.
    "engagement": {"batch": 500, "window": 5, "reserved": 50, "timeout": 60},
    # ~1% of volume. Small and cheap; cold starts are irrelevant here.
    "graph": {"batch": 100, "window": 5, "reserved": 10, "timeout": 30},
    # Correctness-sensitive: modest batches so one poison message quarantines
    # less collateral work on retry.
    "retraction": {"batch": 50, "window": 2, "reserved": 10, "timeout": 30},
    # Observability only.
    "other": {"batch": 100, "window": 10, "reserved": 5, "timeout": 30},
}


class JetstreamRouterStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # --- state ------------------------------------------------------
        # Single-table: the cursor, the rolling windows and the alert claims.
        # TTL replaces Redis key expiry, so old window buckets evict themselves
        # rather than needing a sweeper.
        state = dynamodb.Table(
            self,
            "State",
            partition_key=dynamodb.Attribute(name="pk", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="sk", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            time_to_live_attribute="ttl",
            removal_policy=RemovalPolicy.DESTROY,
        )

        # --- the bus ----------------------------------------------------
        bus = events.EventBus(self, "EventBus", event_bus_name="jetstream-router")

        # --- ingest -----------------------------------------------------
        # Public subnets with no NAT gateway: the task only makes outbound
        # calls, and a NAT would be the single largest line on the bill for a
        # service that is otherwise nearly free at rest.
        vpc = ec2.Vpc(
            self,
            "Vpc",
            max_azs=2,
            nat_gateways=0,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="public", subnet_type=ec2.SubnetType.PUBLIC, cidr_mask=24
                )
            ],
        )
        cluster = ecs.Cluster(self, "Cluster", vpc=vpc)

        task = ecs.FargateTaskDefinition(self, "TapTask", cpu=512, memory_limit_mib=1024)
        task.add_container(
            "tap",
            image=ecs.ContainerImage.from_asset("../..", file="Dockerfile"),
            environment={
                "ROLE": "tap",
                "TRANSPORT": "eventbridge",
                "EVENT_BUS_NAME": bus.event_bus_name,
                "STATE_TABLE": state.table_name,
                "TAP_BACKPRESSURE_POLICY": "shed",
            },
            logging=ecs.LogDrivers.aws_logs(
                stream_prefix="tap",
                log_group=logs.LogGroup(
                    self,
                    "TapLogs",
                    retention=logs.RetentionDays.ONE_WEEK,
                    removal_policy=RemovalPolicy.DESTROY,
                ),
            ),
        )
        bus.grant_put_events_to(task.task_role)
        state.grant_read_write_data(task.task_role)

        # desired_count=1 is a correctness constraint, not a capacity choice —
        # two tasks would each hold a WebSocket and double-publish everything.
        # Scaling out requires partitioning by DID (Jetstream's wantedDids),
        # which is described in DESIGN.md and not built.
        tap = ecs.FargateService(
            self,
            "Tap",
            cluster=cluster,
            task_definition=task,
            desired_count=1,
            assign_public_ip=True,
            # Never run two taps, even mid-deploy.
            min_healthy_percent=0,
            max_healthy_percent=100,
            # Roll back automatically instead of sitting in a failed deploy for
            # up to three hours, which is the ECS default.
            circuit_breaker=ecs.DeploymentCircuitBreaker(rollback=True),
        )

        # --- one independent path per destination ------------------------
        for destination in DESTINATIONS:
            cfg = PATH_CONFIG[destination]

            dlq = sqs.Queue(
                self,
                f"{destination.title()}Dlq",
                retention_period=Duration.days(14),
            )
            queue = sqs.Queue(
                self,
                f"{destination.title()}Queue",
                visibility_timeout=Duration.seconds(cfg["timeout"] * 6),
                # The direct analogue of max_deliver in the NATS consumer: a
                # poison message is quarantined instead of blocking the path.
                dead_letter_queue=sqs.DeadLetterQueue(max_receive_count=5, queue=dlq),
            )

            # Routing as data. Adding a consumer for an existing type is a rule
            # change; the tap never learns about it.
            events.Rule(
                self,
                f"{destination.title()}Rule",
                event_bus=bus,
                event_pattern=events.EventPattern(
                    source=["jetstream.tap"],
                    detail_type=["bsky.event"],
                    detail={"destination": [destination]},
                ),
                targets=[targets.SqsQueue(queue)],
            )

            fn = lambda_.Function(
                self,
                f"{destination.title()}Worker",
                runtime=lambda_.Runtime.PYTHON_3_13,
                handler=f"router.aws_handlers.{destination}_handler",
                code=lambda_.Code.from_asset("../..", exclude=ASSET_EXCLUDE),
                timeout=Duration.seconds(cfg["timeout"]),
                memory_size=512,
                # Per-path concurrency ceiling. This is the isolation that
                # matters most on AWS: without it, a backlog on the engagement
                # path can consume the account's whole concurrency pool and
                # throttle every other path — the exact cross-path interference
                # the design exists to prevent.
                reserved_concurrent_executions=cfg["reserved"],
                environment={
                    "ROLE": destination,
                    "STATE_TABLE": state.table_name,
                },
                log_group=logs.LogGroup(
                    self,
                    f"{destination.title()}Logs",
                    retention=logs.RetentionDays.ONE_WEEK,
                    removal_policy=RemovalPolicy.DESTROY,
                ),
            )
            state.grant_read_write_data(fn)

            fn.add_event_source(
                sources.SqsEventSource(
                    queue,
                    batch_size=cfg["batch"],
                    # Batching window is the cost lever: at ~300 events/sec,
                    # per-message invocation would be absurd. Waiting a few
                    # seconds to fill a batch cuts invocations by orders of
                    # magnitude for a latency cost the aggregation paths do
                    # not notice.
                    max_batching_window=Duration.seconds(cfg["window"]) if cfg["window"] else None,
                    # Only the failed records in a batch are retried, rather
                    # than the whole batch. Without this, one bad message
                    # forces 499 good ones to be reprocessed.
                    report_batch_item_failures=True,
                )
            )

        CfnOutput(self, "EventBusName", value=bus.event_bus_name)
        CfnOutput(self, "StateTableName", value=state.table_name)
        CfnOutput(self, "TapServiceName", value=tap.service_name)


app = App()
JetstreamRouterStack(app, "JetstreamRouterStack")
app.synth()
