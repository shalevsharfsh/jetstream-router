# jetstream-router

One ingest, many event types, fanned out to workers that run, fail and scale
independently.

Consumes the [Bluesky Jetstream](https://docs.bsky.app/blog/jetstream) firehose
over a single WebSocket, classifies every event, and routes it to a
type-specific worker. Each worker is its own Deployment with its own durable
consumer, its own retry budget, its own dead-letter queue and its own scaling
policy — so a failure or a backlog on one path cannot touch another.

Read [DESIGN.md](DESIGN.md) for the reasoning, the trade-offs, and what I chose
not to build. [AI.md](AI.md) covers the agent setup and the prompts behind the
real decisions.

> **Stack note, up front.** The brief specified Go + Kubernetes. This is Python
> + Kubernetes, with the serverless equivalent as real, synthesising CDK in
> `infra/cdk/`. That is a deliberate deviation and DESIGN.md argues it in full
> rather than in passing — including what it costs.

---

## The shape

```
              wss://jetstream2.us-east.bsky.network/subscribe
                    ?wantedCollections=…&cursor=<time_us>
                                  │
                        ┌─────────▼──────────┐
                        │   TAP  (1 replica) │   the only stateful component
                        │  classify · bound  │   asyncio · orjson
                        │  backpressure      │   cursor → Redis
                        └─────────┬──────────┘
                                  │  publish bsky.<destination>
                        ┌─────────▼──────────┐
                        │  NATS JetStream    │  one durable consumer per path
                        └┬────┬────┬────┬───┬┘
        ┌────────────────┘    │    │    │   └──────────────┐
        ▼                     ▼    ▼    ▼                  ▼
   content              engagement  graph  retraction    other
   post/create          like+repost follow  op=delete    identity/account
   keyword match        rolling     burst   cleanup      + unknown types
   → notification       counts      detect  path         (counted only)
        │                     │      │        │              │
     min 1              KEDA 1..6  KEDA 1..4  min 1        min 1
                                  │
                          Redis (windows, cursor, claims)
```

## Run it

Requires Docker, `kind`, `kubectl`. Nothing else — no AWS account, no registry.

```bash
make deploy      # kind cluster + KEDA + build + load + apply. ~3-4 minutes.
make demo        # tail the alerts every path is producing
```

`make deploy` is idempotent; re-run it after changing code.

### See it working

```bash
make demo          # ALERT lines from all four paths, live
make logs          # everything, not just alerts
make metrics       # tap counters: received, classified, published, shed, lag
make scale-watch   # watch KEDA move replicas per path, independently
make chaos         # kill the engagement worker; the other paths carry on
make test          # 50 tests, no infrastructure needed
make down          # delete the namespace, keep the cluster
make clean         # delete the cluster
```

Within a minute or so of `make demo` you should see `keyword-match` from the
content path and `unusual-traction` from engagement. `follow-burst` is rarer by
nature — lower `FOLLOW_THRESHOLD` in `k8s/20-config.yaml` to force it.

### The one thing worth watching

```bash
make chaos
```

Deletes the engagement worker — the busiest path, ~79% of all traffic — and
shows the other three continuing without a blip, then engagement draining its
backlog on restart. That single command is the property the whole design exists
to provide. Measured on this cluster: with engagement down for ~30s, content
still processed 1,067 events and retraction 258, and engagement's consumer
returned to zero pending after restart.

## Configuration

Everything tunable lives in `k8s/20-config.yaml` (a ConfigMap). The interesting
ones:

| Setting | Default | Why you'd change it |
|---|---|---|
| `TAP_BACKPRESSURE_POLICY` | `shed` | `shed` drops the oldest queued events under overload and stays current; `block` stops reading the socket and loses nothing but falls behind. The most consequential setting in the service — see DESIGN.md §6. |
| `TAP_QUEUE_MAXSIZE` | `10000` | How much latency is absorbed before that policy engages. |
| `ROUTER_COLLECTION_ROUTES` | 4 collections | The routing table. Adding a collection to an existing path is a config change; adding a new *path* is not. |
| `CONTENT_KEYWORDS` | common words | Deliberately common so the demo is not silent. |
| `ENGAGEMENT_THRESHOLD` | `25` / 60s | Likes+reposts on one post before "unusual traction". |
| `FOLLOW_THRESHOLD` | `10` / 60s | Follows of one account before "burst". Lower it to see the path fire. |

## The serverless target

`infra/cdk/` is the same topology as AWS managed services: Fargate tap →
EventBridge → SQS (+DLQ) → Lambda per path → DynamoDB. It **synthesises** and is
checked in CI:

```bash
cd infra/cdk && npx aws-cdk@2 synth
```

**It has not been deployed to a live account.** It exists so the claim that this
design is serverless-shaped is checkable rather than rhetorical. The same
handlers run on both targets — `WorkerContext` is handed a store interface, with
`RedisWindowStore` on Kubernetes and `DynamoWindowStore` on Lambda.

## Layout

```
router/routing.py       the classifier — pure, no I/O, the piece to read first
router/tap.py           WebSocket ingest, backpressure, cursor
router/workers/runner.py  consumer loop: ack, retry, DLQ, graceful drain
router/workers/*.py     one module per path
router/windows.py       bucketed sliding-window counters + the store interface
router/aws_handlers.py  Lambda entrypoints (SQS batch → same handlers)
k8s/                    namespace, NATS, Redis, config, tap, workers, KEDA
infra/cdk/              the AWS target
tests/                  routing, workers, and a fake Jetstream server
```

## Assumptions

- **The tap is a singleton.** `replicas: 1` is a correctness constraint, not a
  capacity choice — two taps would double-publish everything. Scaling out means
  partitioning by DID; described in DESIGN.md §9, not built.
- **At-least-once, not exactly-once.** The cursor rewinds a few seconds on
  resume, deliberately reprocessing rather than risking a gap.
- **NATS and Redis here are single-pod and ephemeral.** They make the topology
  runnable on a laptop; they are not a production recommendation, and the
  manifests say so.
- **Post text is matched, never emitted.** Alerts carry the DID, the record key
  and which keyword hit — not the content. This is a public, unfiltered
  firehose.
- **The AWS stack is synthesised, not deployed.**
