# Design

## 1. The problem

One inbound connection carries a heterogeneous, high-volume event stream. Every event must be
classified and dispatched to type-specific downstream work, and those downstream paths must run,
fail, and scale independently of one another.

Three properties of the source shape everything that follows:

**A single connection, with no ingest parallelism.** Jetstream is one WebSocket. Unlike Kafka there
is no consumer group to spread the read across replicas — exactly one goroutine reads the socket.
Anything that goroutine does inline is charged against the throughput of the entire stream.

**A severely skewed type distribution.** Measured against the live firehose, unfiltered:

| Route | Events | Share |
| --- | ---: | ---: |
| engagement (like, repost) | 139,509 | 74.8% |
| content (post) | 18,993 | 10.2% |
| social-graph (follow) | 10,456 | 5.6% |
| retraction (any delete) | 9,622 | 5.2% |
| default (identity, account, unrouted) | 7,912 | 4.2% |

186,492 events, one run, no server-side filter. A **13.3× spread** between the busiest and quietest
real route, sharing one ingest — any resource shared between them will be monopolised by the
former. Note also that **4% of the unfiltered stream is collections this service has no route
for**, which is why unknown types are counted rather than discarded.

**No exactly-once delivery.** Reconnects resume from a `time_us` cursor, which means duplicates on
every reconnect. Every downstream worker must tolerate them, and does — see D7.

One more detail matters for routing: an event's type is not a single field. It is the tuple
`(kind, collection, operation)`, and `operation = delete` cross-cuts collection, since a deletion
may be of a post, a like, or a follow. The routing table is therefore two-dimensional, not a flat
switch.

**The core question the design has to answer: how does one slow or failing consumer avoid affecting
any other?** Everything below is downstream of that.

## 2. Options considered

### Option A — Inline dispatch

Read the socket, switch on type, handle the event in the reader loop.

**Rejected: head-of-line blocking.** Any handler's latency becomes the stream's latency. A single
slow HTTP call stalls the socket, the cursor stops advancing, and the server's replay window
eventually laps us. This is the failure the exercise is really about.

### Option B — One shared queue and a generic worker pool

Read, enqueue, let a pool of identical workers drain it.

**Rejected: no per-type isolation.** This decouples ingest from processing, but a burst of likes
fills the queue and follows wait behind them. Paths cannot be scaled, retried, or failed
independently — which is the actual requirement. Worth saying plainly: under *uniform* load with
similar handlers this is the right answer and Option C is over-engineering.

### Option C — Per-type queue with a dedicated worker pool (chosen)

Read, classify, push to a bounded channel owned by that route, drain with a pool sized for that
route. Each route owns its own buffer, concurrency, retry policy, drop policy and metrics.
Isolation is structural rather than best-effort: a stalled route fills its own buffer and nothing
else's.

**What it costs.** Capacity is fragmented — idle workers on a quiet route cannot help a saturated
one — and N buffers cost N times the memory of one. There is also more configuration surface, and
therefore more ways to tune it wrong. This design trades utilisation for predictability. That is
the right trade when the load distribution is this skewed and behaviour under congestion is the
thing being judged.

**The honest limit of the claim.** Isolation is *between* routes, not *within* one, and all routes
share one process — one scheduler, one heap, one garbage collector, one fate. D6 is what stops that
shared fate from being fatal. A broker gives isolation this design can only approximate, which is
why it is the first item in the production path.

### Considered and deliberately out of scope

**Per-type queues with a shared, capped worker pool.** Admission isolation without fragmenting
capacity: each route keeps its own buffer, but workers come from one pool subject to a per-route
concurrency limit, so spare capacity can help a busy route. Rejected because the failure story
weakens — a leak or a wedged worker in the shared pool still reaches every route, and isolation
drops back to best-effort. Worth revisiting if utilisation ever matters more than containment.

**An external broker** (Kafka / NATS / Redis Streams) instead of in-process channels. The right
answer in production, the wrong cost for a 2–3 hour exercise on a local cluster. The dispatch
interface is deliberately narrow (`Offer(ctx, event) bool`) so this becomes a swap rather than a
rewrite.

**Managed fan-out** (EventBridge → SNS/SQS → Lambda). Would remove most of this code — along with
most of the reasoning being assessed. Discussed in §5 as the production alternative.

**Workflow orchestration** (Step Functions, durable functions). Wrong shape: this is single-hop
dispatch at very high volume, and orchestrators target low-volume, multi-step, long-lived
executions. The state that matters here — rolling counts — also spans events rather than living
inside one execution, so per-execution checkpointing does not help. The right place for an
orchestrator is behind a threshold alert, not in the hot path.

**Server-side filtering via `wantedCollections`.** Supported in config, off by default. Filtering
at the source is legitimate load shedding, but discarding the type mix removes the problem this
service exists to solve, and removes any ability to observe what was dropped — the 4% of unrouted
traffic measured above would simply become invisible.

## 3. The chosen design

| Component | Responsibility |
| --- | --- |
| `internal/jetstream` | Owns the WebSocket, the reconnect state machine and the cursor. Does no work beyond envelope decode and hand-off. |
| `internal/event` | Envelope types and lazy record decoding. |
| `internal/routing` | Derives the routing key and maps it to a route name via a config-driven table. Pure functions. |
| `internal/route` | A bounded channel plus a worker pool, its own config, its own metrics. |
| `internal/handler` | The per-type work. One file per route; each knows nothing about the others. |
| Sinks | Where output goes — structured logs here; a webhook or broker in production. |

```
                  Jetstream WebSocket  (one connection, one reader)
                              │
                    ┌─────────▼─────────┐
                    │  ingestor         │  reconnect FSM · cursor
                    │  envelope decode  │  record left as raw bytes
                    └─────────┬─────────┘
                              │  classify → route name   (pure)
     ┌──────────┬─────────────┼─────────────┬───────────┐
     ▼          ▼             ▼             ▼           ▼
 ┌────────┐ ┌──────────┐ ┌──────────┐ ┌───────────┐ ┌─────────┐
 │content │ │engagement│ │  social  │ │retraction │ │ default │  bounded channel
 │ 4 wrk  │ │  8 wrk   │ │  2 wrk   │ │   2 wrk   │ │  1 wrk  │  + worker pool
 │  drop  │ │   drop   │ │   drop   │ │   block   │ │  drop   │  + drop policy
 └────────┘ └──────────┘ └──────────┘ └───────────┘ └─────────┘
      each box is an isolation domain — nothing crosses it
```

### Key decisions

**D1 — The ingest goroutine never blocks.** Sends into route channels are non-blocking (`select`
with `default`). If a route's buffer is full the event is dropped and `events_dropped_total{route}`
increments. This looks like the wrong call — blocking would apply real backpressure — but on a
single shared reader, blocking one route stalls *every* route. That reintroduces head-of-line
blocking through the back door and lets the slowest consumer dictate the throughput of the whole
system. Explicit, counted shedding on the congested route is what preserves isolation.

Buffer size and full-buffer policy are per-route configuration. `block` exists for low-volume routes
where loss is unacceptable and congestion is implausible — the retraction route uses it — and even
`block` is bounded by a timeout, so one route can never hold the stream hostage indefinitely. §4
revisits this, because in a detection context dropping is not a neutral act.

**D2 — The cursor advances on enqueue, not on completion.** Once an event is in its route's buffer
it is that route's problem. Committing on handler completion would mean tracking N independent
positions and committing the minimum — meaningful complexity for little gain, given the pipeline is
at-least-once regardless. The cursor is persisted roughly once per second, and on reconnect we
rewind a few seconds to guarantee no gap.

Two consequences, stated rather than left to be discovered. A crash loses whatever is buffered in
memory, across every route — §5 fixes this first. And **a dropped event advances the cursor too**:
it was never enqueued, but the events around it were, and the cursor is a single monotonic
position. So shedding is **irreversible, not deferred**. The alternative is holding the cursor back
on the route that is already overloaded, which stalls the stream to protect the very events it
cannot keep up with. `events_dropped_total` is the only record that a dropped event ever existed.

**D3 — A key is mutated by one thing at a time.** Rolling counters are partitioned by
`hash(subjectKey) % N` across shards, and a worker takes that shard's lock for the read-modify-write.

The first implementation had no locks at all: each worker owned a queue, and events were hashed to
a worker *before* being enqueued. That is a nicer property, and it was wrong — the hash key for
these routes lives inside `commit.record`, so selecting the queue meant decoding the record body on
the single ingest goroutine. It violated the reader's decode budget, put the heaviest route's JSON
parse on the one goroutine whose stall halts everything, exposed that goroutine to
attacker-controlled nested content, and decoded every record twice. A reviewer caught it; a test now
pins it.

So the partition moved into the worker, which decodes on its own goroutine and then locks. The
guarantee is unchanged — one mutator per key at a time — and the cost is a mutex. That is the right
way round: an uncontended lock is nanoseconds, while work on the shared reader is charged against
the throughput of the entire stream.

The remaining trade-off is hot keys. A viral post serialises behind one shard's lock, and because
the buffer is per-route, sustained pressure on one key can push the whole route into shedding. The
production answer is an external store with atomic increments, which removes the partitioning
problem entirely.

Windows are evaluated on **event time** (`time_us`), not wall clock. After a reconnect the service
replays events that are already minutes old; a wall-clock window would read that replay as a burst
of simultaneous activity and fire a false alert on the exact path that recovery exercises most.

State is bounded like the buffers are: counter, window and dedup maps are capped by size and TTL.
An unbounded map keyed on an attacker-supplied subject is a memory-exhaustion vector, and capping
it is the same principle already applied to the channels — see §4. The TTL sweep runs on a fixed
event count inside the shard that owns the map, not on a timer: a timer goroutine would contend for
the same lock and buy nothing. (A review found the sweep defined but never called, which left only
the size cap live; there is now a test that fails if it stops running.)

**D4 — Unknown types are routed, not silently discarded.** An unmatched key goes to the default
route, which counts it and logs a bounded bucket. New lexicons appear on the network continuously —
4% of measured traffic — and discovering them should be an observation, not an outage.

**D5 — The connection lifecycle is an explicit state machine.** `Disconnected → Connecting →
Replaying → Live`, plus `Reconnecting` with exponential backoff and full jitter. Keeping cursor
handling, backoff and replay detection in one place is considerably clearer than scattering
booleans through the read loop.

Liveness deliberately does not fail while disconnected — otherwise Kubernetes restarts the pod at
precisely the moment the backoff is working correctly. Readiness does, and also fails on excessive
cursor lag: a process holding a healthy socket while two minutes behind is not working, and a probe
that only checked the socket would report it as fine.

If the stored cursor is older than the server's replay window, the resulting gap is unrecoverable
**and of unknown size** — Jetstream has no sequence numbers, so nothing tells you how much you
missed. It is recorded, alerted, and the service resumes from the live tip rather than silently
pretending it did not happen.

**D6 — Failure is contained per event, not per process.** Every worker runs its handler inside a
`defer recover()`. In Go an unrecovered panic in any goroutine terminates the entire process, so a
single malformed record in one route's handler would take down every other route — falsifying the
isolation claim this design rests on. Recovery logs, increments `handler_panics_total{route}`, and
continues with the next event.

Errors are classified before they are retried. Transient failures (a webhook timing out) retry
inside the route with bounded exponential backoff; permanent failures (an event that cannot be
parsed) go straight to the dead-letter path with the reason recorded. Retries occupy a worker,
never the ingest goroutine, and the route's own buffer absorbs the slack. Decode failures at ingest
are counted and discarded, never fatal.

Shutdown drains in order: the ingestor stops reading first, routes drain against a deadline, and
the cursor is committed last — so a restart replays a bounded overlap rather than opening a gap.

**D7 — Idempotency is a requirement, not a nicety.** D2 plus the reconnect rewind guarantees
duplicates even in single-process operation, before any scaling is involved. Every handler keys on
`did + collection + rkey + operation`, so a repeated event does not double-count a like or fire the
same alert twice. `rkey` is unique only within its collection, which is why the collection is part
of the identity. The seen-key set is itself bounded by time and size; beyond that window a
duplicate is accepted and a counter is marginally wrong — a deliberate trade of exactness for
bounded memory, and the production answer is the same external store that holds the counters.

### Routes implemented

| Routing key | Route | Work | State |
| --- | --- | --- | --- |
| `commit / * / delete` | `retraction` | Cleanup path, cross-cuts all collections | none |
| `commit / app.bsky.feed.post / create` | `content` | Keyword and language matching, alert on hit | none |
| `commit / app.bsky.feed.{like,repost} / create` | `engagement` | Rolling per-subject counts, alert on threshold | bounded counters |
| `commit / app.bsky.graph.follow / create` | `social-graph` | Per-target sliding window, alert on follow burst | bounded window |
| anything else | `default` | Count, log a bounded bucket | none |

Precedence is **operation-first**, and it is a property of the router rather than of ConfigMap
ordering: rules that pin an operation are evaluated before rules that only name a collection. A
delete commit carries no record, so a create-path handler would have nothing to match against even
if it received one.

## 4. Security considerations

Every field on this stream is attacker-controlled: anyone can publish anything to the network, and
the service has no way to distinguish ordinary traffic from traffic shaped deliberately against it.
That reframes several of the decisions above.

**Load shedding is an evasion primitive.** In a detection context a dropped event is a missed
detection, and per D2 it is gone for good — an adversary able to generate load can fill a buffer on
purpose and hide activity inside the discarded window. D1 stands for this prototype, but it is not
a neutral default: in production the answer is a durable broker so shedding is never required, and
where it is unavoidable the low-value route sheds while a detection route never does. Every drop
should raise an alert, not only a counter.

**Bounded queues, bounded state.** Capping the channels while leaving the aggregation maps
unbounded would simply move the exhaustion target: unlimited unique subject keys is a
straightforward memory-exhaustion vector. State is capped by size and TTL for the same reason the
buffers are.

**Record text never reaches a log line.** Post text is attacker-controlled, so writing it to a
structured log hands an adversary control of the log an aggregator or SIEM later ingests — forged
entries, broken parsers, injected fields. Only structural and derived values are logged: length,
language, matched or not.

**Metric labels are a fixed set.** Labelling by route is safe because routes are enumerable.
Labelling by collection would let anyone publishing a novel lexicon grow the metric space without
limit and exhaust the metrics backend; unknown collections are counted under one bucket.

**Parser limits at the edge.** Maximum frame size is enforced on the connection, and record bodies
are parsed inside the owning worker rather than on the shared reader.

**Alert paths are rate-limited.** Thresholds can be crossed deliberately, so an unthrottled alert
path is a remotely triggerable flood against whatever sits downstream of it. The same reasoning
applies to log lines keyed on attacker-supplied values — a bug I shipped and then caught by
watching it run against the live stream, now covered by a test.

**Data minimisation.** DIDs are personal identifiers and record text may contain anything; nothing
is retained beyond what a handler needs for its own decision, and nothing is persisted by default.

**Runtime posture.** Distroless image, non-root, read-only root filesystem, all capabilities
dropped, explicit resource limits, and a NetworkPolicy restricting egress to DNS and outbound TLS.
The service needs no credentials of any kind, and that property is worth keeping.

## 5. From prototype to production

Two things change materially, not just mechanically.

**Durability moves the cursor commit point.** Once the enqueue target is a broker rather than a
channel, the cursor can commit on broker acknowledgement, and a crash stops losing buffered events.
This is the single most valuable upgrade, and the one that also removes the pressure to shed under
load.

**Ingest scaling requires sharding, not replicas.** The connection remains a singleton — running
two readers over the same range means processing everything twice. To scale the front, open N
connections partitioned by `wantedCollections` or `wantedDids`, each a separate shard, and each a
strict singleton: a StatefulSet with one replica per shard or a lease-based leader election, and a
deployment strategy that tears down before it brings up. The default rolling update briefly runs
two pods, which means duplicate processing — tolerable only because of D7.

| Concern | This prototype | Production |
| --- | --- | --- |
| Transport between stages | In-process bounded channels | Kafka / NATS / SQS, one topic per route |
| Workers | Goroutine pools | Separate deployments per route |
| Aggregation state | Bounded in-memory maps | Redis or DynamoDB, atomic increments and TTLs |
| Cursor | Local file, committed on enqueue | Redis or DynamoDB, committed on broker ack |
| Failed events | Counted and logged | Per-route DLQ plus a replay tool |
| Observability | Prometheus counters, structured logs | Alerting on drop rate, queue depth and cursor lag |

**AWS-native alternative.** A Fargate task holding the connection, publishing to EventBridge, with
an SNS/SQS pair and a Lambda per event type. Routing becomes configuration instead of code, and
backpressure, retries and DLQs come off the shelf. The cost is vendor lock-in and much less control
over batching and shedding behaviour — for a service whose entire value is how it handles
congestion, that control is worth keeping until scale justifies giving it up.

## 6. Delivery plan

Built in the order that keeps a working pipeline at every step: skeleton with config, structured
logging and graceful shutdown; then ingestor, classifier, router and a single route wired end to
end; then the remaining routes with bounded buffers, panic recovery, drop accounting and metrics;
then Dockerfile, manifests and a kind deployment; then the write-ups. The exit criterion for the
pipeline is that `kubectl logs` shows every route processing and drop counters moving under load.

Built to the boundary of the timebox and no further. **Deliberately described rather than built:**
the broker swap, external state, per-route dead-letter queues that outlive the process, sharded
multi-connection ingest, and replay tooling. The dispatch interface is the seam each of those
attaches to, and it is narrow on purpose.

## 7. Test plan

A small number of tests aimed at the logic that carries the design, rather than broad coverage.

| Test | Why it matters |
| --- | --- |
| **Router table** — fixture events to expected route | The core of the exercise. Covers each collection, a delete of each type (the cross-cutting case), an `identity` event with no commit block, and an unknown collection falling through to default. |
| **Isolation under congestion** — saturate one route's buffer, assert its drop counter rises while another route continues to drain | The test that actually proves the architecture. If this passes, the isolation claim is not just a diagram. |
| **Panic containment** — a handler that panics on a crafted event; assert the process survives and other routes keep draining | The other half of the isolation claim. Without recovery, one bad record ends every route at once. |
| **Threshold and window logic** — counters fire once at the boundary, the window forgets old entries, and it is evaluated on event time | The stateful logic, where off-by-one errors hide — and where replayed events would otherwise produce a false burst. |
| **Cursor and idempotency** — cursor survives a simulated reconnect without a gap, and the replayed overlap does not double-count | Protects the resume guarantee and the correctness that depends on it. |
| **Integration** — a fake WebSocket server replays fixtures through the full pipeline | End-to-end wiring, without depending on the live network. |

Deliberately not covered: the live Jetstream connection (non-deterministic, and a network test
rather than a logic test), and load testing (worth doing, out of timebox). Manual verification is a
kind deploy with logs tailed for a few minutes against the real firehose — which is how the
measurements in §1 and two of the bugs noted in `AI.md` were found.

## 8. Known gaps

Things I know are missing, rather than things I hope nobody notices.

- **Isolation is between routes, not within one.** One process, one heap, one GC. D6 contains the
  worst case; a broker is the real fix.
- **A hot key degrades its whole route,** because the buffer is per-route and not per-shard, and
  because that key's updates serialise on one shard lock.
- **D3 costs a mutex.** Lock-free single-goroutine ownership would be nicer, but achieving it
  required decoding the record on the reader — a worse trade. Stated here rather than buried.
- **Nothing handles an event type needing state from two routes at once.** Pure fan-out has no
  answer for it: either that state moves to an external store both routes read, weakening the
  no-shared-resource property, or an explicit join stage is added downstream of both.
- **The dead-letter path does not outlive the process.** Counted and logged, not queued.
- **Aggregation state does not survive a restart** — counters reset, so a threshold that was about
  to fire will not.
- **No load test.** The service keeps up with the live firehose comfortably; I have not driven it
  to the point where the shed policy engages other than by shrinking a buffer deliberately.
