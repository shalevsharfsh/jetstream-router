# Design

> A formatted version of this document — with architecture diagrams, the traffic
> breakdown and the delivery plan as a timeline — is published at
> **https://claude.ai/code/artifact/eb72e909-c456-4898-8975-5f01ce4e972c**
> Same content, same section numbers; this file is the canonical copy.

## The problem, restated

One high-volume stream carrying many kinds of events; work out what each event
is; route it to the right downstream work; make those paths run, fail and scale
independently.

The last clause is the whole exercise. Classifying a JSON object is not hard.
Making several workers genuinely not interfere with each other is where the
design decisions are, so that is what I optimised for and what the rest of this
document is about.

**Section map** — §1 problem · §2 requirements · §3 measured traffic ·
§4 high-level design · §5 options and tradeoffs · §6 key decisions ·
§7 failure modes · §8 delivery plan · §9 risks and known gaps · §10 code map.

---

## 1. Problem

See above. The source is the Bluesky Jetstream firehose: a public,
unauthenticated WebSocket carrying posts, likes, reposts, follows, blocks,
profile and account changes, all interleaved on one connection.

---

## 2. Requirements

**Functional.** Hold one durable connection and survive disconnects without
losing position. Classify every event by `kind`, `collection` and `operation`.
Dispatch to at least four behaviourally distinct paths — content matching,
engagement aggregation, graph burst detection, and a separate retraction path.
Never silently discard an unrecognised event type.

**Non-functional — the ones that shaped the design.**

| Requirement | Target | How it is met |
|---|---|---|
| Path isolation | One path failing affects no other | Separate process, consumer, retry budget and DLQ per path |
| Bounded memory | No unbounded growth under overload | Fixed-size ingest queue with explicit shed policy; TTL'd window buckets |
| Independent scaling | Per-path capacity | KEDA on consumer lag, per path, non-uniform policies |
| Observability | Answer "are we keeping up?" directly | `cursor_lag_seconds`, gating readiness |
| Resumability | No gap across a restart | `time_us` cursor, checkpointed, rewound on resume |
| Local reproducibility | One command, no cloud account | `make deploy` onto kind |

**Explicit non-goals.** No UI, no auth, no multi-tenancy, no exactly-once, no
multi-region. Excluded so the paths that exist are complete rather than numerous.

---

## 3. Traffic, measured

I sampled the live firehose before writing the classifier rather than coding
from the docs. Three findings changed the design:

1. **Deletes carry no `record`** — only `collection` and `rkey`. So referential
   cleanup is *impossible* without an index the create path writes
   (`(did, collection, rkey) -> subject_uri`). At firehose volume that is a
   write per engagement event, purely to make deletes resolvable. Whether that
   is worth paying depends on whether counts must be exact; for a signal, no. So
   the retraction path records the retraction (which is genuinely useful — "was
   this deleted?" is what compliance needs) and exposes the gap as
   `jsr_retractions_total{resolution="no-index"}` rather than pretending the
   cleanup happened.
2. **The volume skew is large and stable.** Measured over 440,108 classified
   events on this cluster: engagement 79.3%, content 12.1%, graph 5.6%,
   retraction 2.9%, other 0.2% — a **14.2× spread** between the busiest and
   quietest real path. That is the empirical case for per-path scaling; a
   uniform worker pool would be simultaneously starved and wasteful.

   (A note on measuring this honestly: my first figure was 106×, taken from
   per-worker *handled* counters. It was wrong — the graph worker had been
   scaled to zero for part of that window, so I had measured worker uptime, not
   stream composition. The classifier's counters see every event regardless of
   worker state, which is why they are the right source. Worth knowing which of
   your metrics answers the question you are actually asking.)
3. **Naive keyword matching is unusable.** With `ai` in the keyword list,
   `"ai" in text` fires on *said*, *again*, *email*, *Dubai*. Against live data
   that was nearly every match. The fix is a word boundary — but expressed as
   `(?<!\w)…(?!\w)`, not `\b`: `\b` is a *transition*, so `\bc\+\+\b` matches
   nothing at all, since the character after `+` would have to be a word
   character. My own test for `c++` caught that. A notification path that cries
   wolf is worse than no notification path.

**On content handling.** This is a public, unfiltered firehose. Post text is
matched against, never emitted: alerts carry the DID, record key and which
keyword hit, plus text length — not the text. Logs are the easiest place to
accidentally build a permanent, widely-readable copy of exactly the content you
were told to be careful with. Similarly, `collection` is user-extensible in the
AT Protocol, so it is bounded to a known set before being used as a metric
label — otherwise a stranger can mint unbounded Prometheus series from the
public internet and take out your monitoring during the incident you needed it
for.

---

## 4. High-level design — and the central decision

The obvious implementation is one process: a goroutine (or task) per handler, a
channel per event type, a `select` in the middle. It is less code, it has no
broker to operate, and for a prototype it would work.

I did not do that, because in that design **"independent" is a convention**.
One process, one heap, one scheduler. A panic in the content handler takes down
the aggregation path. A slow handler backs up a shared channel and applies
pressure to paths that have nothing to do with it. A memory leak in one is a
restart for all five. You get isolation only for as long as everyone is careful.

Putting a broker at the fan-out boundary makes isolation **structural** instead:

| Property | Shared process | Broker boundary |
|---|---|---|
| One path crashes | all paths die | one consumer stops; others unaffected |
| One path is slow | shared channel backs up | that consumer's `num_pending` grows; nothing else changes |
| Retry / poison messages | hand-rolled per handler | `max_deliver` + DLQ per consumer |
| Backpressure signal | a channel depth I have to instrument | queue depth, already the thing operators look at |
| Scaling | one process, one scaling unit | per-path replica counts and policies |
| Deploy | all paths at once | one path at a time |

The cost is real and I want to name it before defending it: an extra hop of
latency, a broker to run and reason about, serialisation on both sides, and the
loss of in-process state (aggregations move to Redis). For a service whose
entire stated purpose is path independence, that is the right trade. If the
requirement had been "lowest possible latency for one kind of event", it would
not have been.

**Verification, not assertion.** `make chaos` deletes the busiest worker. With
engagement (~79% of traffic) down for ~30 seconds, content processed 1,067 more
events and retraction 258, uninterrupted; engagement's consumer drained back to
zero pending on restart. That is the property, demonstrated rather than claimed.

### On the stack deviation

The brief specified Go + Kubernetes. This is Python + Kubernetes. I should be
straight about the reasoning rather than bury it.

The honest ranking of my reasons: I am faster and better in Python, and the
design decisions here — routing, backpressure, isolation, delivery semantics —
are language-independent, so spending the time budget on the design rather than
on fighting an unfamiliar language produced a better artifact. Python's asyncio
handles a single WebSocket and a batch publisher perfectly well at ~300
events/sec; nothing in this workload is CPU-bound.

What I gave up is worth stating plainly:

- **Go is the better tool for this specific problem.** Goroutines and channels
  are a more natural expression of per-subscriber fan-out than asyncio tasks,
  `context` cancellation is cleaner than my `asyncio.Event` plumbing, and the
  reference implementation — Jetstream itself — is written in Go for good
  reasons. At 10× this volume, the GIL and per-message Python overhead become
  the constraint, and I would port the tap first.
- **A single-binary Go service would need no broker at this scale**, so the
  isolation argument above is partly me buying my way out of a problem Go
  solves more cheaply in-process. I still think the broker is right for the
  stated requirement, but the trade is less lopsided in Go.

If the intent of specifying Go was to see whether I would pick up an unfamiliar
language, that is a fair thing to have tested and I did not do it. I would take
that on directly rather than pretend the choice was free.

---

---
### Routing

`router/routing.py` is a pure function: event in, destination out, no I/O and no
clock. That purity is deliberate — it is what makes the routing table
exhaustively testable without standing up a broker, and it is why the tests
there are the ones I would keep if I could only keep one file.

Precedence is load-bearing:

1. Non-commit kinds (`identity`, `account`) have no collection, so they can
   never match the table.
2. **`operation = delete` beats collection.** A deleted post goes to retraction,
   not content. This is stated in the brief, and it is also forced by the data:
   a delete carries no `record`, so the content worker would have nothing to
   match against. Getting this backwards is the easiest way to break the
   routing, so it is parametrised across the whole table in tests rather than
   spot-checked.
3. Then the collection table.
4. Anything left is `OTHER` — **counted, never dropped**.

`OTHER` earns its place. Two different reasons land there and they are labelled
differently: `non-commit-kind` (expected forever) and `unmapped-collection` (the
signal that Bluesky shipped a lexicon we have no path for). A rising
`unmapped-collection` rate is how a new path gets justified. Dropping these at
the tap would have been less code and would have made schema drift invisible.

`classify()` returns `None` — distinct from `OTHER` — for structurally unusable
events, counted as `jsr_events_malformed_total`. Conflating "we don't handle
this" with "we can't parse this" would hide upstream schema changes inside a
metric that is expected to be non-zero anyway.

### Where routing lives — and the honest answer to "add a type without redeploying"

On Kubernetes the tap classifies and publishes to `bsky.<destination>`. The
alternative is to publish raw and let the broker route (EventBridge rules, SNS
filter policies, NATS subject wildcards).

I implemented both, because the two targets genuinely differ:

| | Tap-side (this repo, k8s) | Broker-side (`infra/cdk`, AWS) |
|---|---|---|
| Routing lives in | one pure, unit-tested function | declarative EventBridge rules |
| New consumer for an existing type | redeploy the tap | none — subscribe a rule |
| Testability | trivial | needs deployed infrastructure |
| Where a bug hides | one file | spread across IaC |

So the honest answer to "can you add a new event type without redeploying" is
**partly**. Adding a *collection* to an existing path is a ConfigMap edit
(`ROUTER_COLLECTION_ROUTES`) plus a restart. Adding a genuinely new *path* is
not, and should not be: a new destination implies a new worker, a new consumer,
a new DLQ and a new scaling policy. Pretending that is configuration would be
the wrong kind of clever. On the AWS target, the first case needs no ingest
change at all.

---

## 5. Options and tradeoffs

Four architectures were genuinely on the table for the fan-out boundary. The
decision is not "which is best" but "which property am I buying, and what am I
paying for it".

### A — Single process, tasks and channels *(rejected)*

One binary; a task per handler, a channel per event type, a dispatcher between.

- **For:** least code, lowest latency, nothing to operate, in-process state.
- **Against:** isolation is a *convention* — one panic, one leak, one slow
  handler and every path is affected. One scaling unit, one deploy blast radius.
- **Why rejected:** it fails the one requirement the brief emphasises. Worth
  saying plainly, though: in Go this option is considerably stronger than in
  Python, and the gap between A and B narrows.

### B — Broker at the fan-out boundary *(selected)*

The tap classifies and publishes to a per-destination subject; each path is a
separate deployment with its own durable consumer.

- **For:** isolation is *structural*. Retry, DLQ and backpressure come from the
  broker rather than being hand-rolled per handler. Per-path scaling and deploys.
- **Against:** a broker to run, serialisation on both sides, one more hop, and
  state must leave the process.
- **Why selected:** it is the only option where "independent" survives someone
  being careless.

### C — One WebSocket per event type *(rejected)*

Skip the broker; give each worker its own connection with its own
`wantedCollections` filter. Jetstream supports this directly.

- **For:** total isolation, no broker at all, each path scales its own ingest.
  Genuinely tempting.
- **Against:** N connections to a public service we do not own, with rate limits
  that are theirs to change. N independent cursors and no shared view of the
  stream. And deletes cannot be filtered server-side, so the retraction path
  must receive everything regardless.
- **Why rejected:** it pushes our fan-out problem onto someone else's
  infrastructure, and it does not generalise — the moment the source is Kafka or
  a paid API rather than a free public socket, the design collapses.

### D — Stream processor, Flink or Spark Structured Streaming *(rejected for now)*

Land the raw stream in Kafka; express routing and aggregation as a streaming job.

- **For:** proper event-time windows, watermarks, managed state, checkpointing.
  This is where the design ends up if the aggregations get genuinely complex.
- **Against:** heavyweight for a 2–3 hour exercise, a cluster to operate, and
  isolation is per-job — coarser than the per-path independence asked for.
- **Why rejected for now:** the aggregations here are simple commutative
  counters. This becomes the right answer when correlation *across* events —
  "this sequence of individually legitimate actions is suspicious" — is the
  requirement.

### The decision in one table

| Property | A single process | **B broker** | C N sockets | D stream processor |
|---|---|---|---|---|
| One path crashes | all die | **one consumer stops** | one path stops | job restarts |
| One path is slow | shared channel backs up | **its lag grows only** | isolated | backpressure per operator |
| Poison message | hand-rolled | **`max_deliver` + DLQ** | hand-rolled | side output |
| Scaling unit | whole process | **per path** | per path | per operator |
| Operational cost | lowest | **moderate** | low | highest |
| Added latency | none | **one hop** | none | one hop + checkpoint |
| Correlation across events | awkward | **awkward** | impossible | native |

---

## 6. Key decisions
### D1–D3 · Ingest: the one thing that cannot be elastic

Everything downstream of the tap is stateless and disposable. The tap is not,
and finding that boundary is the interesting part of the design.

A long-lived WebSocket holding a resume cursor has no home in a function
execution model — there is no 15-minute-bounded, connection-affinity-free way to
own a socket. So: exactly one always-on process, and everything after it
elastic. On AWS that is a single Fargate task, not a Lambda, and
`infra/cdk/app.py` says so explicitly. Any design that claims to be
"fully serverless" here is either wrong or hiding the tap.

`replicas: 1` is therefore a **correctness** constraint. Two taps would each
hold a socket, receive the same events and publish everything twice, silently
doubling every aggregate in the system. The Deployment uses `strategy: Recreate`
for the same reason — a rolling update would briefly run two.

### Backpressure is a policy, not an emergent behaviour

Reader and publisher are decoupled by a bounded queue. When the broker is slower
than the firehose, something must give, and I made the choice explicit and
configurable rather than accidental:

- **`shed`** — drop the oldest queued event, keep reading. Latency stays
  bounded; completeness does not. Every drop increments
  `jsr_events_shed_total`, labelled by destination. Never silent.
- **`block`** — stop reading the socket. TCP backpressure propagates upstream,
  nothing is lost, but we fall behind and may be disconnected — at which point
  the cursor brings us back without a gap.

Neither is universally right. `shed` suits statistical paths (a traction
estimate does not need every like); `block` suits paths where a missing event is
a correctness bug. The current default is `shed` because three of the four paths
are aggregate. That this is one global setting rather than per-destination is a
real limitation — see §9.

The cheapest backpressure is not receiving the event at all, so
`wantedCollections` is derived *from the routing table* rather than configured
alongside it. The filter cannot drift out of sync with what we can actually
route, and we never pay bandwidth or parse cost for events we would discard.

### Cursor

The cursor advances on the `time_us` of the last **published** event, not the
last read one — advancing on read would let a crash between read and publish
punch a hole in the stream. It is checkpointed to Redis every 2s, and on resume
we rewind 5 seconds. We deliberately reprocess a small window rather than risk a
gap.

Two known properties of Jetstream shape this: `time_us` is instance-local, so
it is not a global identity, and there are no sequence numbers, so **a gap is
undetectable** — you cannot tell whether you missed anything. That asymmetry is
exactly why the resume is biased toward duplication.

`jsr_tap_cursor_lag_seconds` — wall clock minus the last published event's
timestamp — is the single most useful number in the service, and it gates
readiness. A tap holding a healthy socket while ninety seconds behind is not
working, and a readiness probe that only checked socket state would report it as
fine. (Liveness and readiness are deliberately different: losing the upstream
makes the tap un-ready but not dead, and restarting it would only throw away the
reconnect backoff it has earned.)

---

### D4–D6 · Isolation and scaling, concretely

Each destination gets its own durable pull consumer with its own
`filter_subject`, `max_deliver`, ack window and DLQ subject. Failure handling
lives once in `workers/runner.py` rather than four times with three subtly
different bugs:

- Handler raises, under budget → `nak` with exponential delay (a struggling
  dependency gets a breather, not a redelivery storm).
- Handler raises, budget exhausted → publish to `bsky.dlq.<path>`, then ack.
  **Publish before ack**: crashing between them duplicates into the DLQ, which
  is recoverable; acking first loses the message.
- Unparseable payload → straight to DLQ. It will never parse on retry, so
  burning five redeliveries on it is pure waste.
- `SIGTERM` → finish the in-flight batch, then exit. Every rolling deploy and
  every scale-down sends one; without the drain those become bursts of
  redelivery.

Handlers themselves never touch NATS. They receive a decoded event and a store
interface, which is what makes them testable with `fakeredis` and no
infrastructure — and what lets the identical handler code run on Lambda against
DynamoDB.

### Scaling policies are deliberately not uniform

| Path | Policy | Why |
|---|---|---|
| content | fixed 1 | notification latency is user-visible; a cold start is felt |
| engagement | KEDA 1→6 on lag | ~79% of traffic, bursty |
| graph | KEDA 1→4 on lag | low volume, but see below |
| retraction | fixed 1 | correctness/compliance-sensitive, low volume |
| other | fixed 1 | observability only, never the bottleneck |

Scaling on **queue depth, not CPU**: these workers are I/O-bound on Redis, so
they can be badly behind while barely registering CPU. Consumer lag is what a
human would look at, so it is what the autoscaler looks at. This is the same
signal Lambda's SQS event source uses; KEDA is how you get it on Kubernetes.

### A measured mistake worth keeping

I originally set `graph` to `minReplicaCount: 0` — the headline scale-to-zero
case, since it is the lowest-volume path. Then I watched it:

```
19:47:07  replicas 1 -> 0     (drained, cooldown elapsed)
19:49:54  replicas 0 -> 1     (backlog reappeared)
19:51:32  replicas 1 -> 0
19:51:42  replicas 0 -> 1
```

A ~2-minute oscillation, indefinitely. The error was treating *low volume* as
*idle*. On a live firehose the graph path is never idle; it is slow — a steady
~10 events/sec. Scale-to-zero fits work that genuinely stops.

And it was not merely wasteful, it was **incorrect**: this worker detects bursts
over a 60-second window, and the scale-down/up cycle is longer than that window.
A path configured that way can be asleep for exactly the interval it is meant to
be watching. I set a floor of 1 and kept KEDA for scaling *out* on backlog,
which is what it is actually good for here. Genuine scale-to-zero belongs on
intermittent work — DLQ replay, backfill, nightly reconciliation — not on a
continuous stream.

---

### D7 · Delivery semantics

**At-least-once, no ordering guarantee across a destination.** Stated plainly
because the alternatives are frequently claimed and rarely true.

- Duplicates arise from cursor rewind on resume, from DLQ publish-then-ack, and
  from NATS redelivery. Handlers tolerate this to the degree the domain needs:
  the alert claim (`SET NX` / conditional write) is atomic, so a duplicate
  cannot produce a duplicate alert. Counters are *not* idempotent — a replayed
  like double-counts. For a traction *signal* that is acceptable; if exact
  counts were required, deduplication on `did+collection+rkey+rev` (never on
  `time_us`, which is instance-local) with a TTL set would be the fix, and that
  set becomes a new thing to size and evict.
- Ordering: messages within a batch are processed concurrently, and multiple
  replicas consume the same subject. Per-actor ordering would require
  partitioning by DID across replicas — a hash-partitioned consumer group. I did
  not build it because none of the four handlers are order-sensitive: they are
  all commutative counters or independent matches. If a path needed
  "follow then unfollow" ordering, this design would be wrong for it and I would
  partition rather than serialise.
- Exactly-once is not offered. It would require deduplication with a durable,
  bounded identity store on every path, and the honest position is that
  at-least-once plus idempotent-where-it-matters is cheaper and sufficient here.

---

## 7. Failure modes

| Failure | Detected by | Behaviour | Blast radius |
|---|---|---|---|
| Upstream disconnect | `tap_connected` = 0 | Exponential backoff with jitter; resume from cursor | Ingest pauses; nothing lost |
| Tap falls behind | `cursor_lag_seconds` | Readiness fails at 60s; shed policy bounds the queue | Freshness only |
| Tap pod dies | Liveness / restart | New pod resumes from checkpoint minus 5s | Small replay |
| One worker crashes | Consumer `num_pending` grows | Pod restarts, backlog drains, KEDA scales out | **That path only** |
| Handler throws | `messages_handled{outcome}` | Nak with exponential delay, up to the redelivery budget | One message |
| Poison message | `dlq_messages_total` | Published to that path's DLQ, then acked | One message |
| Unparseable payload | Same | Straight to DLQ — retrying will never help | One message |
| Broker unavailable | Publish errors | Cursor does not advance past failures; ingest degrades rather than crashing | Whole pipeline, recoverable |
| Redis unavailable | Handler exceptions | Nak and retry; windows rebuild within one window | Stateful paths |
| Unknown event type | `unmapped-collection` rate | Routed to `other` and counted | None — this is the signal to build a path |

**Ordering matters in the DLQ path.** A message that exhausts its redelivery
budget is published to the DLQ subject and *then* acked off the main consumer.
Crashing between those two steps duplicates into the DLQ, which is recoverable;
acking first would lose it outright. This is the kind of detail that is
invisible until the day it matters, which is why it lives in one shared runner
rather than being reimplemented per handler.

**Verified, not asserted.** `make chaos` deletes the busiest worker. With
engagement (79% of traffic) down for ~30 seconds, content processed 1,067 more
events and retraction 258, uninterrupted; engagement's consumer drained back to
zero pending on restart.

---

## 8. Delivery plan

Phases 0–1 are complete and running. The rest is ordered by risk retired per
unit of effort, not by how interesting the work is.

| Phase | Work | Exit criteria |
|---|---|---|
| **0 · done** | Classifier, tap, four paths, broker, kind manifests, tests | One command deploys; all paths alert on live data |
| **1 · done** | Per-path consumers, DLQs, KEDA on lag, chaos test, metrics, lag-gated readiness | Killing the busiest worker measurably does not affect the others |
| **2** | **Close the DLQ hole** — replay worker + alert on DLQ depth. A dead-letter queue nobody reads is a data-loss mechanism with extra steps, and replay is genuinely intermittent work, so it is the correct home for scale-to-zero | A poisoned message can be fixed and replayed without a deploy |
| **3** | Real infrastructure — clustered broker with PVs (or Kafka, where partitions map onto per-actor sharding better); managed Redis. Not a code change of consequence, which was the point of the store interface | No single pod loses state; broker survives a node failure |
| **4** | SLOs on the numbers that already exist — alert on cursor lag, shed rate, per-consumer pending, DLQ depth | Every failure mode in §7 has a corresponding alert |
| **5** | Per-destination backpressure — split the tap's queue so retraction can `block` while engagement `shed`s | The correctness-sensitive path is lossless under overload |
| **6** | Shard the tap by DID with disjoint server-side filters + a coordinator | Ingest throughput scales horizontally |
| **7** | Deduplication on `did+collection+rkey+rev`, if exactness is ever required. Last deliberately: it adds a store to size and evict | Counters are replay-safe |

---

## 9. Risks and known gaps

Things I know are wrong or missing, rather than things I hope nobody notices:

- **Backpressure policy is global**, not per-destination. The right shape is
  `shed` on aggregate paths and `block` on retraction. That needs per-destination
  queues in the tap, which is maybe thirty lines and one more config dimension.
- **The tap is a scaling ceiling.** One socket, one process. Jetstream's
  `wantedDids` allows sharding by actor across N taps with disjoint filters;
  that is the horizontal scaling story and it is unbuilt.
- **No deduplication**, so counters over-count on replay (§5).
- **Nothing consumes the DLQ.** Messages land there and stay. A replay worker —
  genuinely intermittent work, and therefore the correct home for scale-to-zero —
  is the obvious next piece.
- **Redis and NATS are single-pod and ephemeral.** Losing Redis loses the cursor
  and the windows; the tap restarts from live and counters rebuild in one
  window. Acceptable for a demo, not for production.
- **`identity` and `account` events are counted, not handled.** An account
  deletion arguably should trigger a real retention path.
- **Alerts are log lines.** `WorkerContext.alert()` is deliberately the single
  seam where a webhook or SNS publish goes.
- **No load test.** I know the service keeps up with the live firehose at ~300
  events/sec with lag under 0.1s and zero shed events; I have not driven it to
  the point where the shed policy engages, so that path is tested by unit test
  rather than in anger.

---

## 10. Code map — read in this order

| File | What to look at | Decision |
|---|---|---|
| `router/routing.py` | `classify()` — the precedence chain, and `None` vs `OTHER` | §4 |
| `tests/test_routing.py` | Delete precedence, parametrised across the whole table | §4 |
| `router/tap.py` | `_enqueue()` shed policy; `load_cursor()`; the reconnect loop | §6 D1–D3 |
| `tests/test_tap.py` | The fake Jetstream server — reconnect and resume, made deterministic with an explicit gate | §6 D3 |
| `router/workers/runner.py` | `_process()` ack/nak/DLQ; `_to_dlq()` ordering; SIGTERM drain | §7 |
| `router/windows.py` | Bucketed counters and the store interface both targets implement | §6 D5 |
| `router/workers/retraction.py` | The missing-index limitation, surfaced as a metric | §3, §9 |
| `router/workers/content.py` | `compile_keywords()` — why the boundary is a lookaround, not `\b` | §3 |
| `k8s/50-keda.yaml` | The scale-to-zero correction, with the KEDA logs kept in the comment | §6 D6 |
| `infra/cdk/app.py` | Fargate tap, per-path reserved concurrency, batch windows | §6 D1, D4 |

---

## 11. What I deliberately did not build

Multi-region, exactly-once, a UI, authentication, a schema registry, per-tenant
isolation, and a Grafana dashboard. Four paths wired end-to-end with the failure
semantics thought through says more about how I build than eight paths with the
interesting parts stubbed. Knowing where to stop was, per the brief, part of the
exercise.
