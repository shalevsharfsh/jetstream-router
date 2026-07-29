# Design

## 1. The problem

One connection carries a heterogeneous, high-volume event stream. Every event must be classified
and dispatched to type-specific work, and those paths must run, fail and scale independently.

Three properties of the source drive everything that follows.

**One connection, no ingest parallelism.** Jetstream is a single WebSocket — no consumer group,
exactly one goroutine reads it. Anything that goroutine does inline is charged against the
throughput of the entire stream.

**A severely skewed type distribution.** Measured over 186,492 events, unfiltered:

| Route | Events | Share | ev/s |
| --- | ---: | ---: | ---: |
| engagement (like, repost) | 139,509 | 74.8% | 223 |
| content (post) | 18,993 | 10.2% | 39 |
| social-graph (follow) | 10,456 | 5.6% | 17 |
| retraction (any delete) | 9,622 | 5.2% | 13 |
| default (identity, account, unrouted) | 7,912 | 4.2% | 13 |

A **13.3× spread** between the busiest and quietest real route sharing one ingest — any resource
shared between them is monopolised by the former. And **4% of the stream is collections with no
route at all**, which is the concrete argument for D4 and exactly what a server-side filter would
have hidden. One window, indicative rather than bounding.

**No exactly-once delivery.** Reconnects resume from a `time_us` cursor, so duplicates are
guaranteed and every handler must tolerate them.

Routing is two-dimensional rather than a flat switch: an event's type is the tuple
`(kind, collection, operation)`, and `operation = delete` cross-cuts collection.

**The question the design has to answer: how does one slow or failing consumer avoid affecting any
other?**

## 2. Options considered

**A — Inline dispatch.** Handle the event in the reader loop. *Rejected:* head-of-line blocking.
One slow call stalls the socket, the cursor stops advancing, and the server's replay window
eventually laps us. This is the failure the exercise is really about.

**B — One shared queue and a generic worker pool.** *Rejected:* decouples ingest from processing
but gives no per-type isolation. A burst of likes fills the queue and follows wait behind it;
nothing can be scaled, retried or failed independently.

**C — Per-type queue with a dedicated worker pool (chosen).** Read, classify, push to a bounded
channel owned by that route, drain with a pool sized for it. Each route owns its buffer,
concurrency, retry policy, drop policy and metrics; a stalled route fills its own buffer and
nothing else's.

*What C costs.* Capacity is fragmented — idle workers on a quiet route cannot help a saturated one
— N buffers cost N times the memory, and there is more configuration to get wrong. The trade is
utilisation for predictability: right when the distribution is this skewed and congestion behaviour
is what is being judged; wrong under uniform load, where B is simpler and sufficient.

*The honest limit.* Isolation is **between** routes, not **within** one. All five share a process,
a scheduler, a heap and a garbage collector. D6 stops that shared fate being fatal; a broker is the
real fix, which is why it leads section 5.

**Also rejected.** *Shared capped pool* — isolation on admission without fragmenting capacity, but
a wedged worker still reaches every route. *External broker* — the production answer, the wrong
cost here; `Offer(ctx, event) bool` is narrow so it becomes a swap, not a rewrite. *Managed
fan-out* — removes most of this code along with most of the reasoning being assessed. *Workflow
orchestration* — wrong shape: single-hop dispatch at high volume, and the state that matters spans
events rather than one execution. *Server-side filtering* — legitimate load shedding, but it
discards the type mix this service exists to route.

## 3. The chosen design

| Component | Responsibility |
| --- | --- |
| Ingestor | The socket, the reconnect state machine, the cursor. Decodes only what routing needs |
| Classifier | Derives `(kind, collection, operation)`. Pure function |
| Router | Routing key → route name via a ConfigMap-supplied table with a `default` fallback |
| Route | A bounded channel plus a worker pool, with its own config and metrics |
| Handlers | The per-type work. Each knows nothing about the others |
| Sinks | Structured logs here; webhook or broker in production |

```
        Jetstream WebSocket  (one connection, one reader)
                     |
             ingestor: reconnect FSM, cursor, envelope decode
                     |  classify -> route name   (pure)
   +----------+------+------+-----------+---------+
   v          v             v           v         v
 content  engagement  social-graph  retraction  default
 4 wrk       8 wrk        2 wrk        2 wrk      1 wrk
  drop        drop         drop        block       drop
   +----------+------+------+-----------+---------+
     each is an isolation domain — nothing crosses it
```

Counts and policies are deliberately **not** uniform: the routes differ in volume by more than an
order of magnitude and in what a lost event costs.

### D1 — The ingest goroutine never blocks

Sends into route channels are non-blocking; a full buffer drops the event and increments
`events_dropped_total{route}`. Blocking would be real backpressure, but on a single shared reader
blocking one route stalls *every* route — head-of-line blocking through the back door.

**A buffer is time, not space.** Size divided by arrival rate is how long a route can be completely
stalled before it loses events, and that is the number worth choosing. Sized for roughly sixty
seconds of tolerance at twice the measured mean rate, the five buffers give 53–79 seconds; the
first, unmeasured attempt spanned 80–307 seconds — a 3.9× spread nobody had chosen.

Both bounds are real. **Too small** and the route sheds on every ordinary burst, because traffic
arrives in waves and the buffer exists for the peak. **Too large** and events go stale — an alert
from a ten-minute-old event is worth little, and in a detection context close to nothing — and *the
failure hides*: a route can be wedged for half an hour with its drop counter still at zero. **The
drop counter is not only loss, it is the alarm.**

`block` exists as a per-route policy for retraction, where a lost deletion is worse than a delayed
one and congestion is implausible — and even it carries a timeout, so one route can never hold the
stream hostage.

The reader decodes only the three fields routing needs, leaving `commit.record` as raw bytes for
the owning worker. **"The reader" means everything up to and including the enqueue** — a
distinction that turned out to matter (section 7).

### D2 — The cursor advances on enqueue, not on completion

Once buffered, an event is that route's problem. Committing on completion would mean tracking N
independent positions and committing the minimum — real complexity in a pipeline that is
at-least-once regardless.

**Dropped events advance the cursor too, so shedding is irreversible.** The alternative is holding
the cursor back on precisely the route already overloaded. Drops are permanent and unreplayable,
not deferred — D1's sharpest edge, and why section 4 treats shedding as a security decision. A
crash also loses whatever is buffered; section 5 fixes that first.

### D3 — A key is mutated by one thing at a time

Counters are partitioned by `hash(subjectKey) % N`, and a worker takes that shard's lock for the
read-modify-write.

An earlier version had no locks — events were hashed to a worker *before* enqueue — but the hash
key lives inside `commit.record`, so choosing the queue meant decoding on the ingest goroutine
(section 7). **The guarantee is unchanged; the cost is a mutex**, which is the right way round: an
uncontended lock is nanoseconds, while work on the shared reader is charged against the whole
stream. **Lock-free ownership was a means; per-key consistency was the end.**

Hot keys remain the trade-off: a viral post serialises behind one shard's lock, and because the
buffer is per-route, sustained pressure on one key can push the whole route into shedding.

Windows use **event time**, not wall clock — after a reconnect the service replays events already
minutes old, and a wall-clock window would read that as a burst on the exact path recovery
exercises most. State is bounded by size and TTL, swept on an event count inside the owning shard.

### D4 — Unknown types are routed, not discarded

An unmatched key goes to `default`, counted and logged as a bounded bucket. New lexicons appear
continuously — 4% of measured traffic — and discovering one should be an observation, not an
outage.

### D5 — The connection lifecycle is an explicit state machine

`Disconnected → Connecting → Replaying → Live`, plus `Reconnecting` with backoff and jitter.
Liveness deliberately does not fail while disconnected — that would restart the pod at the moment
backoff is working correctly. Readiness does, and also fails on excessive cursor lag.

**Every read carries a deadline.** A half-open connection produces no bytes and no error, so an
unbounded read is where the process goes to die quietly: still connected, still reporting ready,
processing nothing. The firehose never goes silent for thirty seconds, so silence is a dead
connection whatever the socket claims.

**Lag is recomputed on a timer, not after each read.** Readiness gates on lag, so computing it
inside the read loop would mean the health check could only observe failures the read loop
survived. **A health signal must not be produced by the code path it exists to check.**

The backoff resets after a connection that held; a counter that only climbs leaves a process pinned
at maximum delay after a handful of blips. If the stored cursor predates the server's retention the
gap is unrecoverable **and of unknown size** — there are no sequence numbers — so it is recorded
and alerted rather than silently skipped.

### D6 — Failure is contained per event, not per process

Every worker runs its handler inside `defer recover()`. In Go an unrecovered panic in any goroutine
terminates the process, so one malformed record would take down every other route — falsifying the
isolation claim this design rests on.

Errors are classified before retry: transient failures retry inside the route with bounded backoff,
permanent ones go to the dead-letter path. Retries occupy a worker, never the reader. On `SIGTERM`
the ingestor stops reading first, routes drain against a deadline, and the cursor commits last.

### D7 — Idempotency is a requirement

D2 plus the reconnect rewind guarantees duplicates before any scaling is involved. Handlers key on
`did + collection + rkey + operation` — a record key is unique only *within* its collection. The
seen-key set is bounded by time and size; beyond that window a duplicate is accepted and a counter
is marginally wrong, a deliberate trade of exactness for bounded memory.

### Routes implemented

| Routing key | Route | Work |
| --- | --- | --- |
| `commit / * / delete` | retraction | Cleanup, cross-cuts all collections. **Matched first** |
| `commit / app.bsky.feed.post / create` | content | Keyword and language matching |
| `commit / app.bsky.feed.{like,repost} / create` | engagement | Rolling per-subject counts |
| `commit / app.bsky.graph.follow / create` | social-graph | Per-target sliding window |
| anything else | default | Count, log a bounded bucket |

`operation == delete` is evaluated before the collection map — not by convention but because a
delete commit carries no `record`, so a create-path handler would be matching against a body that
does not exist. The ordering is a property of the router rather than of rule order in the
ConfigMap.

## 4. Security considerations

Every field is attacker-controlled, and the service cannot distinguish ordinary traffic from
traffic shaped against it.

- **Load shedding is an evasion primitive.** A dropped event is a missed detection and, per D2, it
  is gone for good. An adversary who can generate load can fill a buffer on purpose and hide inside
  the discarded window. In production a durable broker removes the need to shed, and where it is
  unavoidable a detection route never sheds. Every drop should raise an alert, not just a counter.
- **Bounded queues, bounded state.** Capping channels while leaving aggregation maps unbounded only
  moves the exhaustion target. Writing the eviction is not the same as wiring it — a mistake this
  codebase actually made.
- **Record text never reaches a log line.** Writing attacker-controlled text into a structured log
  hands an adversary control of what the SIEM ingests: forged entries, broken parsers, injected
  fields. Only length, language and matched-or-not are logged.
- **Metric labels are a fixed set**, and so is anything else keyed on attacker-supplied values —
  including log throttles. Labelling by `collection` would let anyone publishing a novel lexicon
  grow the metric space without limit.
- **Parsing happens where failure is contained** — frame-size limits at the edge, record bodies
  parsed inside the owning worker rather than on the shared reader.
- **Alert paths are rate-limited**; a threshold can be crossed deliberately, making an unthrottled
  alert path a remotely triggerable flood.
- **Minimal data, minimal runtime.** Nothing persisted by default. Distroless, non-root, read-only
  root filesystem, capabilities dropped, NetworkPolicy limiting egress. The service needs no
  credentials at all, and that is worth keeping true.

## 5. From prototype to production

| Concern | This prototype | Production |
| --- | --- | --- |
| Transport | In-process bounded channels | Kafka / NATS / SQS, one topic per route |
| Aggregation state | Bounded in-memory maps, sharded under a mutex | Redis or DynamoDB with atomic increments — removing the partitioning problem entirely |
| Cursor | Local file, committed on enqueue | External store, committed on broker ack |
| Failed events | Counted and logged | Per-route DLQ plus a replay tool |
| Observability | Counters and structured logs | Alerting on drop rate, queue depth and cursor lag |

**Durability moves the cursor commit point.** Once the enqueue target is a broker, the cursor
commits on acknowledgement and a crash stops losing buffered events. The single most valuable
upgrade, and the one that removes the pressure to shed.

**Ingest scales by sharding, not replicas.** The connection is a singleton — two readers over the
same range process everything twice. Scaling means N connections partitioned by `wantedCollections`
or `wantedDids`, each a strict singleton, with a deployment strategy that tears down before it
brings up. The default rolling update briefly runs two pods, tolerable only because of D7.

The AWS-native alternative — Fargate holding the connection, EventBridge to SNS/SQS, a Lambda per
type — makes routing configuration rather than code, at the cost of lock-in and much less control
over batching and shedding. For a service whose value is how it behaves under congestion, that
control is worth keeping.

## 6. Scope and tests

Built to the boundary of the timebox and no further. **Described rather than built:** the broker
swap, external state, a DLQ that outlives the process, sharded multi-connection ingest, replay
tooling. The dispatch interface is the seam each attaches to.

| Test | Why it matters |
| --- | --- |
| **Router table** | Covers a `delete` of each type, an `identity` event with no commit block, and an unknown collection reaching `default` |
| **Isolation under congestion** | The test that proves the architecture rather than asserting it |
| **Panic containment** | The other half of that claim: without recovery, one bad record ends every route |
| **Record decoded only by the worker** | Pins D1's decode budget after a review found it violated |
| **Concurrent workers on one key** | D3 relies on a lock, so the guarantee needs verifying under `-race` |
| **Window and threshold logic** | Fires once at the boundary, forgets old entries, uses event time |
| **Cursor and idempotency** | Survives a simulated reconnect; the replayed overlap does not double-count |
| **Integration** | A fake WebSocket server replays fixtures through the full pipeline |

Not covered: the live connection (a network test, not a logic test) and load testing. Manual
verification is a `kind` deploy with logs tailed against the real firehose.

## 7. Defects the design did not prevent

Seven defects reached working code — three caught by running the service, four by later reviews
that read it. **The design document caught none of them, and it explicitly forbade the worst one.**

**Found by reading the code.** *No read deadline, and a health check that could not see its own
failure*: the read used a process-lifetime context, and because lag was recomputed only after a
successful read, the probe that exists to catch exactly that would have stayed green forever — two
reasonable choices whose interaction was a silent death with no alarm. *The reader decoded the
record body*, which D1 forbids in its own words: selecting a per-worker queue needed a key from
inside `commit.record`, so the enqueue path ran a full `json.Unmarshal` of every like and repost on
the ingest goroutine. The invariant existed before the code and the code broke it anyway, because
"the reader" was read as "the read loop". *A TTL sweep that nothing called*, leaving only the size
cap live. *A backoff counter that never reset.*

**Found by running it.** *A log-flood vector the security section did not prevent* — unknown
collections were throttled on the raw, attacker-supplied collection, so every novel lexicon earned
its own line. *Configuration that could not load*, caught cleanly because config is validated at
startup. *A test that passed for the wrong reason* — the isolation test's own control route was
under-buffered and shedding, and the assertion was too weak to notice.

**The pattern.** Each was written in the same confident register as the code that was correct.
Neither kind would have been found by reviewing the design — which is the argument for treating
this document as **a claim to be checked, not a record of what is true.**

## 8. Known gaps

- **Isolation is between routes, not within one.** One process, one heap, one GC.
- **A hot key degrades its whole route** — updates serialise on one shard lock, and the buffer is
  per-route rather than per-shard.
- **D3 costs a mutex.** Lock-free ownership is nicer, but the only way to get it was to decode on
  the reader — a worse trade this codebase made and had to undo.
- **Nothing handles an event type needing state from two routes at once.** Either that state moves
  to an external store both read, weakening the no-shared-resource property, or an explicit join
  stage goes downstream of both. The second is cleaner; neither is built.
- **The dead-letter path does not outlive the process.** A DLQ nobody can read after a restart is a
  data-loss mechanism with extra steps.
- **Aggregation state does not survive a restart** — a threshold about to fire will not.
- **No load test.** The service keeps up comfortably (lag under 0.1s, zero drops, zero panics over
  186,492 events), but the shed path has only been exercised by shrinking a buffer deliberately.
