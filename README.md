# Jetstream Event Router

A Go service that consumes the Bluesky Jetstream firehose over a single WebSocket, classifies
every event, and fans it out to per-type workers that run, fail and scale independently of one
another.

The reasoning behind the design — the options weighed, the decisions and their costs — is in
[`DESIGN.md`](./DESIGN.md). That is the document to read first. This file is how to run it.

[`AI.md`](./AI.md) covers how the agent was used, and where I overrode it.

## Requirements

- Docker
- [kind](https://kind.sigs.k8s.io/) and `kubectl`
- Go 1.25+ **only if you want to build or test outside Docker**. The metrics client pulls a
  dependency tree that requires it; the container build uses `golang:1.25-alpine`, so
  `make deploy` works on any machine with Docker regardless of the local toolchain.

Nothing else. The service needs no credentials: Jetstream is a public, unauthenticated endpoint.

## Run it

```bash
# 1. Create a local cluster
kind create cluster --name jetstream-router

# 2. Build the image and load it into the cluster
docker build -f deploy/Dockerfile -t jetstream-router:dev .
kind load docker-image jetstream-router:dev --name jetstream-router

# 3. Deploy
kubectl apply -f deploy/

# 4. Watch it work
kubectl logs -f deploy/jetstream-router
```

`make deploy` does steps 1–3 in one command.

Within a few seconds of the pod becoming ready you should see the connection come up and events
start flowing. The firehose is busy — likes dominate — so the engagement route carries roughly
three quarters of all traffic immediately.

## What you should see

Structured JSON logs, one line per notable event. Nothing user-generated is ever logged; only
structural and derived fields (see the security section of `DESIGN.md`).

```
{"level":"INFO","msg":"starting","routes":["retraction","content","engagement","social-graph","default"]}
{"level":"INFO","msg":"connection state","state":"live"}
{"level":"INFO","msg":"keyword matched","route":"content","did":"did:plc:…","matched":["security"],"text_len":300}
{"level":"WARN","msg":"threshold crossed","route":"engagement","subject":"at://…","count":100}
{"level":"INFO","msg":"burst detected","route":"social-graph","target":"did:plc:…","window_s":300,"count":25}
{"level":"INFO","msg":"unknown collection","route":"default","bucket":"other"}
{"level":"WARN","msg":"buffer full, event dropped","route":"engagement","dropped_total":1284}
```

The last two lines are the interesting ones. `unknown collection` is a new lexicon appearing on the
network and being observed rather than swallowed — about 4% of the unfiltered stream. `buffer full,
event dropped` is the isolation mechanism working as designed: one route shedding while the others
keep draining.

Per-event routing decisions are logged at `DEBUG`, not `INFO`. At firehose rates that is several
hundred lines a second, which is a denial-of-service on your own log pipeline; the per-route
counters carry the same information at a rate a human can read. Set `LOG_LEVEL=debug` to see them.

### Seeing the routes behave differently

```bash
kubectl port-forward deploy/jetstream-router 8080:8080
curl -s localhost:8080/metrics | grep -E 'events_(routed|dropped)_total'
```

Watching `events_dropped_total{route=…}` climb on one route while the others keep processing is the
clearest demonstration that the fan-out is real. `make congest` shrinks the engagement buffer to 1
so you can watch exactly that happen against live traffic, and `make restore` puts it back. On a
recent run that produced 17 drops on engagement and zero on every other route, all of them still
processing.

`/healthz` and `/readyz` are on the same port. They differ deliberately: losing the upstream makes
the service un-ready but not dead, and readiness also fails when cursor lag exceeds `max_lag`.

## Configuration

Everything tunable lives in a ConfigMap (`deploy/configmap.yaml`) so that adding an event type is a
configuration change rather than a code change.

| Key | Default | What it does |
| --- | --- | --- |
| `routing.rules[].match` | see below | Routing key: `kind`, `collection` (glob), `operation` |
| `routing.fallback` | `default` | Where unmatched events go. Required |
| `routes[].workers` | varies | Size of that route's worker pool |
| `routes[].buffer` | varies | Bounded channel capacity |
| `routes[].policy` | `drop` | `drop` sheds and counts; `block` applies backpressure |
| `routes[].block_timeout` | `2s` | Caps how long `block` may stall the reader |
| `jetstream.wantedCollections` | *(empty)* | Server-side filter. Off deliberately — see `DESIGN.md`. `["derive"]` takes it from the routing table |
| `keywords`, `languages` | — | Content route match list |
| `thresholds.engagement` | 100 / 5m | Rolling count that raises an alert |
| `thresholds.followBurst` | 25 / 5m | Follows to one target inside the window |
| `state.max_keys_per_shard` | 20000 | Bound on every aggregation map |
| `state.dedup_window` | `2m` | How long a replayed event is remembered |

Routing precedence is operation-first: `operation == delete` matches before the collection map,
because a delete commit carries no record for a create-path handler to match against. That ordering
is a property of the router, not of the order rules appear in the ConfigMap.

## Layout

```
cmd/router/           main: config, wiring, signal handling
internal/jetstream/   WebSocket client, reconnect state machine, cursor
internal/event/       envelope types, lazy record decoding
internal/routing/     classifier + router (pure functions)
internal/route/       bounded channel + worker pool + panic recovery
internal/handler/     one file per route, plus the bounded state primitives
internal/obs/         metrics, structured logging, health endpoints
internal/config/      config struct, loaded from the ConfigMap
deploy/               Dockerfile, manifests
```

## Tests

```bash
go test ./...
go test -race ./...
```

Six tests carry the design rather than chasing coverage: the router table (including delete
precedence, an `identity` event with no commit block, and an unknown collection), isolation under
congestion, panic containment, window logic on event time, cursor and idempotency across a
simulated reconnect, and an integration test against a fake WebSocket server so the suite does not
depend on the live network.

## Assumptions

- **No server-side filtering.** `wantedCollections` is supported but off by default. Filtering at
  the source would discard the type mix this service exists to route.
- **The cursor is a local file** on an `emptyDir`. Adequate for a single-pod prototype; production
  would commit it to Redis or DynamoDB on broker acknowledgement. A pod restart therefore resumes
  from live, and a reconnect replays a bounded overlap, which the handlers tolerate because they
  are idempotent.
- **Aggregation state is in memory,** bounded by size and TTL. It does not survive a restart —
  counters reset, so a threshold that was about to fire will not.
- **One replica.** The connection is a singleton; two readers over the same range would process
  everything twice. The Deployment uses `Recreate` rather than the default rolling update for this
  reason.
- **Alerts are structured log lines.** No webhook, no notification service. The `Sink` interface is
  where a real one would attach.

## Scope

Implemented: the full pipeline — connection with reconnect and resume, classification, routing,
bounded per-route queues with drop accounting, worker pools with panic recovery, and the five
routes described in `DESIGN.md`.

Deliberately described rather than built, and marked as such in the design: the broker swap,
external state, per-route dead-letter queues that outlive the process, sharded multi-connection
ingest, and replay tooling. The dispatch interface is the seam each of those attaches to.

## Teardown

```bash
kind delete cluster --name jetstream-router
```

## A note on the source

This reads the unfiltered public firehose of a live social network. Record text can contain
anything, and it is treated as attacker-controlled throughout: it is never written to a log line,
never persisted, and parsed only inside the worker that owns it. The service processes event
structure and metadata, not content.
