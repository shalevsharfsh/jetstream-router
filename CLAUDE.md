# CLAUDE.md

Rules for working in this repo. Read before writing code.

## What this is

A Go service that consumes the Bluesky Jetstream WebSocket firehose, classifies each event,
and fans it out to per-type workers that run, fail and scale independently. Runs on a local
Kubernetes cluster (kind). Full reasoning is in `DESIGN.md` — read it before proposing
anything structural.

This is a take-home built against a 2–3 hour budget. **Scope discipline is part of the
deliverable.** A clean shape with a few paths wired through beats a sprawling half-built
system.

## Invariants

These come from the design decisions in `DESIGN.md`. Do not violate them without stopping
and asking first — each one is load-bearing for a claim the document makes.

**I1 — The ingest goroutine never blocks.**
Every send into a route channel is non-blocking: `select` with a `default` that drops and
increments the drop counter. A bare `ch <- event` on the ingest path is a bug. The only
exception is a route explicitly configured with `policy: block`, and that path must be
obviously marked and bounded by a timeout.

**I2 — The reader decodes only what routing needs.**
Unmarshal the envelope (`kind`, `commit.collection`, `commit.operation`, `did`, `time_us`,
`commit.rkey`). Leave `commit.record` as `json.RawMessage`. Handlers parse their own record
inside their own worker. Never fully decode a record on the shared reader goroutine.

**I3 — Delete is matched before the collection map.**
`operation == delete` routes to retraction regardless of collection. A delete commit carries
no record, so create-path handlers have nothing to match against. Routing reads
operation-first, then collection — and that ordering is a property of the router, not of the
order rules appear in the ConfigMap.

**I4 — Every worker recovers.**
The handler call in each worker loop is wrapped in `defer recover()`, which logs, increments
`handler_panics_total{route}`, and continues to the next event. An unrecovered panic in any
goroutine kills the process and takes every other route with it — which would make the
isolation claim false.

**I5 — The cursor advances on enqueue, not on completion.**
Including for dropped events. Do not add completion tracking or a per-route commit position.

**I6 — Time comes from the event, not the clock.**
Windows and aggregations use `time_us` from the event. `time.Now()` in windowing logic is a
bug: after a reconnect we replay minutes-old events, and a wall-clock window reads that as a
burst.

**I7 — Every map is bounded.**
Counters, windows and the dedup set are capped by size and TTL. An unbounded map keyed on
anything attacker-supplied is a memory-exhaustion vector. If you add state, add its bound in
the same change.

**I8 — Idempotency key is `did + collection + rkey + operation`.**
`rkey` is unique only within its collection. Do not shorten this key.

**I9 — Unknown types are routed, never silently dropped.**
Unmatched routing keys go to the `default` route, logged and counted.

## Security rules

The input is a public firehose. Treat every field as attacker-controlled.

- **Never log record content.** No post text, no user-supplied strings. Log structural and
  derived values only: length, language code, matched/not-matched, route, counts. Writing
  attacker-controlled text into a structured log hands an adversary the contents of the log.
- **Metric labels come from the fixed route set only.** Never label a metric by `collection`,
  `did`, or any other unbounded value — that is a cardinality-exhaustion vector. Unknown
  collections share one bucket.
- **Anything keyed on an attacker-supplied value must be bounded** — including log throttles.
  Throttle on the bounded bucket, not on the raw value.
- **Enforce limits at decode**: max message size. Reject malformed frames rather than
  propagating them.
- **Rate-limit every alert path.** Thresholds can be crossed deliberately.
- Do not persist anything by default. Do not add a datastore without being asked.

## Code

- **Standard library first.** The only dependencies worth adding are a WebSocket client and
  a metrics client. If you want a third, stop and ask.
- `log/slog` for structured logging. No `fmt.Println`, no custom logger.
- `context.Context` is the first parameter of anything that blocks or does I/O, and is
  honoured on shutdown.
- Keep the classifier and the router **pure functions**. They take an event and return a
  routing key or a route name. No I/O, no state, no logging inside them. This is what makes
  the routing table trivially testable.
- No package-level mutable state. Wire dependencies through constructors.
- Errors wrap with `%w` and carry enough context to identify the route.
- Exported identifiers get a doc comment. Unexported ones get a comment only when the
  reason is not obvious from the name.

## Layout

```
cmd/router/           main: config, wiring, signal handling
internal/jetstream/   WebSocket client, reconnect FSM, cursor
internal/event/       envelope types, lazy record decoding
internal/routing/     classifier + router (pure)
internal/route/       bounded channel + worker pool + recover
internal/handler/     one file per route, plus bounded state primitives
internal/obs/         metrics, logging, health endpoints
internal/config/      config struct, loaded from ConfigMap
deploy/               Dockerfile, manifests
```

The routing registry is configuration, not code. Adding an event type should mean editing a
ConfigMap, not editing a switch statement.

## Tests

Six tests carry the design. Write these before anything else:

1. **Router table** — table-driven, fixture events to expected route. Must cover a `delete`
   of each collection (precedence), an `identity` event with no `commit` block, and an
   unknown collection reaching `default`.
2. **Isolation under congestion** — saturate one route's buffer; assert its drop counter
   rises while another route keeps draining. This is the test that proves the architecture.
3. **Panic containment** — a handler that panics; assert the process survives and other
   routes keep draining.
4. **Window logic** — fires once at the boundary, forgets old entries, evaluated on event
   time.
5. **Cursor and idempotency** — survives a simulated reconnect without a gap, and the
   replayed overlap does not double-count.
6. **Integration** — a fake WebSocket server replays fixtures through the full pipeline.

No mocking frameworks. Fakes are hand-written. Do not chase coverage.

## Out of scope

Do not build: a UI, auth, user management, a database layer, a broker integration, a DLQ
that outlives the process, or multi-connection sharding. These are **described** in
`DESIGN.md`, deliberately. If a change starts pulling one of them in, stop and say so.

## Working style

- Make the smallest change that satisfies the request. Do not refactor adjacent code
  opportunistically.
- If a request conflicts with an invariant above, **say which invariant and why** before
  writing anything. Do not quietly pick one.
- If something in `DESIGN.md` is ambiguous, ask rather than inventing a reading — the
  document is the deliverable being assessed, so a mismatch between it and the code is worse
  than a gap in either.
- When you change behaviour that `DESIGN.md` describes, flag that the document needs
  updating too.
- Do not add TODOs. Either do it or tell me it is out of scope.

## Definition of done

- `go vet ./...` and `go test ./...` pass, and `go test -race ./...` passes for anything
  touching a channel or shared map.
- No new dependency without a stated reason.
- `kubectl logs` shows the change working, since that is how it will be reviewed.
- `DESIGN.md` still matches the code.
