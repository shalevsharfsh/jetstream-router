# AI.md

How I used AI agents on this exercise, and where I did not take their output.

## Setup

| | |
| --- | --- |
| **Tool** | Claude Code, in the terminal, against this repo |
| **Model** | Claude Opus 5 throughout — design conversation, implementation, and the review passes |
| **Rules file** | `CLAUDE.md`, checked into the repo root. The single most useful piece of configuration here; see below |
| **MCP servers** | **Atlassian** — the design was drafted in and still lives in Confluence, and the agent reads and edits that page directly rather than me copying prose between two places. **Lucid** — the architecture diagram in section 3 |
| **Sub-agents** | Two: one to research the problem domain and prior art before any design work, and one read-only agent to map an existing codebase whose conventions I wanted to reuse |
| **Verification** | `go vet`, `go test -race`, a `kind` deploy, and the live firehose. Nothing here is claimed working because the model said so |

I split the work deliberately: design reasoning in a long conversation with no repo access, implementation in a session with the repo and `CLAUDE.md` in context. Keeping those apart stopped the design conversation from drifting into premature implementation detail, and stopped the implementation agent from relitigating decisions that were already settled.

**Division of labour, plainly.** The agent wrote most of the code and the first draft of every document. I set the objective, made the architectural calls, rejected several of its recommendations, and drove the review passes that found what mattered. I have read every file and can defend every decision in it.

## The configuration that mattered most

`CLAUDE.md` encodes the design decisions as invariants a reviewer — human or agent — can check code against:

- the ingest goroutine never blocks, so a bare `ch <- event` on that path is a bug
- windows use `time_us` from the event, so `time.Now()` in windowing logic is a bug
- every map is bounded, and if you add state you add its bound in the same change
- never log record content; metric labels come from the fixed route set only

The rule that changed agent behaviour most was not technical: *if a request conflicts with an invariant, say which invariant and why before writing anything.* Without it the agent silently picks one. With it, the disagreements surface where I can rule on them.

I also wrote the out-of-scope list into the rules file. Left alone, agents build the broker, the DLQ and the sharding — all of which the design deliberately describes rather than implements. Naming them as out of scope worked better than repeating "keep it small".

**Two invariants were written after a bug rather than before it.** `I2` now defines "the reader" as everything up to and including the enqueue, because that ambiguity is the loophole a real defect walked through. `I7` now says *wire* the eviction, not just write it. Both are in section 7 of the design.

## Prompts that did the real work

Condensed — the originals were longer and more conversational — but each carries the instruction
that was actually given, and the outcome described after it is what came back.

### 1. Sample the wire before writing the classifier

> Before writing the classifier, connect to the live Jetstream and confirm the real payload shapes — especially what a `delete` actually carries.

The single most valuable instruction I gave. It produced three findings that changed the code: deletes carry no `record` at all — so referential cleanup is impossible without an index the create path writes, now a documented limitation rather than a silent bug — the measured type distribution in section 1, and real event shapes for the test fixtures. Writing the classifier from the brief's example event would have produced something that looked right and was wrong.

### 2. Attack my own design

> Read the requirements and my design. Are we done? What did we miss? This is a security company and there is no threat modelling at all. We never discussed error handling or idempotency. Is the solution we chose the best one? What are its drawbacks?

The largest single improvement. It surfaced that a panic in any goroutine kills the whole process — which meant the isolation claim in my document was not true in code without `defer recover()` in every worker. It produced the security section, and reframed the drop policy: in a detection context, load shedding is an evasion primitive, because an adversary who can generate load can hide inside the discarded window.

### 3. A second opinion, with the agent told not to touch anything

> Can you do a design review for this design? Please don't change anything there — just read it and write your thoughts here.

Constraining it to *read and report* rather than *read and fix* was the point. An agent with write access quietly patches what it finds, and I would have inherited the fixes without ever seeing the findings. Four came back:

- **Delete precedence was never stated.** The routing table implied it by row order. "Where does a deleted post go?" is the first routing question anyone asks. Now stated *and* enforced by the router rather than by rule ordering, so a reshuffled ConfigMap cannot reintroduce it.
- **D1 and D2 did not compose.** A dropped event is never enqueued, so does the cursor advance past it? It must — which makes shedding irreversible, not merely counted. The document argued drops were dangerous and stayed silent on the fact that they are permanent.
- **Option C had no stated cost** while A and B each had a crisp rejection. That asymmetry is the tell of a document rationalising a decision rather than making one.
- **The skew was asserted, not measured.** Fixed by measuring it — the figures in section 1 are from a real run.

### 4. Implementation, against a contract I wrote first

Before any code, I turned the settled design into two files: `README.md` — how the service runs,
what is configurable, what the log output looks like — and `CLAUDE.md`, which restates each design
decision as an invariant with its reason attached. Then:

> The design is settled and both files are in the repo. Build the service against them.
>
> Where they are specific — the package layout, the config schema, the metric names, the log
> messages, the deployment shape — follow them exactly. Those are deliberate choices, not
> suggestions, so do not improve on them. Where they are ambiguous or contradict each other, stop
> and ask rather than picking a reading. If you think one of the invariants is wrong, tell me which
> and why before you write any code.

Writing the contract first is the part that mattered; the prompt is thin because it can be. Every
decision an agent would otherwise invent plausibly and wrongly — where packages live, what the
metrics are called, whether the ingest deployment is a `Deployment` or a `StatefulSet` — was
already made, so there was nothing to negotiate and nothing to review afterwards except conformance.

It also changed how disagreements surfaced. When the agent wanted a log line per routed event, it
flagged the conflict with the log-flood rule and asked, instead of quietly doing it — which is the
rules file working exactly as intended. Several hundred lines a second would have been a
denial-of-service on my own log pipeline; the routing decision is now at `DEBUG`, and the per-route
counters carry the same information at a rate a human can read.

### 5. Review the code, not the design

> What do you think of this review?

Late, and the most productive pass of all, because by then there was code to read rather than prose to agree with. It found an invariant violation that both the design document and the rules file explicitly forbade, plus a dead TTL sweep and a backoff counter that never reset.

I did not take it on trust: I verified every claim against the code before changing anything. All were real — and one, a mismatch between the document and the code, turned out to be an error I had introduced myself earlier the same day.

## Where it helped

- **Pressure-testing.** Asking "what did we miss" repeatedly, and taking the answers seriously, was worth more than any generated code. Most of what it found were internal contradictions — places where the document claimed something the design did not deliver.
- **Boilerplate with a known shape.** Worker pool wiring, table-driven test scaffolding, Dockerfile and manifests. Well-trodden ground where review is fast.
- **Writing the argument down.** I knew why I rejected the shared-queue option; getting it into three defensible sentences was faster with help.
- **A test that caught the code written beside it.** A case for a keyword containing `+` failed, exposing that `\b` cannot match next to punctuation — `\bc\+\+\b` matches nothing, because the character after `+` would have to be a word character. The boundary check is now a lookaround pair.

## Where I overrode it

- **The idempotency key was wrong.** It proposed `did + rkey + operation`. In the AT Protocol a record key is unique only within its collection, so the collection has to be part of the identity. The kind of error that reads as correct and is not; now an invariant.
- **It violated an invariant it had itself written.** Shard selection called into the handler to hash `subject.uri`, which meant a full `json.Unmarshal` of every like and repost **on the single ingest goroutine** — the busiest route's parse, on the one goroutine whose stall halts the stream, on attacker-controlled nested content, and decoded twice. `I2` forbids exactly this. The fix moved partitioning into the worker and pays a sharded mutex for it: D3 loses its "no locks" property, `I2` keeps the thing that actually matters.
- **A TTL sweep that nothing called.** `Window.Sweep` existed, was documented as running on a timer, and had exactly one caller: its own test. Half of `I7` was decorative.
- **Substring keyword matching shipped and ran.** With `ai` in the list it fired on *said*, *again*, *email*, *Dubai* — against live data, the overwhelming majority of matches. On a notification path that is worse than not matching at all.
- **A log throttle keyed on the wrong thing.** The default route logs unknown collections as a bounded bucket, but the throttle was keyed on the raw, attacker-supplied collection — so every novel lexicon earned its own line, which is the exact flood the throttle existed to prevent.
- **It left another project's CI in place.** After the rewrite, `.github/workflows/ci.yml` still installed Python and ran `pytest` against directories that no longer existed — five referenced paths, none of them present. Actions was red on three consecutive commits and I pushed past it without looking. The most embarrassing item on this list, and the one a reviewer would have seen first.
- **It over-built the design document.** After several improvement passes the write-up had grown well past the requested length. Cutting it back was my call; it will keep adding for as long as you keep asking.

## What I would do differently

**Anticipate more of the invariants instead of harvesting them from bugs.** The contract existed
before the implementation, but two of its rules did not: `I2`'s definition of "the reader", and
`I7`'s insistence that eviction be *wired* rather than merely written. Both were added after the
defects they would have prevented, and both were foreseeable. The rules written up front are also
the ones that held.

**Run it earlier.** Three of the six defects in section 7 of the design needed the service running to surface, and one needed a clean-cluster deploy following my own README. I wrote a great deal of prose about behaviour under congestion before ever watching it behave.

**Read the CI output.** Three red builds is not a subtle signal.

**Open with a stricter prompt.** Working backwards from the commits that should not have existed,
the instruction I actually gave was missing four things, and each one maps to a specific defect:

- *"If you think the specified approach is wrong, say so before writing any code, and wait."*
  Settling the stack in conversation while building something else was the single largest source
  of rework.
- *"Everything you create must have a caller; everything you reference must exist — check."*
  `Window.Sweep` had no caller; the CI workflow referenced five paths, none of which existed.
- *"Name the file and line that satisfies each invariant in `CLAUDE.md`."* This is the one I would
  keep above all others. Stating a rule does not enforce it, and a plainly written invariant did
  not stop the code from breaking it. Forcing the agent to cite its own compliance turns prose
  into a check.
- *"Then do one pass whose only job is to break it, and report what you did not finish."* Asked to
  build and verify in the same breath, an agent verifies optimistically. The adversarial pass has
  to be a separate named step with its own output.

The broader lesson is that the prompt was not the bottleneck — the missing contract was. Once
`README.md` and `CLAUDE.md` existed, the implementation instruction could be one line and still
land, because every decision the agent would otherwise invent had already been made. A thin prompt
over a thick contract beats a thick prompt over nothing.

## An honest summary

The agent was most valuable as an adversary and least valuable as an author. Every structural decision in `DESIGN.md` is one I can defend and would defend differently under different constraints; the sharpest additions came from asking it to attack the design rather than produce one.

The pattern in the override list is the thing worth taking away: **the agent will write an invariant, cite that invariant, and then violate it three files away — all in the same confident register.** Its output reads identically whether or not it has been verified. So the review that counts is the one that executes the code, or reads it against the document — not the one that reads the document alone.

That is also why section 7 of the design exists, and why it is written the way it is: a design document is a claim to be checked, not a record of what is true.
