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

Prompts 1, 2, 3 and 5 are condensed — the originals were longer and more conversational — but each
carries the instruction that was actually given, and the outcome described after it is what came
back. Prompt 4 is reproduced in full, because its length is the point.

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

Before any code, I turned the settled design into two files: `DESIGN.md` — the reasoning, the
options weighed, the decisions and their costs — and `CLAUDE.md`, which restates each of those
decisions as an invariant with the reason attached. The implementation prompt is given in full
because the contract is the interesting part, not the instruction:

```
Context: DESIGN.md and CLAUDE.md are in the repo root. Read both before anything else.

The design is settled and both files are authoritative. Where they are specific —
the package layout, the config schema, the metric and label names, the log messages,
the routing precedence, the deployment shape — follow them exactly. Those are
deliberate choices with reasoning attached, not defaults I reached for. Do not
improve on them, rename them, or reorganise them. If the code and the document
disagree after you are done, that is a defect regardless of which one is nicer.

Where the two documents are ambiguous or contradict each other, stop and ask rather
than picking a reading. A quiet choice between two plausible interpretations is
worse than a question, because I will not know it was made.

BEFORE YOU WRITE ANY CODE

Do not start with the skeleton. Start with two lists:

1. Anything in the design you expect to be genuinely hard to satisfy, and why.

2. Any invariant in CLAUDE.md that the most natural implementation of some other
   part of the design would violate as a side effect. Not "where might I forget
   to follow a rule" — where do two correct-looking decisions collide.

The second list is the one I actually care about. The failure mode I am worried
about is not you ignoring the design; it is you following two parts of it faithfully
and breaking a third in the gap between them. I9 says unknown types are routed, D3
says state has a single owner per key, I2 says the reader decodes only the routing
tuple — walk the interactions, not the items.

Then propose the build order and the seams you will build against, and wait for me
to confirm before creating files.

WHILE YOU WORK

- Smallest change that satisfies the request. No opportunistic refactoring of
  adjacent code, no renaming things you did not need to touch.
- If a request conflicts with an invariant, name the invariant and the conflict
  before writing anything. Do not pick one silently and do not write a comment
  explaining why you deviated.
- The classifier and the router stay pure functions: an event in, a routing key or
  a route name out. No I/O, no state, no logging inside them. That property is what
  makes the routing table exhaustively testable, so it is load-bearing rather than
  stylistic.
- Standard library first. The only dependencies I expect are a WebSocket client and
  a metrics client. A third one is a conversation, not a decision.
- Every map you add gets its bound in the same change. If you cannot state the bound,
  you do not understand the structure well enough to add it yet.
- Nothing derived from event content reaches a log line or a metric label. Log
  structure and derived values only — length, language code, matched or not. Metric
  labels come from the fixed route set.

TESTS

Write the router table test and the isolation-under-congestion test first, before
the implementation they cover. Those two carry the design; the rest of the suite is
support. No mocking frameworks, hand-written fakes, and do not chase coverage — a
test that passes without proving anything is worse than no test, so make each one
fail for the right reason first.

SCOPE

The out-of-scope list in CLAUDE.md is a hard boundary, not a backlog. If a change
starts pulling in the broker, a persistent DLQ, external state, or multi-connection
sharding, stop and say so. Those are described in DESIGN.md deliberately. "Knowing
where to stop" is part of what is being assessed here, so scope creep is a defect
and not enthusiasm.

DONE MEANS

- go vet ./... clean, go test -race ./... green.
- No new dependency without a stated reason.
- Every claim DESIGN.md makes about behaviour is either true in the code or flagged
  by you as now stale.
- You can point at the line that satisfies each invariant. If you cannot, say which
  one and we will look at it together.

Start with the two lists.
```

**The second list is the whole point.** An agent asked "will you follow these rules" says yes and
means it. The interesting failure is not disobedience — it is two rules that are individually
satisfiable and jointly are not, where following both faithfully breaks a third in the gap between
them. Asking for that analysis *before* the skeleton exists is cheap; asking for it afterwards
means asking someone to argue against code they have already written.

**It also did not work, and that is the useful part.** The collision the prompt names as an example
— D3 wanting a single owner per key, I2 keeping the record body away from the reader — is precisely
the pair that turned out to be violated. The shard key lives inside `commit.record`, so partitioning
before the enqueue meant decoding on the ingest goroutine. The prompt pointed directly at it,
produced an analysis, and the analysis did not catch it.

That is not an argument against the prompt; it is the argument for everything downstream of it. A
pre-implementation interaction review narrows the space but does not close it, which is why the
definition of done asks the agent to *point at the line* satisfying each invariant rather than
confirm that it did, and why the code review in prompt 5 existed at all. Section 7 of the design is
what those two caught between them.

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
`DESIGN.md` and `CLAUDE.md` existed, most of the instruction's length went on *how to look for
trouble* rather than on what to build — because everything the agent would otherwise invent had
already been decided. A thin specification makes for a long prompt; a thick one makes the prompt
almost entirely about verification.

## An honest summary

The agent was most valuable as an adversary and least valuable as an author. Every structural decision in `DESIGN.md` is one I can defend and would defend differently under different constraints; the sharpest additions came from asking it to attack the design rather than produce one.

The pattern in the override list is the thing worth taking away: **the agent will write an invariant, cite that invariant, and then violate it three files away — all in the same confident register.** Its output reads identically whether or not it has been verified. So the review that counts is the one that executes the code, or reads it against the document — not the one that reads the document alone.

That is also why section 7 of the design exists, and why it is written the way it is: a design document is a claim to be checked, not a record of what is true.
