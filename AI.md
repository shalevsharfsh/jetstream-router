# AI.md

How I used AI agents on this exercise, and where I did not take their output.

## Setup

| | |
| --- | --- |
| **Tool** | Claude Code, in the terminal, against this repo |
| **Model** | Claude Opus 5 throughout — design, implementation, and review |
| **Rules file** | `CLAUDE.md`, in the repo root. The single most useful piece of configuration here |
| **MCP servers** | **Atlassian** — the design was drafted in Confluence and the agent edits that page directly rather than me moving prose between two places. **Lucid** — the architecture diagram |
| **Sub-agents** | Two: one to research the problem domain before any design work, one read-only agent to map a prior codebase whose conventions I wanted to reuse |
| **Verification** | `go vet`, `go test -race`, a `kind` deploy, and the live firehose. Nothing here is claimed working because the model said so |

I split the work deliberately: design reasoning in a long conversation with no repo access, implementation in a session with the repo and `CLAUDE.md` in context. That stopped the design conversation drifting into premature implementation detail, and stopped the implementation agent relitigating decisions already settled.

**Division of labour, plainly.** The agent wrote most of the code and the first draft of every document. I set the objective, made the architectural calls, rejected several of its recommendations, and drove the review passes that found what mattered. I have read every file and can defend every decision in it.

## The configuration that mattered most

`CLAUDE.md` restates each design decision as an invariant a reviewer — human or agent — can check code against: the ingest goroutine never blocks, so a bare `ch <- event` on that path is a bug; windows use `time_us`, so `time.Now()` in windowing logic is a bug; every map gets its bound in the same change that adds it.

The rule that changed agent behaviour most was not technical: *if a request conflicts with an invariant, say which invariant and why before writing anything.* Without it the agent silently picks one. With it, the disagreements surface where I can rule on them.

I also wrote the out-of-scope list into the rules file. Left alone, agents build the broker, the DLQ and the sharding — all of which the design deliberately describes rather than implements. Naming them as out of scope worked better than repeating "keep it small".

**Two invariants were written after a bug rather than before it.** `I2` now defines "the reader" as everything up to and including the enqueue, because that ambiguity is the loophole a real defect walked through. `I7` now says *wire* the eviction, not just write it.

## The prompts behind the decisions

Condensed from longer originals, but each carries the instruction actually given. The implementation prompt is reproduced in full in the appendix.

**1. Sample the wire before writing the classifier.**

> Before writing the classifier, connect to the live Jetstream and confirm the real payload shapes — especially what a `delete` actually carries.

The most valuable instruction I gave. Three findings changed the code: deletes carry no `record` at all, so referential cleanup is impossible without an index the create path writes — now a documented limitation rather than a silent bug; the measured type distribution in section 1 of the design; and real event shapes for the fixtures. Writing the classifier from the brief's example event would have produced something that looked right and was wrong.

**2. Attack my own design.**

> Read the requirements and my design. Are we done? What did we miss? This is a security company and there is no threat modelling at all. We never discussed error handling or idempotency. Is the solution we chose the best one? What are its drawbacks?

The largest single improvement. It surfaced that a panic in any goroutine kills the whole process — so the isolation claim in my document was not true in code without `defer recover()` in every worker. It produced the security section, and reframed the drop policy: in a detection context, load shedding is an evasion primitive.

**3. A second opinion, with the agent told not to touch anything.**

> Can you do a design review for this design? Please don't change anything there — just read it and write your thoughts here.

Constraining it to *read and report* rather than *read and fix* was the point. An agent with write access quietly patches what it finds, and I would have inherited the fixes without ever seeing the findings.

Four came back, all real: delete precedence was implied by row order and never stated; D1 and D2 did not compose, because a dropped event advances the cursor and is therefore permanently lost rather than merely counted; Option C had no stated cost while A and B each had a crisp rejection — the tell of a document rationalising a decision rather than making one; and the skew was asserted rather than measured. Each is now addressed in the design.

**4. Implementation, against a contract written first.**

Before any code I turned the settled design into two files: `DESIGN.md` for the reasoning, and `CLAUDE.md` restating each decision as an invariant with its reason attached. The prompt itself is in the appendix; what matters is one section of it, which asked the agent — before writing anything — to list *any invariant that the most natural implementation of some other part of the design would violate as a side effect.* Not where it might forget a rule. Where two correct-looking decisions collide.

**It also did not work, and that is the useful part.** The collision the prompt names as its own example — D3 wanting a single owner per key, `I2` keeping the record body away from the reader — is precisely the pair that turned out to be violated. The shard key lives inside `commit.record`, so partitioning before the enqueue meant decoding on the ingest goroutine. The prompt pointed directly at it, produced an analysis, and the analysis did not catch it.

That is not an argument against the prompt; it is the argument for everything downstream of it. A pre-implementation review narrows the space but does not close it — which is why the definition of done asks the agent to *point at the line* satisfying each invariant rather than confirm that it did, and why the code review below existed at all.

**5. Review the code, not the design.**

Late, and the most productive pass of all, because by then there was code to read rather than prose to agree with. It found an invariant violation that both the design document and the rules file explicitly forbade, plus a dead TTL sweep and a backoff counter that never reset.

I did not take it on trust: I verified every claim against the code before changing anything. All were real — and one, a mismatch between document and code, turned out to be an error I had introduced myself earlier the same day.

## Where I overrode it

- **The idempotency key was wrong.** It proposed `did + rkey + operation`. In the AT Protocol a record key is unique only within its collection, so the collection has to be part of the identity. The kind of error that reads as correct and is not; now an invariant.
- **It violated an invariant it had itself written.** Shard selection called into the handler to hash `subject.uri` — a full `json.Unmarshal` of every like and repost **on the single ingest goroutine**, on attacker-controlled nested content, decoded twice. `I2` forbids exactly this. The fix moved partitioning into the worker and pays a sharded mutex for it: D3 loses its "no locks" property, `I2` keeps the thing that actually matters.
- **A TTL sweep that nothing called.** `Window.Sweep` existed, was documented as running on a timer, and had exactly one caller: its own test. Half of `I7` was decorative.
- **Substring keyword matching shipped and ran.** With `ai` in the list it fired on *said*, *again*, *email*, *Dubai* — against live data, the overwhelming majority of matches. On a notification path that is worse than not matching at all. The fix came from a test case for a keyword containing `+`, which exposed that `\bc\+\+\b` can never match: the character after `+` would have to be a word character.
- **A log throttle keyed on the wrong thing.** The default route logs unknown collections as a bounded bucket, but the throttle was keyed on the raw, attacker-supplied collection — so every novel lexicon earned its own line, which is the exact flood the throttle existed to prevent.
- **It left another project's CI in place.** After the rewrite, `.github/workflows/ci.yml` still installed Python and ran `pytest` against five paths, none of which existed. Actions was red on three consecutive commits and I pushed past it without looking. The most embarrassing item here, and the one a reviewer would have seen first.
- **It over-built the design document.** After several improvement passes the write-up had reached roughly 4,500 words against a brief asking for 1–3 pages. I cut it by a third and stopped there, keeping the two sections a reader would least benefit from losing — and said so at the top of the document rather than quietly shipping eight pages. An agent will keep adding for as long as you keep asking; deciding when to stop is not something it will do for you.

## What I would do differently

**Anticipate more of the invariants instead of harvesting them from bugs.** The contract existed before the implementation, but two of its rules did not — and both were foreseeable. The rules written up front are also the ones that held.

**Run it earlier.** Three of the six defects in section 7 of the design needed the service running to surface, and one needed a clean-cluster deploy following my own README. I wrote a great deal of prose about behaviour under congestion before ever watching it behave. And three red builds is not a subtle signal.

**Ask for compliance to be cited, not confirmed.** Working backwards from the commits that should not have existed, the instruction I most wish I had opened with is *name the file and line that satisfies each invariant in `CLAUDE.md`*. Stating a rule does not enforce it — a plainly written invariant did not stop the code from breaking it. Forcing the agent to cite its own compliance turns prose into a check. The second would be *then do one pass whose only job is to break it*: asked to build and verify in the same breath, an agent verifies optimistically.

The broader lesson is that the prompt was not the bottleneck — the missing contract was. Once `DESIGN.md` and `CLAUDE.md` existed, most of the instruction's length went on *how to look for trouble* rather than on what to build, because everything the agent would otherwise invent had already been decided. **A thin specification makes for a long prompt; a thick one makes the prompt almost entirely about verification.**

## An honest summary

The agent was most valuable as an adversary and least valuable as an author. Every structural decision in `DESIGN.md` is one I can defend and would defend differently under different constraints; the sharpest additions came from asking it to attack the design rather than produce one.

The pattern in the override list is the thing worth taking away: **the agent will write an invariant, cite that invariant, and then violate it three files away — all in the same confident register.** Its output reads identically whether or not it has been verified. So the review that counts is the one that executes the code, or reads it against the document — not the one that reads the document alone.

That is also why section 7 of the design exists, and why it is written the way it is: a design document is a claim to be checked, not a record of what is true.

---

## Appendix — the implementation prompt in full

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
