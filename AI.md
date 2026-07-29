# AI.md

How I used AI agents on this exercise, and where I did not take their output.

> **Before submitting:** the items marked `FILL` need your own answer — they describe your setup
> and your prompts, which only you can report honestly. Everything else in this file records
> something that actually happened and is checkable against the repo or its history. Delete this
> block when you are done.

## Setup

| | |
| --- | --- |
| **Editor** | `FILL` — e.g. Cursor, with Claude Code running in the integrated terminal |
| **Models** | `FILL` — which model for which kind of work |
| **Rules file** | `CLAUDE.md`, checked into the repo root |
| **MCP servers** | `FILL` — Atlassian (drafting the design in Confluence) and Lucid (architecture diagram) were used for the write-up; add or remove |
| **Other** | `FILL` — subagents, skills, anything else that shaped output |

I split the work deliberately: design reasoning in a long conversation with no repo access,
implementation in the editor with the repo and `CLAUDE.md` in context. Keeping those apart stopped
the design conversation from drifting into premature implementation detail, and stopped the
implementation agent from relitigating decisions that were already settled.

## The configuration that mattered most

`CLAUDE.md` is the piece of this setup I would keep. It encodes the design decisions as invariants
a reviewer can check code against — for example:

- the ingest goroutine never blocks, so a bare `ch <- event` on the ingest path is a bug
- windows use `time_us` from the event, so `time.Now()` in windowing logic is a bug
- every map is bounded, and if you add state you add its bound in the same change
- never log record content; metric labels come from the fixed route set only

The rule that changed agent behaviour most was not technical: *if a request conflicts with an
invariant, say which invariant and why before writing anything.* Without it the agent silently
picks one. With it, the disagreements surface where I can rule on them.

I also wrote the out-of-scope list into the rules file. Left alone, agents build the broker, the
DLQ and the sharding — all of which the design deliberately describes rather than implements.
Naming them as out of scope was more effective than repeating "keep it small".

One invariant was added *because* of a bug rather than before it. The security rules originally
said metric labels must be bounded; they now also say that anything keyed on an attacker-supplied
value must be bounded, including log throttles. See the third item under "Where I overrode it".

## Prompts that did the real work

### 1. Framing the problem before any code

> `FILL — the prompt you used to open the design conversation`

What came back was a survey of options. What was useful was not the recommendation but being
forced to say why the shared-queue option fails, which became Option B in the design.

### 2. Attacking my own design

> Read the requirements and my design. Are we done? What did we miss? This is a security company
> and there is no threat modelling at all. We never discussed error handling or idempotency. Is the
> solution we chose the best one? What are its drawbacks?

This produced the largest single improvement. It surfaced that a panic in any goroutine kills the
whole process — which meant the isolation claim in my document was not true in code without
`defer recover()` in every worker. It also produced the security section, and reframed the drop
policy: in a detection context, load shedding is an evasion primitive, because an adversary who can
generate load can hide inside the discarded window.

### 3. A second opinion on the draft

I took the design to a separate review pass rather than asking the same context to grade its own
work. It found real inconsistencies I had missed:

- **Delete precedence was never stated.** The routing table implied it by row order and nothing
  said it. "Where does a deleted post go?" is the first routing question an interviewer asks. It is
  now stated in the design *and* enforced by the router rather than by rule ordering, so a
  reshuffled ConfigMap cannot reintroduce the bug.
- **D1 and D2 did not compose.** A dropped event is never enqueued, so does the cursor advance past
  it? It must — which makes shedding irreversible, not merely counted. The document argued drops
  were dangerous and stayed silent on the fact that they are permanent.
- **Option C had no stated cost** while A and B each had a crisp rejection. That asymmetry is the
  tell of a document rationalising a decision rather than making one. The cost paragraph and the
  "isolation is between routes, not within one" admission both came from that.
- **The skew was asserted, not measured.** Addressed by measuring it: the figures in §1 of the
  design are from a real ten-minute run, not borrowed.

### 4. Implementation, scoped tightly

> `FILL — a representative implementation prompt, ideally one where you constrained the agent`

## Where it helped

- **Pressure-testing.** Asking "what did we miss" repeatedly, and taking the answers seriously, was
  worth more than any generated code. Most of what it found were internal contradictions — places
  where the document claimed something the design did not deliver.
- **Boilerplate with a known shape.** Worker pool wiring, table-driven test scaffolding, Dockerfile
  and manifests. Well-trodden ground where review is fast.
- **Writing the argument down.** I knew why I rejected the shared-queue option; getting it into
  three defensible sentences was faster with help.
- **One test caught a bug in the code written alongside it.** A test case for a keyword containing
  `+` failed, which exposed that `\b` cannot match next to punctuation — `\bc\+\+\b` matches
  nothing, because the character after `+` would have to be a word character. The boundary check is
  now a lookaround pair.

## Where I overrode it

- **The idempotency key was wrong.** It proposed `did + rkey + operation`. In AT Protocol a record
  key is unique only within its collection, so the collection has to be part of the identity. This
  is the kind of error that reads as correct and is not; it is now an invariant in `CLAUDE.md`.
- **Substring keyword matching shipped and ran before anyone noticed.** With `ai` in the keyword
  list it fired on *said*, *again*, *email* and *Dubai* — which against live data was the
  overwhelming majority of matches. On a notification path that is worse than not matching at all.
  Only running it against the real firehose made it obvious.
- **It throttled a log line on the wrong key.** The default route logs unknown collections as a
  bounded bucket, but the throttle was keyed on the raw collection — so every novel lexicon earned
  its own log line, which is exactly the log-flood vector the throttle was meant to prevent. Found
  by watching `kubectl logs` rather than by reading the code, and now covered by a test.
- **Confident config that could not load.** `block_timeout: "2s"` in the ConfigMap cannot unmarshal
  into a `time.Duration`, and the pod crash-looped on first deploy. The failure was clean because
  config is validated at startup, which is the argument for validating it there.
- **A test that passed for the wrong reason.** The isolation test's *healthy* route was itself
  under-buffered, so it was shedding too; it only looked correct because the assertion was weak.
  Tightened to fail loudly if the control route sheds.
- **It asserted a load distribution I had not measured.** A draft included specific percentages for
  the type skew. A borrowed number is worse than no number, so I measured it — the table in §1 is
  from a real run against the live firehose.
- **It over-built the design document.** After several improvement passes the write-up had grown
  well past the requested length. Cutting it back was my call, not the agent's — it will keep
  adding as long as you keep asking.

## What I would do differently

`FILL — one or two honest lines. Something like: I would have written CLAUDE.md before the first
line of code rather than after the second correction, and I would have measured the event
distribution on day one instead of asserting it.`

## An honest summary

The agent was most valuable as an adversary and least valuable as an author. Every structural
decision in `DESIGN.md` is one I can defend and would defend differently under different
constraints; the sharpest additions came from asking it to attack the design rather than to produce
one.

The pattern in the override list is worth stating plainly: almost every real defect was found by
*running* the thing — against the live firehose, or in the cluster — and not by reading the code.
The agent's output is uniformly confident whether or not it has been verified, so the review that
matters is the one that executes it. Anything I could not explain did not ship.
