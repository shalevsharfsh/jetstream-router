# Agent setup and prompts

The brief asks what I used, how it was configured, the prompts behind the real
decisions, and where the agent helped versus where I overrode it. Straight
answers below.

## Setup

| | |
|---|---|
| Tool | Claude Code (CLI), Opus 5, plan mode for the design phase |
| Session shape | One long session: research → plan → build → measure → document |
| MCP servers | None used for this project |
| Rules files | No `CLAUDE.md` in this repo. My global setup carries house conventions from another codebase (structured JSON logging, `pytest` with happy/rainy test naming, ruff) which visibly shaped the output here |
| Sub-agents | Two: one web-research agent on the company and on prior public solutions to this exercise; one codebase-exploration agent over my existing Python backend to mirror its conventions |
| Verification | `pytest` (50 tests), `ruff`, a real `kind` cluster, and the live firehose. Nothing in this repo is claimed working on the strength of the model saying so |

**Division of labour, plainly:** the agent wrote essentially all of the code and
the first draft of these documents. I set the objective, made the architectural
calls, rejected several of its recommendations, and directed the measurement
work that changed the design. I have read every file and can defend every
decision in it — which is the point of the follow-up conversation, and the
standard I held this to.

## The prompts that did the real work

### 1. The stack decision — where I overrode it

This is the clearest example, so I will lead with it. The agent argued, at
length and unprompted, *against* what I asked for:

> "The brief names Go + Kubernetes, and this round is explicitly judged on
> design reasoning. Serverless is a genuinely poor fit for the actual problem:
> the core is a long-lived WebSocket holding a `time_us` cursor across
> reconnects. […] Deviating from a named stack is allowed, but that clause is
> for genuine architectural conviction, not tooling preference."

I overrode it and chose Python. I was not persuaded that following the spec was
worth spending my time budget on an unfamiliar language when the graded
substance — routing, backpressure, isolation, delivery semantics — is
language-independent.

But the agent was *half right*, and the half it got right changed the design.
Its point that the WebSocket tap cannot be serverless was correct, and rather
than dropping it, I made it the spine of the argument: the design now has
exactly one stateful component and is explicit that anything claiming to be
"fully serverless" here is hiding the tap. Later research also showed the
agent's premise was factually wrong in one respect — it warned this would cost
me credibility "on a Go role", when Zenity's backend is primarily Python. It
corrected itself on that when the evidence arrived.

Net: I kept my decision, adopted its strongest objection as a section of
DESIGN.md, and discarded its risk assessment once it turned out to be based on
a wrong assumption.

### 2. Sampling the wire before writing the classifier

> "Before writing the classifier, connect to the live Jetstream and confirm the
> real payload shapes — especially what a `delete` actually carries."

The most valuable instruction I gave. It produced three findings that changed
the code: deletes carry no `record` (so referential cleanup is impossible
without an index — now a documented limitation instead of a bug), a ~106×
volume skew across paths (the empirical case for independent scaling), and real
event shapes for the test fixtures. Writing the classifier from the PDF's
example event would have produced something that looked right and was wrong.

### 3. Reconciling "serverless" with "must run on kind"

> "The brief hard-requires a runnable local kind deploy, and a reviewer can't
> run a Lambda stack without an AWS account. How do we deliver both?"

The agent proposed three options; I chose KEDA locally plus a real, synthesising
CDK stack as the AWS target. This is the decision I would most expect to be
challenged on, and the reason I took it is that it makes the serverless claim
checkable rather than rhetorical.

### 4. Measuring instead of asserting

> "Watch the graph worker's replica count over two minutes and tell me whether
> it's actually cycling."

This is what caught the KEDA flapping (DESIGN.md §4). The agent had written
`minReplicaCount: 0` for the graph path and described it in a comment as the
"genuine scale-to-zero case" — plausible, well-argued, and wrong. Only watching
it run revealed the ~2-minute oscillation, and only then did the sharper point
surface: a 60-second burst detector that scales to zero can sleep through the
window it is supposed to be watching. That finding is now one of the strongest
paragraphs in the design, and no amount of prompting would have produced it
without running the thing.

### 5. Adversarial review of its own output

> "`collection` is user-controlled — check that against the cardinality warning
> you wrote in the metrics module."

The agent had written a docstring warning against unbounded metric labels and
then, forty lines later, used a user-extensible field as a label. Asking it to
check its own work against its own stated principle fixed it. I found this
class of error more often than logic errors: the code was internally
inconsistent rather than wrong in isolation.

## Where the agent was strong

- **Breadth-first plumbing.** Manifests, Dockerfile, Makefile, the consumer
  loop's ack/nak/DLQ semantics, the fake WebSocket server for tests. Correct
  and fast.
- **Failure-path completeness.** Graceful drain on SIGTERM, jittered backoff,
  publish-before-ack ordering in the DLQ path, separating liveness from
  readiness. It reached for these without being asked.
- **Catching its own bug via a test it wrote.** It added a case for a keyword
  containing `+`, which failed, which exposed that `\b` cannot match next to
  punctuation — so the boundary check became `(?<!\w)…(?!\w)`. A real bug found
  by a test written in the same breath as the code.

## Where it needed overriding

- **Plausible-but-unvalidated defaults.** `minReplicaCount: 0` is the headline
  example: confidently commented, superficially reasonable, wrong under
  measurement. Its comments are written with equal conviction whether or not
  anything has been verified, which is the single most important thing to know
  when working this way.
- **Naive first implementations.** Substring keyword matching shipped and ran
  against live data before anyone noticed `ai` matching *said*. It took real
  output to see it.
- **Alert/metric confusion.** It initially emitted an "alert" per deletion —
  roughly six per second. Distinguishing "a human should look at this" from
  "count this" was a correction I had to make.
- **Research that needed checking.** Its first research pass misattributed
  several competitors' security research to Zenity. It caught and corrected this
  on a second pass when asked to verify sources. I would not have used the first
  version.
- **Test flakiness.** Its first reconnect test passed ~60% of the time due to a
  race. I had it replace the timing assumption with an explicit gate rather
  than accept an intermittent test.

## Honest summary

The agent moved perhaps 4–5× faster than I would have on the plumbing, and was
roughly at my level on failure-handling patterns. It was consistently weakest
exactly where it sounded most confident: unverified constants and first-pass
implementations, both delivered with the same assured comments as the parts that
were correct.

The work that made this design mine was choosing the stack and owning that
trade-off, deciding where the fan-out boundary goes, and insisting on measuring
the things the agent asserted. Every finding in DESIGN.md §6 and the KEDA
correction in §4 came from running the system, not from prompting it.
