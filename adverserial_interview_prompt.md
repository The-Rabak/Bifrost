# Adversarial architectural interview prompt

Configure the next session as **`gpt-6.1-sol` with `high` reasoning**, then use the prompt below. The orchestrator's model must be selected in the session configuration; this prompt does not change its model.

---

Run a read-only architectural interview of this repository, using two subagents coordinated by you.

Repository: /mnt/d/The Rabak/bifrost

The purpose is to test architectural explanations against the actual implementation and identify any glaring weaknesses that deserve potential refactoring. Finding no refactoring candidates is a valid outcome.

## Models and roles

All three participants must use gpt-6.1-sol with high reasoning:

- You: orchestrator and independent adjudicator.
- Interviewer: plays the author of the original interview assignment.
- Interviewee: plays the engineer defending this implementation.

Create two persistent subagents and reuse them throughout the session. Where the tool supports explicit overrides, use:

`model="gpt-6.1-sol", reasoning_effort="high", fork_turns="none"`

Give each agent a self-contained task, repository location, shared rules, and document references. Both must have full read access to the repository and independently gather their own context.

This request explicitly authorizes subagents, superseding any compatibility mapping that would replace subagent work with sequential main-thread simulation. Conduct the interview rounds sequentially. Do not substitute models or simulate either participant yourself. If the required model or subagent capability is unavailable, report that limitation.

## Read-only boundary

No participant may modify repository files, infrastructure, application data, configuration, or Git state.

Use read-only inspection: search files, read code and documents, inspect Git history/diffs, and examine existing test results and artifacts. Consult primary external documentation when needed to verify a technical claim.

Do not run tests, builds, package installation, formatters, deployment commands, fault injection, or application startup. These can write files or mutate state. Do not create notes, reports, tickets, or temporary files. Keep the transcript and findings in conversation memory and return them in the final response.

Record the initial HEAD, working-tree status, and in-memory fingerprints of reviewed files. Preserve pre-existing changes. Compare at the end and report observed differences without reverting anything. Never print secrets.

## Initial context

Before round 1, all participants independently inspect:

- PY-Backend-Senior-job-queue-Kit-Assignment.pdf
- prd.md
- spec.md and any actual planning artifacts
- DECISIONS.md
- README.md
- Application code, schema, deployment configuration, and tests
- docs/implementation.md, docs/acceptance.md, and docs/review.md

Inspect the current implementation, including the worker-owned Redis execution registry. Do not assume an earlier version of the architecture still applies.

Distinguish:

1. Original assignment requirements.
2. Additional promises introduced by the PRD/specification.
3. Accepted exercise constraints and tradeoffs.
4. Actual implemented behavior.
5. Evidence provided by tests and recorded executions.

Documentation is evidence of intent, not proof of implementation. A test name is not proof of its assertions. Historical passing results do not establish that the current source was tested unless the recorded source identity matches.

## Interview protocol: exactly 10 rounds

Each round contains one substantive interviewer question and one interviewee answer. Complete exactly 10 rounds, without asking the human to participate.

For every round:

### 1. Interviewer researches

Reinspect the relevant code, callers, transaction boundaries, message paths, tests, and documented decisions before formulating the question.

Ask one focused architectural question, preferably grounded in a concrete execution or failure scenario. Explore mechanisms, consequences, and tradeoffs. Avoid unrelated multipart questions and generic technology trivia.

Send the orchestrator the question and relevant source references. Do not supply an answer key for forwarding to the interviewee.

### 2. Orchestrator forwards

Forward the question unchanged to the interviewee. Include relevant prior settled conclusions, but do not coach the answer or present the interviewer's expected conclusion.

### 3. Interviewee researches and answers

Independently inspect the implementation before answering. Trace the actual behavior rather than reciting intended design.

Explain the mechanism, guarantees, failure boundaries, and rationale. Cite repository paths and line numbers for central claims. Clearly separate current behavior from hypothetical production improvements.

Defend legitimate tradeoffs honestly. Admit a defect or uncertainty when warranted. Do not invent missing mechanisms or defend every choice automatically.

### 4. Interviewer evaluates

Give the interviewer the answer. Have it check the claims against the source and identify:

- What the answer establishes.
- Any factual mistakes or unanswered assumptions.
- Whether the scenario reveals an implementation problem, an accepted tradeoff, or a mistaken premise in the question.

This evaluation must not introduce an additional interview question. Any follow-up consumes the next numbered round.

### 5. Orchestrator adjudicates

Independently verify disputed claims and any proposed weakness. Read the relevant source yourself.

Correct a mistaken interviewer premise as readily as a mistaken interviewee explanation. Agreement between agents is not independent proof.

Record a short judgment for the round and update the in-memory transcript and candidate ledger. An incorrect answer alone is not an architectural defect.

### 6. Select the next question

Coordinate the next question using what remains unresolved or unexplored. Pursue a disputed seam in the next round when useful; otherwise move on.

Avoid repeatedly challenging a settled tradeoff without new evidence.

## Areas to consider

Maintain a coverage map rather than a rigid questionnaire. Relevant areas include:

- Service and data ownership.
- Submission idempotency versus execution/effect idempotency.
- Database-to-broker publication and recovery.
- Worker claims, lease expiry, outcome replay, and stale owners.
- Logical attempts versus physical executions.
- Retry budgets and failure classification.
- Cancellation races.
- Scheduling and priority guarantees.
- State transitions, status ordering, progress, and audit consistency.
- Dependency outages, shutdown, persistence, and operational evidence.

Choose the highest-information questions for this repository. Ten rounds do not require ten distinct problems.

## Strict threshold for refactoring candidates

Record a potential refactoring candidate only when all of these hold:

- There is a concrete, reachable code path or credible execution interleaving.
- It violates an assignment requirement, a promised invariant, or a materially important architectural boundary.
- The consequence matters within this exercise's intended scope.
- The finding survives checking the implementation, tests, documented constraints, and the strongest reasonable defense.
- You can explain a proportionate improvement and its cost without implementing it.

Explicit documentation of a limitation does not excuse violating a mandatory assignment requirement. Conversely, production aspirations do not automatically become exercise requirements.

Treat a constraint-based defense as valid when it explains:

- Why the simplification is appropriate here.
- What behavior the current code actually guarantees.
- What limitation remains.
- How a real production service would address that limitation if needed.

No finding quota. Do not inflate severity, manufacture recommendations, demand impossible guarantees, or recommend infrastructure merely because production systems sometimes use it.

Keep unresolved suspicions separate from confirmed candidates. A suspicion without sufficient evidence must not appear as a recommended refactor.

## Final deliverable

Return the complete interview in this order:

### 1. Scope and verification limits

State the participants' model configuration as actually known, the reviewed revision/working-tree context, and that the process used read-only inspection rather than fresh execution evidence.

### 2. All 10 rounds

For each round include:

- Interviewer's exact question.
- Interviewee's exact answer, including source references.
- A concise orchestrator judgment, incorporating relevant interviewer critique.
- Disposition: explained tradeoff, no issue demonstrated, confirmed candidate, or unresolved.

Preserve the questions and answers verbatim. Do not replace them with summaries.

### 3. Potential refactoring candidates

Include only candidates meeting the threshold above. Deduplicate findings across rounds.

For each:

- Title and originating round(s).
- Concrete scenario and affected source locations.
- Violated requirement or invariant.
- Practical consequence within this exercise.
- Why the constraint-based defense was insufficient.
- Smallest reasonable improvement and its tradeoffs.
- Confidence and priority, justified by evidence.

If none qualify, say:

> No glaring architectural issues warranting refactoring were established in these 10 rounds.

### 4. Accepted tradeoffs and unresolved questions

Briefly preserve important explanations and remaining uncertainty. Keep hypothetical production improvements clearly separate from refactoring recommendations.

### 5. Read-only verification

Report whether the reviewed file fingerprints and Git state changed during the session. Do not attribute externally observed changes to a participant without evidence.

Finish after the report. Make no changes and begin no implementation.
