# AI Usage

I used Muse Spark for the initial product discussion and Codex for planning, implementation, testing, and review. In Codex, I used Matt Pocock's `grill-me`, `to-spec`, and `implement` skills at different stages.

## From the assignment to a PRD

I started with a plain conversation in Muse Spark about my understanding of the assignment: what the system needed to do, what mattered for the interview, and how much architecture was appropriate for the exercise. We went back and forth on microservices practices, especially service boundaries, database ownership, asynchronous communication, and failure handling.

That conversation became the first [PRD](prd.md). It gave me something concrete to question before asking an agent to write code.

## Challenging the PRD and writing the spec

I brought the PRD and original assignment PDF into Codex and used Matt Pocock's `grill-me` skill to question the decisions and find gaps. This helped turn vague expectations such as “prevent duplicates” and “recover crashed jobs” into specific guarantees, failure cases, and accepted limitations.

I then used `to-spec` to turn the refined PRD into the [implementation specification](spec.md). The PDF stayed part of the context so the plan could be checked against the actual assignment. This stage clarified choices such as database ownership, the dispatch boundary, retry cycles, cancellation, and what the tests needed to prove.

## Implementation in two rounds

I split the specification into two halves and used Matt Pocock's `implement` skill in Codex over two rounds. Each round covered its part of the spec through implementation and testing. The tests ran against deployed Docker services with real Postgres and RabbitMQ so they could exercise actual transactions and message delivery.

Implementation ran in the main thread, following the repository's tool mapping. The later adversarial interviews used a separate process with explicit subagent roles.

## Review, simplify, test, and fix

After implementation, I ran multiple regular Codex sessions with review prompts. These asked it to look for bugs, check the code against the assignment and spec, simplify unnecessary code, strengthen tests, and fix what it found. Reviews checked the code and traced what happened at each failure point.

This took several passes. Fixes included startup locking, idempotency-key cleanup races, status-event ordering, invalid JSON handling, shutdown behavior, and test-runner cleanup. Some failures came from the test harness itself, so those needed diagnosis too. [docs/implementation.md](docs/implementation.md) records failed runs, corrections, and the later passing runs.

## Mock interview

I used Codex as the assignment author in a mock interview. It asked concrete “what if” questions and checked my explanations against the implementation. The aim was to make sure I could explain the code and defend the tradeoffs myself.

While discussing two executions of the same dispatch reporting conflicting outcomes, I recognized that the dispatch token protected database state but did not select one physical executor. That led to the worker-owned Redis claims and saved-outcome replay described in [decision 18](DECISIONS.md#18-worker-owned-execution-claims-in-redis).

The change reduced duplicate execution without giving workers access to the API's database or making them poll the API. It also left an explicit limit: a crash after an external effect but before saving the outcome can still cause that effect to repeat.

## Adversarial interviews

After that discovery, I used adversarial AI interviews to look for further architectural weaknesses. One agent played the interviewer, another defended the implementation, and an independent orchestrator checked disputed claims against the code. The [saved prompt](adverserial_interview_prompt.md) defines ten question-and-answer rounds, independent research, and a read-only review.

The interviews found no major architectural issues. The agents checked proposed weaknesses against the code and allowed reasonable exercise constraints when the implementation supported the explanation.

## What helped most

AI was useful for turning the assignment into explicit contracts and for tracing failures across service boundaries. Two contributions mattered most: checking when messages could be acknowledged safely, and separating submission idempotency, logical attempts, physical execution, and external effects. Those distinctions shaped both the implementation and its tests.

The mock interview was also useful beyond preparation. Explaining a concrete failure scenario exposed a design gap that earlier code reviews had missed.

## What I had to fix

AI suggestions and generated code needed correction, particularly around concurrency:

- **Event order:** Publishing start before completion does not guarantee that the API applies them in that order after reconnects or replay. A final outcome for the current token now records its missing start and outcome in one transaction.
- **Duplicate execution:** A token check in Postgres prevents stale state updates; it does not stop two workers running the same attempt. Redis claims and retained outcomes address that gap within their documented limits.
- **Startup locking:** An advisory lock around schema creation did not prevent repeated DDL from conflicting with a running cleanup transaction. Later replicas now detect the committed initial schema and skip DDL.
- **Idempotency cleanup:** Looking up a key after a conflicting insert could race with cleanup releasing the old key. The lookup now locks the existing row and retries insertion if cleanup won the race.
- **Crash tests:** Early tests assumed broker redelivery would always beat the reaper. Both are valid recovery paths. Separate scenarios now test each mechanism, and job-specific pause points replaced timing-sensitive external hook installation.
- **Readiness and teardown:** A broker liveness check did not prove AMQP readiness, and diagnostic failures could bypass test cleanup. Readiness now checks the actual dependency path, and teardown runs even if collecting diagnostics fails.

## What AI struggled with

AI was better at proposing individual mechanisms than checking how they interacted. Database locks, broker acknowledgements, Redis leases, and retry counters can each look correct in isolation while leaving a gap between them. Concrete timelines and failure tests were more useful than broad requests to “make it reliable.”

It also needed repeated correction on what tests could establish. Stable mock IDs, clean audit logs, and passing tests do not prove exactly-once execution. The final design and documentation keep that distinction explicit.
