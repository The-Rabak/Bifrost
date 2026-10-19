# AI usage

The interview review exposed that dispatch-token guards alone allowed duplicate physical executions to report conflicting outcomes. The follow-up adds a small Redis adapter and execution service: expiring owner claims, compare-and-set outcome storage, and replay before broker acknowledgement. Workers retain no access to the API's Postgres and do not poll the API. Tests distinguish saved-outcome replay from the remaining effect-before-outcome crash window; Redis is not presented as exactly-once execution.

We used a mock interview to challenge the architecture against the original assignment and actual code. Codex played the assignment author, asked concrete failure-scenario questions, and evaluated the user's explanations against the implementation. The user recognized the architectural weak point while discussing two executions of the same dispatch reporting conflicting outcomes. Challenging that behavior led to the Redis execution-coordination change above. This made the interview useful for finding a design problem as well as assessing understanding of the code.

Inspired by that discovery, the user proposed a repeatable adversarial interview between an interviewer subagent and an interviewee subagent, coordinated by an independent orchestrator. The [saved next-session prompt](adverserial_interview_prompt.md) specifies exactly 10 question-and-answer rounds, GPT-6.1-SOL with high reasoning for all participants, independent code research, and no repository or infrastructure changes. The final report must preserve every question and answer and record only substantiated architectural issues worth considering for refactoring. Properly explained exercise constraints are valid defenses, and finding no refactoring candidates is acceptable. This subagent process has been prepared but has not yet been run; the discovery described above came from the human/AI mock interview.

Codex assisted with interpreting the supplied specification, selecting concrete contracts, writing the Python service and SQL, authoring deployed Docker acceptance tests, diagnosing failures, and reviewing the resulting changes. No implementation agents were delegated: the supplied AGENTS.md mapping requests sequential work in the main thread.

Useful contributions include keeping workers independent of the API's Postgres, separating logical attempts from physical executions, using guarded transactions with audit inserts, preserving terminal-before-start outcomes, isolating Compose projects, and using real broker/container failure boundaries rather than mocks.

Incorrect initial suggestions/implementations and corrections:

- A broker `ping` health check only established Erlang liveness, allowing API startup before AMQP readiness. The final setup uses a cheap container TCP probe plus API HTTP readiness that checks the real AMQP connection and queue; connection startup/recovery remains explicit.
- Running a RabbitMQ health check as root could create the Erlang cookie with the wrong owner during startup. The broker now runs as its own non-root user.
- Giving the API only an internal Docker network prevented published HTTP access on this Docker installation. The API now also joins an edge network; dependencies retain only the internal network and no published ports.
- Reconnecting status events can put a final outcome before its start. Assuming worker publication order alone solves this would lose valid results. Current-token outcomes now imply their missing start inside the same state/audit transaction.
- A claim token does not provide exclusive execution: uncertain publication and worker redelivery can invoke a handler again. Documentation and fault tests distinguish those windows from ordinary competing-consumer delivery.

The implementation evidence records observed test results and remaining limitations. Generated test names, stable mock IDs, or passing static checks are not evidence of exactly-once execution.

Concrete concurrency mistakes found during implementation were also corrected:

- Treating an advisory lock around repeated startup DDL as sufficient for live replica startup overlooked lock conflicts with the already-running cleaner. Initial DDL now commits atomically once; later replicas detect the committed schema and avoid DDL locks.
- A crash test initially assumed RabbitMQ redelivery would beat reaping. The accepted contract allows either recovery mechanism to win. The redelivery-specific scenarios now pause the real reaper poll before database acquisition, preserving their strict one-token assertions; separate deployed tests exercise reaping, continuing progress, abandoned tokens, and completion races.
- Global pause hooks could stop a stale queued delivery from a previous scenario instead of the intended job. A subsequent two-second scheduling window still raced with slow Docker control calls. Fault tests now self-arm job-specific pauses from validated immutable test controls; no external installation deadline is needed.
- Looking up an existing idempotency key after `ON CONFLICT DO NOTHING` could race with old-key cleanup. The lookup now takes a key-share lock and retries insertion if the eligible old key was released between statements.
- Follow-up review exposed NUL strings that PostgreSQL cannot persist, JSON nesting accepted by HTTP but rejected inside the worker envelope, scheduler reservations continuing during shutdown, and diagnostic failures bypassing test-stack teardown. Shared validation and the existing shutdown/cleanup paths now handle these cases, with deployed regressions and four focused host-runner fault checks. The Docker rerun also exposed a failover test polling consumer removal before the sampled active flag had updated; its bounded poll now waits for both required conditions.
