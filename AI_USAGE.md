# AI usage

Codex assisted with interpreting the supplied specification, selecting concrete contracts, writing the Python service and SQL, authoring deployed Docker acceptance tests, diagnosing failures, and reviewing the resulting changes. No implementation agents were delegated: the supplied AGENTS.md mapping requests sequential work in the main thread.

Useful contributions include keeping workers database-free, separating logical attempts from physical executions, using guarded transactions with audit inserts, preserving terminal-before-start outcomes, isolating Compose projects, and using real broker/container failure boundaries rather than mocks.

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
