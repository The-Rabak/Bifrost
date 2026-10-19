# Design Decisions

The [assignment](PY-Backend-Senior-job-queue-Kit-Assignment.pdf) leaves the technology and several failure-handling details open. I chose a small API service and a separately scalable worker service, with Postgres for job state, RabbitMQ for messages, and Redis for worker execution records.

The main departures from the PDF are an extra `dispatched` state, workers reporting results through messages instead of writing to the API's database, and Redis to reduce duplicate execution. I also defined cancellation and manual-retry limits where the assignment leaves them unspecified. The reasons and costs are below. [README.md](README.md) covers usage; [implementation evidence](docs/implementation.md) records test runs.

## 1. Job Pickup Strategy

The API's scheduler reserves due jobs in a short Postgres transaction using `FOR UPDATE SKIP LOCKED`. It changes each job to `dispatched`, assigns a token for that logical attempt, and commits before publishing to RabbitMQ. Other schedulers skip locked rows and cannot reserve an already dispatched job.

The extra state records the gap between reserving work and a worker starting it. Leaving the job pending during that gap would allow repeated dispatch and misleading cancellation results. Calling it processing would suggest execution had started. `dispatched` makes that gap visible to clients.

The job row also records whether publication was confirmed and when an unconfirmed dispatch can be recovered. If the API crashes before recording confirmation, another scheduler republishes with the same token. Recovery itself uses an atomic reservation. Confirmed work stays in RabbitMQ while workers are offline.

This uses the job row as the record of work still needing publication, avoiding a separate outbox table. There is no transaction spanning Postgres and RabbitMQ: a crash after broker acceptance but before the database update can produce a duplicate message. The token and worker claims address different parts of that problem; neither removes the failure window entirely.

## 2. Worker Crash Recovery and Timeouts

Workers enforce a timeout around each handler. The API also runs a reaper that finds processing attempts past their deadline.

| Type | Handler timeout |
| --- | --- |
| Email / webhook | 15 seconds |
| Report | 60 seconds |
| Batch | 180 seconds |

These limits leave room above the assignment's normal mock delays while bounding a stuck handler. The API sets its recovery deadline when it accepts the attempt's start, allowing an extra five seconds by default. Progress updates do not extend it.

After a worker crash, RabbitMQ can redeliver the unacknowledged message with the same token. Redis allows takeover after the old execution claim expires. Separately, the reaper can abandon the attempt and schedule a new business attempt with backoff, or fail the job if its budget is exhausted. Both paths are possible; recovery does not depend on one always winning.

The reaper locks and checks the current attempt before changing it, so it cannot overwrite a completion that committed first. An abandoned token can no longer update job state. I chose fixed deadlines over heartbeats because these handlers have bounded runtimes. Recovery is eventual, and a disconnected worker may still be running when a new attempt starts.

## 3. Priority Queue Implementation

All four job types use one shared work queue, `q.jobs`. Every worker can run every handler. Adding capacity means scaling one worker service, without configuring a fleet for each type.

Priority applies twice: the scheduler selects higher-priority eligible jobs first, and RabbitMQ 4.3's quorum queue prioritizes waiting messages. Priorities range from 0 to 31, with 4 as the default. Worker prefetch is one, so a worker does not reserve a long backlog that the broker can no longer reorder.

The PDF's priority requirement needs a practical boundary: priority affects work still waiting for dispatch or delivery. It cannot interrupt a running job, reorder work already delivered, or bypass scheduling and retry delays. The tests demonstrate ordering by queuing eligible same-type jobs before starting workers. The shared queue also compares priorities across types, but there is no cross-type execution-order promise.

A shared queue gives no type its own capacity. Long batches can occupy all workers, and strict priority can starve low-priority jobs. Separate queues or fairness rules would be justified if workload isolation became a requirement.

## 4. Retry Backoff Strategy

The API owns business retries and stores their eligibility times in Postgres. Workers report outcomes; they do not independently decide to schedule another attempt. This keeps one authority for retry budgets and avoids combining broker redelivery with a second, hidden retry policy.

The default matches the assignment's three total attempts:

| Attempt | Earliest eligibility |
| --- | --- |
| 1 | Immediately, or at the requested schedule |
| 2 | 30 seconds after the first failure |
| 3 | 120 seconds after the second failure |

I also allowed budgets of one to five attempts, keeping three as the default. This is an extension for callers that need a smaller or larger budget; further delays are 480 and 1,920 seconds. Transient failures and timeouts use the budget. Permanent failures end the cycle immediately because repeating the same invalid operation would not help.

The PDF asks for manual retry without defining its budget. I allow one fresh cycle on a failed job, with the same payload and attempt limit. This supports recovery without allowing unlimited retries through the endpoint. With the default, that means at most six logical attempts across both cycles. A second manual retry returns 409.

Manual retry preserves the job ID, lifetime attempt count, first start time, and audit history. It clears current output, error, progress, and attempt timing, then becomes immediately eligible. Automatic retries preserve priority and keep the latest error visible while waiting.

Broker redelivery retains the same token and does not consume another logical attempt. Attempt counters therefore describe accepted business attempts, not every physical handler invocation.

## 5. Database Ownership and Worker Coupling

The PDF's diagram shows workers writing directly to the database. I changed that boundary: the API service owns all Postgres reads and writes, including scheduling, retries, results, recovery, and audit history. Workers receive work and publish status events through RabbitMQ. They have no database credentials and make no API calls to request execution approval.

This follows service-owned data: workers depend on a message contract rather than the API's tables. It also keeps lifecycle rules and audit writes in one place. Redis belongs to the worker service; the API has neither its configuration nor access to its private network.

The cost is eventual consistency. A handler may have finished while an API query still shows an earlier state because its event is waiting to be applied. Processing can continue during an API outage, but persistent status and business retries wait for the API to return.

I kept scheduling, status consumption, recovery, and cleanup inside the API service because they all manage the same job lifecycle and database. Splitting each loop into a microservice would add deployments and coordination without a separate business responsibility. The cost is that those loops share the HTTP process's restarts and cannot scale separately. Likewise, the four mock handlers share one worker service; separate email or report services would make sense only if they needed independent ownership, dependencies, or scaling.

## 6. Delivery and Execution Guarantees

Work and status messages are durable. Publishers wait for broker confirmation, and consumers acknowledge messages explicitly. A worker confirms its start event before running the handler, saves the final outcome in Redis, confirms that outcome's publication, and only then acknowledges the work. A reported business failure is also acknowledged; the API decides whether to retry it.

The guarantee is at-least-once delivery. Publisher confirmation means the broker accepted a message, not that the API applied it. Lost connections and uncertain publication can create duplicates.

Dispatch tokens stop stale events from changing current job state. Redis claims reduce duplicate execution of the same attempt and allow saved outcomes to be replayed. These are separate protections. Lease expiry, Redis data loss, outcome expiry, or a new business attempt can still repeat execution. Real external effects need their own idempotency, as described in decision 17.

## 7. Cancellation Cutoff and Scheduling

Cancellation is allowed while a job is scheduled or pending, including during retry backoff. Cancellation and scheduler reservation lock the same row: either cancellation commits and prevents dispatch, or dispatch commits and cancellation returns 409.

This makes the cutoff earlier than worker execution. Once a job is `dispatched`, it cannot be cancelled, even if it is still waiting in RabbitMQ. I chose this explicit boundary to avoid broker-message deletion and a worker interruption protocol. The PDF does not define how to handle that in-between period.

Future jobs stay in Postgres until due. The same eligibility timestamp handles initial scheduling and retry backoff, so no delayed-message plugin or extra scheduling queue is needed. Priority never overrides it. A manual retry is a new request to run now and therefore ignores the original schedule. Dispatch happens at or after eligibility, on a scheduler poll.

## 8. Status Processing and Audit Consistency

One active consumer applies `q.job-status` events sequentially. RabbitMQ's Single Active Consumer feature lets another API replica take over if it disconnects. Work consumers remain concurrent.

Each execution event carries its attempt token. The consumer locks the job row, checks that token and state, and commits lifecycle changes with their audit records before acknowledging the event. Duplicate starts count once, stale outcomes cannot overwrite a newer attempt, and progress cannot decrease within an attempt.

Workers publish events in order, but reconnects and replay can still deliver a final outcome before its start has been applied. A valid current-token outcome therefore records the missing start and final result in one transaction. Rejecting it simply because the job is still dispatched would lose useful work.

Sequential consumption keeps this small and easy to reason about, at the cost of a status-processing bottleneck. State guards remain necessary because scheduling, cancellation, and recovery run concurrently with that consumer.

## 9. Submission Idempotency and API Contracts

A unique Postgres constraint protects optional idempotency keys, including simultaneous submissions. A new job returns 202. A repeated key returns 200 with the existing job's current state and leaves its payload, priority, schedule, and retry budget unchanged. Returning the original job even when the new payload differs follows the PDF's instruction to return the existing job.

Keys remain protected while jobs are active. Cleanup may release a key only when both job creation and terminal completion are over 30 days old, exceeding the required 24 hours. This favors safe deduplication over prompt key reuse. Submission idempotency prevents duplicate job records; it does not prevent repeated execution.

I added strict per-type validation and bounded inputs: 64 KiB payloads, 1,000 batch items, 200-character keys, and at most 100 results per page. Timezones are required for schedules. JSON validation rejects non-finite numbers, NUL characters, and excessive nesting because these can otherwise pass HTTP validation and fail later in storage or message parsing.

These limits are application choices rather than PDF requirements. They reject unusable work early and keep responses manageable. Errors use a consistent shape, with 404 for missing jobs and 409 for actions the current state does not allow. Stable sort tie-breakers make pagination predictable, though offset pages can still shift as new jobs arrive.

## 10. Mock Handlers and Progress

The handlers keep the assignment's simulated delays and webhook's 80% success rate. They make no external calls. Email IDs and report URLs derive from the job ID so mock results are easy to inspect and compare.

Batch handlers report progress at roughly ten-percentage-point intervals. Reporting every item made broker round trips dominate execution time in testing. Progress resets for a new attempt, reaches 100 on success, and an empty batch returns an empty summary.

Explicit test mode provides deterministic failures, delays, and pauses around real operations. This lets tests exercise retries and crash windows without relying on random failures or changing a submitted payload between attempts. These controls are rejected in ordinary operation. Stable mock IDs are a convenience, not evidence of exactly-once effects.

## 11. Background Failures and Graceful Shutdown

The services use `asyncio` because the mock workload waits on timers and network I/O. Each worker handles one outstanding delivery; concurrency comes from more worker processes. Real CPU-heavy reports would need a different execution model to avoid blocking the event loop.

Expected dependency failures reconnect with bounded backoff. Unexpected programming errors terminate the service visibly so the restart policy can act. Silently restarting every failed task would hide defects.

On shutdown, workers stop accepting work, finish their current bounded handler, publish its saved outcome, and acknowledge before exiting. Compose allows 220 seconds, longer than the longest handler timeout plus publication time. The API stops new reservations and finishes work already in progress. If it exits first, durable worker events wait in RabbitMQ.

A forced kill follows crash recovery and may repeat execution. Graceful shutdown can take several minutes for a long batch; that is the cost of finishing current work.

## 12. Dead-Letter Policy

The work queue has a delivery limit of five and routes rejected or exhausted deliveries to `q.jobs.dlq`. An API-side consumer marks an identifiable current dispatched/processing job failed and records why. A stale dead letter cannot fail a newer attempt or overwrite a terminal result.

This adds the PDF's optional poison-message handling without an automatic replay system. If status publication fails but the work channel remains usable, the worker can reject the delivery into the DLQ. If the connection is gone, ordinary unacknowledged-message recovery applies instead.

The status queue is durable and classic, without the work queue's delivery limit, so repeated database-outage redeliveries do not exhaust it. Malformed status messages are discarded so useful events can continue.

The simple policy can fail a valid job after an infrastructure fault and require its manual retry. Dead-letter transfer is not guaranteed lossless in this setup. A production replay tool would need to distinguish poison work from recoverable infrastructure failures.

## 13. Security and Rate Limits

The PDF does not require authentication. I added a shared bearer token for job routes and per-process submission/retry limits to provide basic access and load controls. Credentials come from the environment, application containers run as non-root, and only the API has a host port, bound to loopback. Health stays unauthenticated for local readiness checks.

Workers authenticate to RabbitMQ with broker credentials. They do not need an HTTP token because they never call the API. Local services share one broker identity; database credentials belong only to the API.

This keeps setup small. There are no users, scopes, JWT service, per-service broker identities, or global rate-limit store. Adding API replicas increases the effective rate limit. Those are deliberate local-exercise limits, not production access control.

## 14. Logs and Retention

I included the recommended job log table because retry and crash behavior is difficult to explain from a final status alone. Lifecycle changes and their audit entries commit in the same database transaction. Structured stdout logs carry job, type, cycle, attempt, and token context where applicable.

An hourly cleanup removes old audit logs and releases eligible terminal-job idempotency keys in batches of 500, using a 30-day threshold. Small batches avoid one large cleanup transaction competing with normal work.

Job rows remain indefinitely, and old logs are not archived. This is basic housekeeping, not a complete retention policy or a guarantee of bounded storage. I kept two tables, jobs and logs, rather than adding an event store or a separate attempt-history subsystem.

## 15. Deployment and Health

Docker Compose runs the API, scalable workers, Postgres, RabbitMQ, and Redis. `compose.yaml` is the Compose filename used here instead of the PDF's example `docker-compose.yml`; the documented `docker compose` command discovers it. Named volumes preserve database, broker, and execution data across ordinary restarts.

Postgres 16 provides transactions, constraints, JSON storage, and row locking. RabbitMQ provides durable delivery, acknowledgements, competing consumers, priority, and status-consumer failover. The broker is pinned to 4.3.0 to use its quorum-priority support and keep local runs reproducible. One database and one broker keep local setup manageable. A quorum queue on one broker does not provide replicated availability or protect against permanent volume loss.

Multiple API replicas coordinate through database locks and broker-managed status-consumer failover. Workers scale independently. There is no load balancer in this exercise; the README shows separate local ports for demonstrating API replicas.

Health checks verify database and AMQP connectivity and report job counts, queued/unacknowledged work, consumers, status backlog, and DLQ depth. Required dependency failure returns 503. Missing management statistics or zero workers is reported as degraded. The API does not probe private worker Redis; a Redis outage removes work consumers and is visible indirectly. A connected consumer still does not prove its handler is healthy.

## 16. Technology, Code Structure, and Verification

I used Python 3.12 with FastAPI and Pydantic for HTTP contracts and validation, asyncpg for explicit Postgres transactions, and aio-pika for RabbitMQ. Explicit SQL makes locking and transition conditions visible, which matters more here than an ORM's convenience. Using RabbitMQ directly also makes acknowledgement and retry behavior visible for the assessment, at the cost of writing recovery code a task framework would otherwise supply. The initial schema is created atomically under an advisory lock; later replicas skip DDL. Versioned migrations would be needed once an existing deployment's schema changes.

API and worker share a Dockerfile and Python package, with separate entrypoints and credentials. This avoids separate packaging for the exercise while keeping runtime responsibilities separate. Shared message models reduce contract drift but mean incompatible contract changes need coordination.

Modules separate HTTP, persistence, lifecycle rules, messaging, handler execution, and supervision. I kept concrete functions and small adapters instead of generic repositories or a handler plugin framework. There are four known handlers and one implementation of each dependency.

The PDF asks for at least six meaningful tests. I went further with deployed Docker tests because the hardest promises involve actual transactions, broker delivery, crashes, and restarts. Tests use real dependencies, deterministic mock controls, isolated Compose projects, and bounded polling. They cover functional behavior as well as cancellation races, interrupted publication, duplicate/stale events, retry limits, reaping, shutdown, and persistence. Focused runner tests cover cleanup failures that are cheaper to isolate outside Docker.

This costs more runtime and harness code than a small unit suite. The latest recorded full run passed 81 deployed tests plus four runner tests; the exact evidence is in [docs/implementation.md](docs/implementation.md). Those results support the tested scenarios, not every possible distributed failure. Attempt counters and audit entries alone cannot prove one physical execution.

## 17. One Thing I Would Do Differently With More Time

Before adding real email, webhook, or storage calls, I would define idempotency for each external effect and test a crash after the effect happens but before its outcome is saved.

For example, a receiver that supports idempotency keys could use the job ID as a stable operation key across business attempts. Where that is unavailable, the handler needs a way to reconcile an unknown outcome before retrying. More queue machinery cannot solve that ambiguity by itself.

## 18. Worker-Owned Execution Claims in Redis

The mock interview exposed a gap: two deliveries with the same dispatch token could both execute and report different outcomes. Postgres guards would accept only one result, but that did not prevent the duplicate execution. I added Redis even though the PDF does not require another store.

Redis holds temporary execution ownership and saved outcomes, not authoritative job state. Workers own it privately, preserving the API's Postgres boundary. This is a small coordination need that Redis can handle atomically without an admission HTTP call or another coordinator service.

Each key includes a stable service namespace, job type, dispatch token, and job ID. Replicas share the namespace; a new business attempt gets a new token and therefore cannot accidentally reuse an earlier failure.

1. A worker claims a missing key atomically with a unique owner ID and an expiry of the handler timeout plus 15 seconds. No heartbeat is needed for the bounded execution window.
2. Duplicate deliveries wait without acknowledging their work. They either find a saved outcome or take over after the old claim expires. Keeping them unacknowledged avoids repeatedly requeuing them into RabbitMQ's delivery limit.
3. The owner publishes its start, runs the handler, and atomically replaces its own claim with the final outcome. A short Lua comparison prevents an expired owner from overwriting a successor. If saving fails because ownership was lost, that execution cannot publish its result.
4. The saved success or failure stays for 24 hours. Redelivery replays that same outcome before acknowledging, rather than running the handler again. Deleting a lock immediately after execution would lose this protection.

Redis persists to a named volume with AOF, fsync on every write, and no eviction. The extra disk writes are acceptable for this workload. If Redis is unavailable, workers stop consuming rather than execute without a claim. There is one concrete storage adapter and one execution service, not a general locking framework.

This is a single Redis instance, not a highly available lock service. Expiry permits takeover even if the old process is still running. Claims apply per attempt, and the API can issue a new attempt while an old outcome is delayed. Lost Redis data, expired outcomes, and a crash between an external effect and saving its result still permit repetition. In separate real services, each service would own its execution store and credentials; a key prefix alone would not enforce that boundary.
