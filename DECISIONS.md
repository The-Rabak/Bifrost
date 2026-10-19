# Design Decisions

These choices explain the departures from the assignment, the service boundaries, and the main tradeoffs.

## 1. Job Pickup Strategy

The scheduler reserves due jobs with `FOR UPDATE SKIP LOCKED`, assigns an attempt token, and commits `dispatched` before publishing. The extra state separates reserved work from running work and prevents competing schedulers from claiming it again. Locks are not held during network calls.

Publication confirmation and recovery timing live on the job row, avoiding a separate outbox. Unconfirmed dispatches are atomically reserved for republication with the same token; confirmed backlog stays in RabbitMQ. A crash after broker acceptance but before recording confirmation can duplicate messages.

## 2. Worker Crash Recovery and Timeouts

Handlers time out after 15 seconds for email/webhook, 60 for reports, and 180 for batches. Bounded execution avoids heartbeats. The API reaper uses the API-accepted start plus that timeout and five seconds; progress cannot extend it.

RabbitMQ may redeliver the same attempt, or the reaper may abandon it and apply the retry policy. Locked state checks protect committed completions, and abandoned tokens cannot update successors. Recovery is eventual; a disconnected worker may still run when a new attempt starts.

## 3. Priority Queue Implementation

One shared quorum queue lets every worker handle every type. There is no reserved capacity per type, so batches can occupy all workers.

Priority applies in due-job selection and broker delivery: 0–31, default 4, prefetch one. It favors eligible waiting work without preempting delivered work or bypassing scheduling/backoff. Tests establish same-type ordering, not cross-type execution order. Low-priority starvation is possible; there is no fairness policy.

## 4. Retry Backoff Strategy

The API owns business retries in Postgres. Defaults match the PDF: three total attempts, with 30- and 120-second delays after failures. Optional budgets of 1–5 extend delays to 480 and 1,920 seconds. Transient failures and timeouts retry; permanent failures stop. Priority and the latest error survive backoff.

The PDF leaves manual-retry limits open. One fresh cycle keeps the same payload and budget: up to six logical attempts by default. It preserves identity, lifetime history, and first start, clears current output/error/progress, and runs immediately. A second manual retry returns 409.

Broker redelivery keeps the token without consuming another logical attempt. Counters do not count every physical execution.

## 5. Database Ownership and Worker Coupling

Unlike the PDF's diagram, only the API accesses Postgres. It owns lifecycle rules and audit history. Workers use RabbitMQ without database credentials or API approval calls. Redis belongs privately to workers.

This couples services through messages rather than tables. Status is eventually consistent, and business retries wait for API recovery.

Scheduling, recovery, and cleanup stay with the API because they manage the same data. They share its restarts and cannot scale separately. Handlers share one worker service; separate services would need independent ownership or scaling to justify them.

## 6. Delivery and Execution Guarantees

Messages are durable. Workers confirm the start event, execute, save the outcome in Redis, confirm its publication, then acknowledge work. Business failures are acknowledged too; the API decides retries.

Delivery is at least once. Broker confirmation does not mean database application. Tokens protect state; Redis reduces duplicate execution. Expired leases/outcomes, Redis data loss, and new attempts can repeat execution. External effects need their own idempotency.

## 7. Cancellation Cutoff and Scheduling

Cancellation and dispatch lock the same row. Cancellation succeeds only from scheduled/pending, including backoff. Committed reservation makes cancellation return 409, even before execution. This avoids message deletion and interruption protocols.

One Postgres eligibility timestamp handles scheduling and backoff without delayed-message infrastructure. Polling may delay dispatch; jobs cannot be reserved before eligibility. Manual retry ignores the original schedule.

## 8. Status Processing and Audit Consistency

RabbitMQ Single Active Consumer gives sequential status processing with replica failover, trading throughput for simpler ordering. Work consumption remains concurrent.

Events must match the current token and state. Lifecycle changes and audit records commit before acknowledgement. Duplicate starts count once; progress cannot decrease within an attempt; stale events cannot change accepted outcomes.

Reconnects can deliver an outcome before its start. Valid current-token outcomes record both together rather than lose the result.

## 9. Submission Idempotency and API Contracts

A unique constraint handles concurrent same-key submissions. Creation returns 202; duplicates return 200 with the unchanged original job's current state, even for differing payloads. Keys stay protected while active and until creation and terminal timestamps are both over 30 days old, exceeding the required 24 hours.

Added limits bound work: 64 KiB payloads, 1,000 batch items, 200-character keys, and 100 results per page. Validation rejects unknown fields, non-finite numbers, NUL, excessive nesting, and schedules without timezones. Errors use one format: missing jobs return 404, ineligible actions 409. Stable sort tie-breakers help pagination; new entries can still shift offset pages.

## 10. Mock Handlers and Progress

Handlers follow the PDF's delays and webhook success rate, without external calls. Results use stable job-derived IDs.

Batch progress updates roughly every ten percentage points to limit broker overhead. It resets between attempts, ends at 100 on success, and handles empty batches. Test-only failures, delays, and pauses make fault tests repeatable; ordinary operation rejects those controls.

## 11. Background Failures and Graceful Shutdown

`asyncio` suits waiting and network I/O; worker replicas provide concurrency. CPU-heavy handlers would need another execution model. Dependency failures reconnect with bounded backoff; unexpected bugs terminate visibly.

Shutdown stops intake, finishes bounded work, publishes and confirms outcomes, then acknowledges. The API stops new reservations and finishes in-progress work. Compose allows 220 seconds. Durable events survive API-first shutdown; forced termination uses crash recovery.

## 12. Dead-Letter Policy

The work queue's delivery limit is five. Rejected/exhausted work goes to a DLQ; the API fails an identifiable current job and logs why. Stale dead letters cannot overwrite newer work. Connection loss instead allows unacknowledged-message recovery.

The durable classic status queue has no such delivery limit, avoiding exhaustion during database outages. Invalid events are discarded.

Infrastructure faults can fail valid work and require manual retry. DLQ transfer is not guaranteed lossless, and replay is manual.

## 13. Security and Rate Limits

I added a shared bearer token and per-process submission/retry limits beyond the PDF's requirements. Secrets come from the environment, application containers run as non-root, and only the API is exposed on loopback. Health remains unauthenticated.

Workers use broker credentials; only the API receives database credentials. A shared broker identity simplifies setup. There are no user scopes or global rate limits; more API replicas increase the effective limit.

## 14. Logs and Retention

The recommended audit table explains retries and recovery. Lifecycle changes and audit entries commit together; JSON stdout logs carry job/attempt context.

Hourly cleanup removes logs and releases eligible keys after 30 days, in batches of 500 to limit contention. Job rows remain indefinitely; logs are not archived. Storage is not bounded. No separate event store is needed.

## 15. Deployment and Health

Compose runs the API, workers, Postgres, RabbitMQ, and Redis with persistent volumes. `compose.yaml` replaces the PDF's example filename. RabbitMQ is pinned to 4.3.0 for quorum priority.

Database locks and status-consumer failover coordinate API replicas. Workers scale separately. The deployment is single-node, without a load balancer or replicated availability; volumes protect restarts, not disk loss.

Health reports dependencies, job counts, queues, and consumers. Required dependency failures return 503; missing statistics or workers report degraded. Redis is observed indirectly through missing consumers. Connectivity does not prove handler health.

## 16. Technology, Code Structure, and Verification

Python 3.12, FastAPI/Pydantic, asyncpg, and aio-pika keep validation, locking, and delivery explicit. Direct SQL and broker use make concurrency inspectable but leave recovery code to us.

API and worker share a package/Dockerfile with separate entrypoints; incompatible message changes need coordination. Modules separate transport, storage, lifecycle, and execution without generic frameworks. Initial schema creation is atomic under an advisory lock; later replicas skip DDL. Future changes need migrations.

Beyond the six-test minimum, isolated Docker tests exercise real dependencies, races, crashes, and restarts. [Recorded results](docs/implementation.md): 81 E2E passes and four runner passes. They establish tested behavior, not exactly-once execution.

## 17. One Thing I Would Do Differently With More Time

Before real external calls, define handler-specific idempotency and test a crash after an effect but before saving its outcome. Use a stable job ID at idempotent receivers; otherwise reconcile unknown outcomes before retrying. Queue coordination cannot close that gap.

## 18. Worker-Owned Execution Claims in Redis

The mock interview exposed duplicate deliveries executing with conflicting outcomes. Tokens protected Postgres but did not choose one executor. Redis adds ownership and outcome replay without database sharing or API approval calls.

Keys include service namespace, type, job ID, and attempt token. Atomic claims last the handler timeout plus 15 seconds. Duplicates wait unacknowledged for outcomes or expiry, avoiding repeated requeues into the delivery limit.

An owner-checked atomic update saves success/failure for 24 hours. Expired owners cannot overwrite successors or publish unsaved results. Redelivery replays outcomes; new business attempts use new keys so cached failures cannot block retries.

Redis uses persistent AOF, fsync on every write, and no eviction. Outages pause consumption. This adds a single-instance dependency; expiry can permit overlapping execution. Separate services need enforced storage boundaries, not just different key prefixes.
