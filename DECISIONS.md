# Design Decisions

Agreed design for the assignment, now implemented and verified against the Docker E2E suite. [prd.md](prd.md) defines behavior and acceptance criteria. This document records the choices and their costs; [execution evidence](docs/implementation.md) records the implemented details and verification.

## 1. Job Pickup Strategy

**Choice:** The scheduler atomically moves eligible scheduled/pending jobs to dispatched using short database claims such as FOR UPDATE SKIP LOCKED. It assigns a claim token and commits before publishing to RabbitMQ. Dispatched jobs are excluded from normal pickup.

**Why:** Row locks prevent competing schedulers from reserving the same eligible row. Persisted dispatched status prevents another claim after the lock is released. Transactions do not stay open during publication or execution.

The job row records publication confirmation and recovery timing. An unconfirmed publication is retried with the same token after a delay. Confirmed work is left waiting in the broker, including when workers are offline.

**Tradeoff:** There is no separate outbox table or distributed transaction. A crash after broker acceptance but before confirmation is recorded can produce duplicate messages. Recovery reservations must themselves be atomic.

## 2. Worker Crash Recovery and Timeouts

**Choice:** Workers enforce bounded handler execution. The API reaper recovers processing attempts left stuck after failure, using attempt_started_at plus the type timeout and a recovery allowance.

| Type | Execution timeout |
| --- | --- |
| Email / webhook | 15 seconds |
| Report | 60 seconds |
| Batch | 180 seconds |

Progress and general updated_at changes do not extend the deadline. No heartbeat service is needed for these bounded mock handlers.

**Mid-job crash:** RabbitMQ may redeliver the unacknowledged work with the same token. If the attempt remains stuck, the reaper atomically checks its state/token/deadline, invalidates the token, and schedules another attempt with backoff or marks the job failed when its budget is exhausted. A concurrent completion must not be overwritten.

**Tradeoff:** Recovery is eventual. The reaper cannot stop a disconnected worker that is still running; late events cannot change current state, but physical execution can overlap during recovery.

## 3. Priority Queue Implementation

**Choice:** One shared quorum work queue, q.jobs, serves every type. Identical workers select handlers from the message type. Priority is applied in database selection and RabbitMQ 4.3 message delivery, using 0–31 with suggested levels 0 low, 4 normal, 15 high, and 30 critical.

**Why:** One queue avoids type-specific worker configuration. Any additional worker can process any job.

**Tradeoff:** The guarantee is preference within a type among eligible jobs still waiting for dispatch/delivery. The shared queue also compares priorities across types, but there is no promised cross-type execution order or reserved capacity. Priority does not preempt delivered/running work or bypass scheduling/backoff. Strict priority can starve lower-priority work; no aging or fairness mechanism is included.

## 4. Retry Backoff Strategy

**Choice:** The API schedules business retries in Postgres. The default cycle has three total logical attempts:

| Attempt | Delay before eligibility |
| --- | --- |
| 1 | Immediate, or at the initial scheduled time |
| 2 | 30 seconds after attempt 1 fails |
| 3 | 120 seconds after attempt 2 fails |

Continue the exponential progression for supported budgets of 1–5 attempts. Tests shorten delays through configuration. Transient failures and timeouts consume the cycle's budget; permanent failures end it immediately. Retries preserve priority.

**Manual retry:** A failed job may receive one fresh cycle with the same max_attempts budget. Track attempts for the current cycle, total_attempts for its lifetime, and manual_retry_used. With the default budget, at most six logical attempts are available across both cycles.

Manual retry preserves payload and history, resets current output/error/progress and attempt-local state, clears completed_at, and makes the job immediately eligible. started_at remains the first lifetime start. A second manual retry returns 409.

**Tradeoff:** Broker redelivery retains the same token and does not consume another logical attempt. Counters describe accepted logical attempts, not every physical handler invocation. A permanent failure can leave some attempt budget unused.

## 5. Database Ownership and Worker Coupling

**Choice:** The API service owns all Postgres access, including scheduling, status updates, recovery, logs, and cleanup. Workers have no DATABASE_URL. They communicate through RabbitMQ without an HTTP execution-admission handshake. Workers own a small Redis execution registry on a separate internal network; the API has no Redis configuration or network access. See decision 18.

**Why:** Workers depend on the message contract rather than the database schema and credentials.

**Tradeoff:** Job queries can lag execution while status events await processing. API-side background processing is required to advance persistent state.

## 6. Delivery and Execution Guarantees

**Choice:** Use durable messages, publisher confirmations, competing work consumers, and manual acknowledgements with one outstanding work delivery per worker. Confirm processing publication before running the handler; confirm final-outcome publication before acknowledging work. Business failures are acknowledged after their failure event is confirmed.

**Guarantee:** At-least-once delivery with guarded database state. Worker-owned Redis claims suppress duplicate execution of the same logical attempt while its lease is valid; saved outcomes are replayed without rerunning handlers for 24 hours. Connection loss and uncertain publication can still produce duplicate messages. Publisher confirmation means broker acceptance, not that the API applied the event. See [RabbitMQ acknowledgements](https://www.rabbitmq.com/docs/confirms).

**Tradeoff:** Dispatch tokens identify logical attempts, and Redis owner IDs distinguish physical claimants. An expired lease, lost Redis data, an expired saved outcome, or a different business attempt can still permit repeated effects. There is no exactly-once claim. Real external effects need handler-specific idempotency.

## 7. Cancellation Cutoff and Scheduling

**Choice:** Cancellation is allowed only from scheduled/pending. Scheduler reservation and cancellation compete atomically: cancellation wins and prevents dispatch, or dispatch wins and cancellation returns 409. Dispatched includes publication still awaiting confirmation or recovery.

Future jobs are not dispatched before scheduled_for. Scheduling and retry backoff share next_attempt_at; a manual cycle is immediately eligible regardless of the original schedule.

**Tradeoff:** A queued job cannot be cancelled after reservation, even before a worker starts it. No broker-message deletion or worker interruption protocol is needed.

## 8. Status Processing and Audit Consistency

**Choice:** One active consumer processes q.job-status sequentially. RabbitMQ Single Active Consumer supplies failover between API replicas. It is enabled for status consumption, not the work queue. Workers publish their events in order on the same channel. See [RabbitMQ Single Active Consumer](https://www.rabbitmq.com/docs/consumers#single-active-consumer).

Every execution event requires a claim token. State/token guards reject stale or duplicate transitions. Count the first accepted processing event once, keep progress monotonic within the attempt, and commit state and audit logs before acknowledging the event.

A valid current-attempt outcome must survive reconnect/recovery even if its start has not yet been applied. Planning must define that handling without adding a separate event store.

**Tradeoff:** Sequential status processing limits throughput. That is acceptable here and avoids ordinary races between status consumers.

## 9. Submission Idempotency and API Contracts

**Choice:** A unique database constraint protects optional idempotency keys, including concurrent requests. New submissions return 202; duplicates return 200 with the existing job's current state and never mutate it. Retain keys for at least 24 hours; best-effort cleanup may release terminal-job keys after 30 days.

Use strict per-type validation, timezone-aware scheduling, stable paginated jobs/logs, and consistent errors. Unknown jobs return 404; invalid cancel/retry actions return 409. Pagination defaults to 50 with a maximum of 100.

**Tradeoff:** Submission deduplication prevents duplicate job records, not repeated execution. A differing payload under an existing key still returns the original job.

## 10. Mock Handlers and Progress

**Choice:** Use asynchronous mock handlers with the assignment's delays and webhook success/failure distribution. No external email, HTTP, or storage calls. Email/report identifiers derive from the job ID. Batch progress resets for a new logical attempt and reaches 100 on success; empty batches return an empty summary.

**Why:** Stable results and deterministic test controls, such as failing the first N lifetime attempts, make retry/recovery tests repeatable without changing job payloads. Normal webhook behavior remains 80% success and 20% transient failure.

**Tradeoff:** These handlers demonstrate queue behavior, not real integrations. Stable identifiers do not prove exactly-once effects, and polling may miss intermediate progress values.

## 11. Background Failures and Graceful Shutdown

**Choice:** Use asyncio for the I/O-bound mock workload. Background loops reconnect with bounded backoff for expected dependency failures. Unexpected programming errors fail the service visibly; TaskGroup is not treated as automatic failure isolation.

On shutdown, workers stop accepting work, finish the current bounded handler, publish the outcome, acknowledge, and exit. The shutdown allowance exceeds the longest handler timeout plus publication time. The API stops scheduling and finishes current database transactions. Worker outcomes remain durable if the API exits first.

**Tradeoff:** Shutdown can wait for a long batch. Forced termination uses ordinary crash recovery and may repeat execution.

## 12. Dead-Letter Policy

**Choice:** Use q.jobs.dlq through dlx.jobs and a small API-side consumer. A matching current dispatched/processing job becomes failed with a useful error and log. Stale dead letters cannot overwrite a newer attempt or terminal outcome.

If status publication fails and the work channel is usable, rejecting without requeue can dead-letter the work. If the connection is gone, unacknowledged-delivery recovery applies. Choose a small delivery limit compatible with the crash tests during planning.

**Tradeoff:** A transient infrastructure fault may fail a valid job and require its one manual retry. There is no separate infrastructure retry framework, automatic DLQ replay, or lossless DLQ-transfer guarantee.

## 13. Security and Rate Limits

**Choice:** Protect /jobs with one environment-configured shared bearer token. Workers using only RabbitMQ authenticate with broker credentials; they do not call the API just to authenticate. Use one broker user locally, non-root application containers, an internal dependency network, and API-only database credentials. Keep secrets out of source code.

Apply configurable per-process limits to submission and manual retry. Leave /health available for local readiness checks.

**Tradeoff:** There is no JWT service, user identity, authorization scope, per-service broker identity, or globally coordinated rate limit. Production authentication/authorization would require separate work.

## 14. Logs and Retention

**Choice:** Emit structured JSON stdout logs with job/type/cycle/attempt/token context when applicable. Any API-side component changing state writes its audit record in the same database transaction. Use jobs and job_logs; no separate attempt-history subsystem.

Delete old logs and release eligible idempotency keys periodically in small batches, using an hourly best-effort cleanup and a 30-day age threshold.

**Tradeoff:** Cleanup does not guarantee bounded storage. Job rows may remain indefinitely, and expired audit history is not archived. This is exercise-level housekeeping.

## 15. Deployment and Health

**Choice:** Docker Compose runs one Postgres instance, one RabbitMQ broker, the API, and a scalable shared worker service. Persist database/broker data, pin the selected RabbitMQ 4.3-compatible version, provide repeatable schema initialization, and handle readiness/reconnection. Multiple API replicas use atomic scheduler claims and one active status consumer.

Health reports dependency connectivity, queue ready/unacknowledged counts, connected work consumers, status backlog, and DLQ depth. Required dependency failure returns 503. Missing management statistics or zero work consumers is reported as degraded.

**Tradeoff:** A connected consumer is not proof of handler health. Single-node persistence supports ordinary restarts, not high availability or permanent disk/volume loss. Partitioning, sharding, and clustering are future considerations; the current schema is not claimed to be ready for them.

## 16. Verification and Planning Boundary

**Choice:** Use live Postgres/RabbitMQ for integration evidence, deterministic mock behavior, and focused worker tests. Cover the required functional cases plus interrupted publication, cancellation races, stale/duplicate events, worker failure, timeout, retry-cycle limits, shutdown, and restart persistence.

Priority tests queue work before enabling consumption. Crash tests allow broker redelivery or reaping; exercise the reaper separately. Attempts and accepted logs are not evidence that physical execution happened exactly once.

**Tradeoff:** This is a small set of meaningful scenarios, not an exhaustive fault matrix. Concrete SQL, schemas, modules, configuration, and test mechanics belong in the execution plan.

## 17. One Thing I Would Do Differently With More Time

Before connecting real external effects, define idempotency for each handler and test the failure between performing the effect and reporting completion. For example, an external service that accepts an idempotency key can use the job ID to prevent duplicate effects.

The exercise deliberately stops at mock handlers and guarded job state. Adding more queue machinery alone would not make real side effects exactly once.

## 18. Worker-Owned Execution Claims in Redis

**Problem:** A recovered publication can put the same dispatch in RabbitMQ twice. Previously both workers executed it and could report conflicting results; the first final event accepted by the API won. A dispatch token protects job state, but does not select one physical executor.

**Choice:** Add one Redis container, one concrete adapter (`execution_store.py`), and one execution service (`execution.py`). Keep Postgres private to the API and communicate between services only through RabbitMQ. No API polling, admission RPC, new Postgres database, generic storage framework, or separate coordinator service is needed.

The key is `bifrost:worker:{type}:{claim_token}:{job_id}`. `EXECUTION_NAMESPACE` is a stable service prefix shared by all its replicas, never a container hostname. Including the token lets automatic and manual business retries run under new keys instead of replaying a previous attempt's failure.

1. `SET ... NX PX` claims a missing key with a fresh `claimed:{owner_uuid}` value. Its lease is the handler timeout plus 15 seconds for processing publication and saving the outcome. There are no heartbeats.
2. A duplicate waits on Redis with its broker delivery still unacknowledged. It either observes the saved outcome or claims the key after the dead owner's lease expires. Waiting observes worker shutdown; it does not busy-requeue into RabbitMQ's delivery limit.
3. The owner publishes processing and runs the bounded handler. A short Lua compare-and-set replaces its own claim with the final Event JSON, retained for 24 hours. An expired owner cannot overwrite a successor's claim or saved outcome, and cannot publish its unsaved result.
4. The worker publishes that recorded event with confirmation, then acknowledges work. Redelivery after saving or publication replays the same recorded outcome without another execution. Both success and failure outcomes are retained; progress is not cached.

Redis uses a named volume, AOF with `appendfsync always`, and `noeviction`. The extra fsync cost is acceptable for two small writes per mock execution. Redis has no host port and joins only the worker execution network. Connection failures stop execution and close the broker channel so unacknowledged work can return. Before reopening broker consumption, the worker checks Redis connectivity, avoiding repeated redeliveries during a prolonged outage. The API's health endpoint still measures its own dependencies and reports missing worker consumers as degraded; it does not probe the worker's private Redis.

**Why Redis:** This is temporary coordination and outcome replay, not another relational domain model. Redis provides an atomic expiring claim with little code. Saving the outcome matters: deleting a lock immediately after success would let a redelivered message execute again. The Lua comparison matters: an old owner must not overwrite a newer owner after expiry. These follow Redis's [SET semantics](https://redis.io/docs/latest/commands/set/) and [single-instance lease guidance](https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/).

**On actual services:** Email service replicas would share an email-service namespace/store; report service replicas would share their own. Each service owns its execution records and credentials. Independent services do not read each other's business tables or the job API's database. Separate Redis deployments or prefix-scoped ACLs can enforce this boundary; a prefix by itself is not authorization.

**Limits:** This is one Redis instance, not a high-availability lock system. Lease expiry permits takeover even if an old process is still running. The claim is per logical attempt, not across all future attempts of a job. The API remains the retry/reaper authority: delayed status application can abandon an attempt and issue a new token even if its old execution succeeded. A crash after an external effect but before saving the outcome is still ambiguous. Real email/webhook integrations must use a stable operation key such as job ID at an idempotent receiver, or reconcile unknown outcomes. Redis claims reduce duplicate execution; they do not make arbitrary effects exactly once.
