# Execution decisions and evidence

Baseline: `69ff8920a082d7e6b6e43c3b87f4185293855188`.

The specification already agrees the seams: external HTTP, actual RabbitMQ, and deployed container lifecycle. No additional test-seam approval is needed. Work proceeds in vertical slices: API/handlers, scheduling/retries, delivery/recovery, operations. Tests are written before each slice, then run against the application images. Reviews run sequentially in the main thread as instructed by the supplied AGENTS.md tool mapping.

Concrete choices:

- Python 3.12, FastAPI/Pydantic, asyncpg SQL, aio-pika. One repeatable SQL schema, serialized by a Postgres advisory lock; no migration framework for an initial schema.
- Payloads: email `to`, optional `subject`/`body`; webhook `url`, optional `data`; report `parameters`; batch `items`. Reject unknown fields. Maximum JSON payload 64 KiB; batch 1,000 items; key 200 characters. Strict integer bounds. Errors use `{error: {code, message, details?}}`.
- One row lock serializes each lifecycle action and its audit insert. A final event for the current dispatched token atomically records the implied start and outcome, so a reordered start cannot lose a terminal result. Tokens become null when attempts end.
- Recovery claims use `FOR UPDATE SKIP LOCKED` and advance a lease before commit. Confirmation updates only the matching dispatched token; it never changes status. Lease expiration can cause duplicate publication after a sufficiently delayed publisher; that is an accepted uncertainty window.
- Durable quorum work/DLQ queues and a durable classic status queue with Single Active Consumer. Work delivery limit 5, worker prefetch 1. The status queue has no quorum delivery limit that could discard repeated database-outage redeliveries. All processing/final events use one confirmed channel per worker.
- Retry delay is `RETRY_BASE * 4 ** (attempts - 1)`; default base 30 seconds. Timeouts are 15/15/60/180 seconds, with test-only time scaling. Test controls are accepted only with `TEST_MODE=true` and are immutable payload data: failure-first-N lifetime attempts, permanent failure, and delay.
- Narrow test pause points use files inside containers, only in test mode. They pause real operations around reservation/publication, processing/final publication, and status commit/ack; they never replace persistence or broker calls.
- Postgres and RabbitMQ have persistent named volumes and no host ports. Only API has database configuration. API replicas bind independent host ports (`API_PORT=0`). Application UID 10001, shutdown allowance 220 seconds.

Evidence is recorded here as each slice runs. The Docker E2E suite is the completion gate; a local typecheck or unit test is not a substitute.

Initial red (core path): `uv run python scripts/e2e.py tests/e2e/test_api.py::test_submit_and_complete_all_handlers -x` reached the running API and failed on HTTP 404 instead of 202 for `POST /jobs` (one failing test). Earlier startup-only failures were plumbing issues and are not counted as behavioral red. Diagnostic stack: `bifrost-test-147c92b8`.

Recovery red: the deployed reaper scenario failed after its 30-second deadline with a job still processing and no `attempt_reaped` audit. Health red: the deployed health scenario failed because `database` was absent. Both were observed before their respective recovery/health implementation. The reaper subsequently passed against Docker in `bifrost-test-aaa10d67`.

The first full API/lifecycle slice run had 25 passes and one batch-progress timeout. Confirming a status for every item made broker round trips part of the handler budget. Progress now publishes at approximately ten-percent checkpoints; the timeout still bounds the complete real handler path. Test recovery allowance is five seconds, independently of the shortened handler timeout.

Dependency-outage diagnosis found the ordinary asyncpg disconnect, a transaction-exit wrapper error, and an intermittent protocol-state error during server termination. The latter matches upstream [asyncpg #1307](https://github.com/MagicStack/asyncpg/issues/1307) and [SQLAlchemy's driver-disconnect report](https://github.com/sqlalchemy/sqlalchemy/issues/13241). The dependency floor is asyncpg 0.32 (with the pool-release fix). Only the observed protocol state signature and closed-transaction wrapper are treated as disconnects; other internal client errors and programming errors remain fatal. Health uses native pool/query timeouts. Broker channel creation waits for reconnect readiness within a bounded timeout.

The Docker host also exposed expensive Erlang VM startup in repeated broker health commands. Both the broker and its CLI use small explicit scheduler counts suitable for this local exercise. One slow parallel diagnostic run was interrupted and is not passing evidence. Final verification runs use one isolated stack.

Further adversarial findings and fixes:

- `_test: null` was observed returning 202 where strict payload validation required 422 (`bifrost-test-9c4e9e7a`). It is now rejected before job creation.
- Replica startup plus cleanup reproduced a Postgres deadlock, terminating the first API (`bifrost-test-d3fe93d8`). Repeating DDL while normal transactions were active acquired relation locks in conflicting order. Initialization now takes its advisory lock, checks for the already-committed atomic initial schema, and skips DDL on ordinary replica startup. This changes the cause rather than treating deadlocks as generic retries.
- Redelivery tests isolate that mechanism by pausing the reaper before it acquires any database connection. Separate scenarios leave reaping enabled, send continuing progress, verify the fixed deadline, reject abandoned tokens, and race final events with expiry. A fresh logical attempt after reaping is an accepted outcome, not an attempt-counting bug.
- A Compose 2.31 scale-down command stalled during diagnostic cleanup while direct bounded `docker stop` completed normally. The harness now stops and removes only the named extra replicas directly. Assisted diagnostic runs are not counted as final passing evidence.
- Review centralized the current-attempt guard, reused the management HTTP client for its lifespan, separated broker publication timeouts from actual handler deadline expiry, and protected idempotency lookup against concurrent eligible-key cleanup.

The first complete final-stack run (`bifrost-test-a511b35f`) produced 55 passes and one health-recovery failure, with a nonzero exit and successful cleanup. Required database and AMQP connectivity had recovered, but sampled management fields were not yet available inside the harness's 40-second recovery budget. Cold RabbitMQ startup was measured at 36–44 seconds; repeated Erlang CLI health probes also exceeded their five-second timeout and prevented an otherwise-running broker from becoming container-ready. The container probe now checks the AMQP TCP port cheaply, while the API still verifies a real AMQP channel/queue and database operation. Recovery readiness has a bounded 90-second budget. No lifecycle, attempt, race, or queue-depth assertion was removed. An HTTP keep-alive hypothesis was ruled out and its experimental change reverted. Health now reports the missing-statistic/probe error explicitly instead of concealing its cause.

The strengthened targeted case, including database recovery and **two** broker restarts, passed in `bifrost-test-3503675f` (157.38 seconds; test and cleanup exit codes zero). The complete suite must pass again on this source before completion.

A subsequent full run (`bifrost-test-4098e918`) exposed the two-second external-hook installation race and was stopped after preserving its failure diagnostics. The replacement self-arming `pause_at` control was first observed failing with HTTP 422 instead of 202 (`bifrost-test-2d6d429b`), then implemented at the existing real-path pause boundaries. All six deployed publication/worker crash cases passed (`bifrost-test-f20c12d2`, 189.83 seconds). Pause markers survive ordinary container restart so release does not re-arm the same job; the feature is entirely disabled outside test mode.

Additional hostile-input tracers reproduced a nullable batch-delay crash (`bifrost-test-c6df60d1`) and non-finite JSON admission/status poisoning (`bifrost-test-f31447f7`). A null test delay now means the ordinary handler delay consistently; its all-handler scenario passed (`bifrost-test-7d4a3951`). One strict JSON encoder is shared by payload validation, result validation, and persistence; validation occurs before coercion or state application. Both HTTP `NaN` rejection and useful status consumption after a non-finite terminal message passed (`bifrost-test-777e4d9d`, 15.23 seconds). The full suite is rerun after these concrete fixes.

## Final completion gate

**Passed:** `UV_PROJECT_ENVIRONMENT=/tmp/bifrost-venv uv run python scripts/e2e.py` (the optional environment setting only places host test dependencies on the Linux filesystem). The documented equivalent is `uv run python scripts/e2e.py`.

- Project: `bifrost-test-9f92249d`; empty isolated dependency volumes at startup.
- Result: **56 passed in 811.12 seconds**, zero failures, errors, or skips. Test exit code 0; cleanup exit code 0. Cleanup removed this run's containers, networks, volumes, and application image tags.
- API: 19 passing test cases; delivery/recovery: 17; lifecycle: 9; operations: 11.
- Tested source SHA-256: `fcfcfa92a33f0f10da3e95e8e9bcd4fc73966c4f714ae2d930d82b2680869033`. Application, deployment, dependency lock, test, and runner files were frozen for the run and matched this identity afterward.
- API image: `sha256:f354fa059e6f6d0c2af46797f149b04135ebbfbb91b77c7c93496f6d7b035a7a`.
- Worker image: `sha256:42dc69645bfea43617c5a679454ac49c1ea09134a93673f93f81ebf5985795b8`.
- Static verification: `uv run ruff format --check bifrost tests scripts`, `uv run ruff check bifrost tests scripts`, and `uv run ty check bifrost` all passed.

Local evidence is retained under `artifacts/bifrost-test-9f92249d/`: `build.json`, `images.json`, `results.xml`, `stack.log`, and `execution.log`. Generated artifacts and credentials are excluded from Git; the runner reproduces the acceptance evidence. [acceptance.md](acceptance.md) provides the requirement mapping and [review.md](review.md) records the separate standards/spec review. No required acceptance case remains skipped or unverified; the limitations are the specification's accepted operational boundaries.

## Review follow-up

The review supplied reproductions for NUL admission/status poisoning, submission nesting that exceeded the worker JSON parser's envelope budget, dispatch continuing to reserve after shutdown, and diagnostics failures bypassing runner teardown. Shared JSON validation now rejects NUL keys/values and limits nesting to 64 object/array containers, safely below the broker parser's limit after adding Work/Event envelopes. Idempotency keys and failure text also reject NUL. Dispatch observes the lifespan stop event before taking another reservation, while completing a handoff already in progress. Runner teardown is in a nested `finally`; diagnostic failures still produce a nonzero exit.

Focused host-runner tests passed all four cases: log timeout, stack-log write failure, report-redaction write failure, and successful diagnostics. These simulate failures of the host filesystem/subprocess interfaces, not application behavior. Ruff lint, formatting, and `ty check bifrost` passed.

The first review run, `bifrost-test-9ef6e239`, finished with **73 passed, 1 failed in 503.01 seconds**, test exit 1 and cleanup exit 0. All new validation and shutdown regressions passed. The existing replica-failover case completed a job through the sole surviving API, then observed a sampled management response with one consumer still marked inactive. Its poll previously waited only for consumer removal and asserted activity on that first response. The bounded poll now requires both one remaining consumer and its active flag; the activity assertion and actual completion requirement remain intact. A new complete run follows this fix; the failed run is retained as diagnostic evidence.

**Review completion gate passed:** `UV_PROJECT_ENVIRONMENT=/tmp/bifrost-venv uv run python scripts/e2e.py`.

- Project: `bifrost-test-867afc4c`, with fresh isolated volumes.
- Result: **74 passed in 548.98 seconds**, zero failures, errors, or skips. Test exit 0; cleanup exit 0. All project containers, volumes, and networks were removed.
- Tested source SHA-256: `056fd1c8fce48ff1173bdfde051c30d722e67b0a5d88ee6583b75843c34052e1`, verified unchanged after the run. Base revision: `c775cabe0ae3de8cac6f4a3e5bac4b86b80b4c1f` plus the review fixes.
- API image: `sha256:56c0ffaf6b6f228c9e3fa3481d004d02ae885584e1127a853ebfecfe8588ead3`; worker image: `sha256:10c27f748bd16c737caa2e8adfd058405cea79fff8ba2024a378d249b84e961d`.
- Supporting runner tests: **4 passed** using `UV_PROJECT_ENVIRONMENT=/tmp/bifrost-venv uv run pytest tests/runner -q`. Ruff lint/format checks and `ty check bifrost` passed on the final source.

Evidence is retained in `artifacts/bifrost-test-867afc4c/`: `build.json`, `images.json`, `results.xml`, `stack.log`, and `execution.log`. The first review run's failure and cleanup diagnostics remain in its separate artifact directory.

## Redis execution coordination (2026-10-20)

The interview review changed the stateless-worker choice: workers now own Redis claims and retained final outcomes. Postgres remains private to the API. The Redis adapter contains the atomic claim and owner-checked outcome save; the execution service contains waiting, timeout/failure handling, and replay. Compose adds Redis 7.4 with a named AOF volume, fsync on every write, no eviction, and a worker-only internal network. Decision 18 records the rationale, namespace layout, actual-service ownership, and remaining effect/recovery limits.

Behavioral red: the new deployed replay test in `bifrost-test-555ef2d8` failed because two handler executions were observed after replaying one completed delivery. No application imports or dependency mocks were used.

Focused green: `uv run python scripts/e2e.py tests/e2e/test_execution.py tests/e2e/test_delivery.py::test_worker_crash_redelivery_keeps_logical_attempt tests/e2e/test_lifecycle.py::test_manual_retry_race_grants_one_cycle_and_preserves_history -x` passed **10 tests in 73.77 seconds**, with cleanup exit 0 (`bifrost-test-e65d2986`). This includes saved success/failure replay, competing replicas, forced Redis restart, Redis outage recovery, worker crash boundaries, and manual retry. A subsequent full run was deliberately interrupted to correct the Redis async retry-class import; it is not counted as passing evidence. The final complete gate below uses rebuilt images after that correction and includes an adversarial expired-owner test and Redis network-isolation assertions.

**Redis completion gate passed:** `PATH=/mnt/wsl/docker-desktop/cli-tools/usr/bin:$PATH UV_PROJECT_ENVIRONMENT=/tmp/bifrost-venv uv run python scripts/e2e.py`. The PATH override selects this host's Linux Docker Desktop client; the documented command remains `uv run python scripts/e2e.py` with Docker configured on PATH.

- Project: `bifrost-test-2c3baf56`, with fresh isolated Postgres, RabbitMQ, and Redis volumes.
- Result: **81 passed in 577.65 seconds**, zero failures, errors, or skips. Test exit 0; cleanup exit 0. The runner removed its containers, volumes, networks, and application image tags.
- Tested source SHA-256: `16fbe8be0bb11672c8978ac1ec4e893c6ae258ef7adaca1c486555dcb33207cf`; independently recomputed and unchanged after the run. Base revision: `c775cabe0ae3de8cac6f4a3e5bac4b86b80b4c1f` plus the working-tree changes.
- API image: `sha256:3e3708b496757afec99032ea5284f019156ab27f08d0326555abeecb94167956`; worker image: `sha256:43a050c49090bb14de8a03fc716ce187c711efbc0622bded43734de093ff2e96` (inspected before teardown).
- Supporting host-runner tests: **4 passed**. Ruff lint/format checks, `ty check bifrost`, and CRLF-aware diff whitespace checks passed.

Local evidence: `artifacts/bifrost-test-2c3baf56/build.json`, `results.xml`, and redacted `stack.log`. The source manifest and JUnit report establish the exact tested source and complete scenario results; generated artifacts remain ignored.
