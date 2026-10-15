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
