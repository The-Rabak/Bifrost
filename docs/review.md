# Implementation review

The review below records the original implementation. The 2026-10-20 Redis extension adds worker-owned atomic claims and saved-outcome replay; its design is recorded in DECISIONS.md decision 18 and its current verification is appended to implementation.md. Review checked service-scoped namespaces, per-attempt retry isolation, owner-checked outcome replacement, Redis outage handling, and preservation of API/Postgres ownership. It also corrected the Redis client's async retry configuration before the final complete gate.

The Redis extension passed **81 deployed E2E tests**, with zero failures/errors/skips, plus four runner tests and lint/format/type checks. The recorded source hash matches the unchanged post-run checkout. Recovery and external-effect limitations remain explicit; there is no exactly-once claim.

Fixed point: `69ff8920a082d7e6b6e43c3b87f4185293855188`. Diff: `git diff 69ff8920a082d7e6b6e43c3b87f4185293855188...HEAD`.

Standards sources: the supplied AGENTS.md tool mapping, `spec.md` implementation decisions 25–29 and testing decisions, `DECISIONS.md`, and `prd.md`. Standards and specification were reviewed sequentially in the main thread, as directed by the supplied tool mapping. The review included the skill's twelve code-smell heuristics; these were treated as judgement calls rather than automatic violations.

## Standards

No remaining findings. Responsibilities are divided among HTTP, persistence, lifecycle policy, broker transport, execution, and supervision. Current-attempt guards, retry decisions, timeout policy, JSON validation, and state/audit writes have authoritative shared implementations. Workers perform no Postgres operations. There are no generic repositories, plugin frameworks, event stores, admission RPCs, or duplicated unit suites.

Resolved judgement calls included duplicated current-attempt guards and repeated management-client allocation. The accepted storage constraints intentionally mirror public validation at the persistence boundary. The small concrete registries and functions provide the four-handler contract without an abstraction hierarchy.

## Spec

No remaining findings. Review and deployed tests resolved startup-DDL/cleanup deadlocks, idempotency-key cleanup races, nullable mock-control failures, non-finite JSON admission and status poisoning, publication-timeout classification, and timing-dependent fault-hook installation. State/token guards preserve terminal outcomes and logical counters through the specified recovery windows.

Follow-up review also resolved NUL admission and status poisoning, HTTP/worker nesting-limit disagreement, scheduler reservations continuing during shutdown, and runner diagnostics bypassing teardown. Shared JSON validation limits payload/result nesting to 64 containers and rejects NUL keys/values; text validators protect keys and failure messages. Shutdown finishes the current handoff and stops further reservations. Teardown runs even when diagnostics fail.

The final source passed all 74 deployed Docker E2E tests with no skipped cases, errors, or failures, plus four focused host-runner fault tests. The rerun exposed and fixed a management-statistics polling race in the existing replica-failover test without removing its activity or job-completion assertions. The acceptance matrix maps requirements to executable assertions. Exact commands, source/image identities, results, and earlier failures are recorded in `implementation.md`. At-least-once physical execution and the other agreed operational limits remain explicit in README; the implementation makes no exactly-once claim.

Summary: Standards 0 remaining findings; Spec 0 remaining findings. Neither axis has an unresolved blocking issue.
