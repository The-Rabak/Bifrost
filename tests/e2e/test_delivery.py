import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import pytest
from conftest import (
    compose,
    eventually,
    executions,
    finished,
    hook,
    inject,
    job,
    management,
    reached,
    ready,
    remove_replica,
    submit,
    token,
    unhook,
)
from test_lifecycle import logs


def test_confirmed_backlog_is_not_republished_and_broker_prefers_priority(api, stopped_workers):
    ready(api, consumers=0)
    schedule = (datetime.now(UTC) + timedelta(seconds=2)).isoformat()
    ids = [submit(api, priority=p, scheduled_for=schedule) for p in (0, 4, 15, 31)]
    assert all(job(api, i)["status"] == "scheduled" for i in ids)
    for job_id in ids:
        eventually(lambda job_id=job_id: job(api, job_id), lambda j: j["status"] == "dispatched")
    dispatch_order = sorted(
        ids, key=lambda i: next(e["id"] for e in logs(api, i) if e["message"] == "dispatched")
    )
    for job_id in ids:
        dispatched = next(e for e in logs(api, job_id) if e["message"] == "dispatched")
        assert datetime.fromisoformat(dispatched["created_at"]) >= datetime.fromisoformat(schedule)
    assert dispatch_order == ids[::-1]
    snapshot = ready(api, consumers=0)
    eventually(lambda: ready(api, consumers=0)["queues"]["q.jobs"]["ready"], lambda n: n == 4)
    end = time.monotonic() + 5.5
    while time.monotonic() < end:
        assert ready(api, consumers=0)["queues"]["q.jobs"]["ready"] == 4
        time.sleep(0.1)
    assert snapshot["status"] == "degraded"
    compose("start", "worker")
    for job_id in ids:
        finished(api, job_id)
    order = [r["job_id"] for r in executions() if r["job_id"] in ids]
    assert order == ids[::-1]


@pytest.mark.parametrize("point", ["after_reserve", "after_publish"])
def test_publication_crash_recovers_same_token(api, stopped_workers, point):
    job_id = submit(api, payload={"to": "a@example.com", "_test": {"pause_at": point}})
    eventually(lambda: reached("api", point, job_id), bool)
    claim = token(api, job_id)
    assert job(api, job_id)["status"] == "dispatched"
    compose("kill", "-s", "SIGKILL", "api")
    compose("start", "api")
    unhook("api", point)
    ready(api, consumers=0)
    compose("up", "-d", "--scale", "api=2", "--no-recreate", "api")
    compose("start", "worker")
    try:
        result = finished(api, job_id)
        assert result["attempts"] == result["total_attempts"] == 1
        assert [e["claim_token"] for e in logs(api, job_id) if e["message"] == "processing"] == [
            claim
        ]
        assert all(r["claim_token"] == claim for r in executions() if r["job_id"] == job_id)
    finally:
        remove_replica("api", 2)


def test_late_publish_confirmation_cannot_regress_completed_execution(api):
    job_id = submit(api, payload={"to": "a@example.com", "_test": {"pause_at": "after_publish"}})
    try:
        eventually(lambda: reached("api", "after_publish", job_id), bool)
        result = finished(api, job_id)
        assert result["attempts"] == 1
        pending = submit(api)
        assert job(api, pending)["status"] == "pending"
        assert api.post(f"/jobs/{pending}/cancel").status_code == 200
        assert job(api, pending)["status"] == "cancelled"
        assert not any(r["job_id"] == pending for r in executions())
    finally:
        unhook("api", "after_publish")
    assert finished(api, job_id)["result"] == result["result"]


def test_terminal_before_start_duplicate_stale_and_malformed_events(api, stopped_workers):
    job_id = submit(api)
    eventually(lambda: job(api, job_id), lambda j: j["status"] == "dispatched")
    claim = token(api, job_id)
    base = {"job_id": job_id, "claim_token": claim}
    inject("q.job-status", {**base, "event": "completed", "result": {"message_id": "out-of-order"}})
    done = finished(api, job_id)
    assert done["attempts"] == done["total_attempts"] == 1
    initial_logs = logs(api, job_id)
    for event in [
        {**base, "event": "processing"},
        {**base, "event": "completed", "result": {"message_id": "duplicate"}},
        {
            **base,
            "event": "failed",
            "error": {"code": "late", "message": "late", "transient": True},
        },
        {**base, "event": "progress", "progress": 5},
        {"job_id": job_id, "event": "processing"},
    ]:
        inject("q.job-status", event)
    inject("q.jobs.dlq", {**base})
    inject("q.job-status", b"not json")
    next_id = submit(api)
    eventually(lambda: job(api, next_id), lambda j: j["status"] == "dispatched")
    next_claim = token(api, next_id)
    inject(
        "q.job-status",
        {
            "job_id": next_id,
            "claim_token": next_claim,
            "event": "completed",
            "result": {"message_id": "alive"},
        },
    )
    assert finished(api, next_id)["result"] == {"message_id": "alive"}
    assert job(api, job_id) == done
    assert logs(api, job_id) == initial_logs


def test_duplicate_start_monotonic_progress_and_current_dead_letter(api, stopped_workers):
    job_id = submit(api)
    eventually(lambda: job(api, job_id), lambda j: j["status"] == "dispatched")
    claim = token(api, job_id)
    base = {"job_id": job_id, "claim_token": claim}
    for _ in range(3):
        inject("q.job-status", {**base, "event": "processing"})
    for progress in [60, 20, 90]:
        inject("q.job-status", {**base, "event": "progress", "progress": progress})
    result = eventually(lambda: job(api, job_id), lambda j: j["progress"] == 90)
    assert result["attempts"] == result["total_attempts"] == 1
    assert len([e for e in logs(api, job_id) if e["message"] == "processing"]) == 1
    inject("q.jobs.dlq", base)
    failed = finished(api, job_id, "failed")
    assert failed["error"]["code"] == "dead_letter"
    assert api.post(f"/jobs/{job_id}/retry").status_code == 202
    eventually(lambda: job(api, job_id), lambda j: j["status"] == "dispatched")
    new_claim = token(api, job_id)
    assert new_claim != claim
    inject("q.jobs.dlq", base)
    inject("q.job-status", {**base, "event": "completed", "result": {"stale": True}})
    inject(
        "q.job-status",
        {
            "job_id": job_id,
            "claim_token": new_claim,
            "event": "completed",
            "result": {"fresh": True},
        },
    )
    assert finished(api, job_id)["result"] == {"fresh": True}


@pytest.mark.parametrize("point", ["before_status_commit", "after_status_commit"])
def test_status_commit_ack_crash_keeps_atomic_history(api, stopped_workers, point):
    job_id = submit(api)
    hook("api", point, job_id)
    eventually(lambda: job(api, job_id), lambda j: j["status"] == "dispatched")
    inject(
        "q.job-status",
        {
            "job_id": job_id,
            "claim_token": token(api, job_id),
            "event": "completed",
            "result": {"durable": True},
        },
    )
    eventually(lambda: reached("api", point, job_id), bool)
    if point == "before_status_commit":
        assert job(api, job_id)["status"] == "dispatched"
        assert [e["message"] for e in logs(api, job_id)] == ["submitted", "dispatched"]
    else:
        assert job(api, job_id)["status"] == "completed"
    compose("kill", "-s", "SIGKILL", "api")
    compose("start", "api")
    unhook("api", point)
    ready(api, consumers=0)
    result = finished(api, job_id)
    assert result["result"] == {"durable": True} and result["total_attempts"] == 1
    assert [e["message"] for e in logs(api, job_id)] == [
        "submitted",
        "dispatched",
        "processing",
        "completed",
    ]


@pytest.mark.parametrize(
    "point", ["before_processing", "after_outcome_saved", "after_final", "during_execution"]
)
def test_worker_crash_redelivery_keeps_logical_attempt(api, point):
    # Redelivery and reaping are separate mechanisms; this case isolates redelivery.
    hook("api", "reaper_tick", "reaper")
    try:
        job_id = submit(
            api,
            payload={"to": "a@example.com", "_test": {"delay": 0.15, "pause_at": point}},
        )
        eventually(lambda: reached("worker", point, job_id), bool)
        if point == "before_processing":
            assert job(api, job_id)["total_attempts"] == 0
        else:
            eventually(lambda: job(api, job_id), lambda j: j["total_attempts"] == 1)
        assert api.post(f"/jobs/{job_id}/cancel").status_code == 409
        assert api.post(f"/jobs/{job_id}/retry").status_code == 409
        claim = token(api, job_id)
        compose("kill", "-s", "SIGKILL", "worker")
        compose("start", "worker")
        unhook("worker", point)
        result = finished(api, job_id)
        assert result["total_attempts"] == 1
        assert len([e for e in logs(api, job_id) if e["message"] == "processing"]) == 1
        assert all(r["claim_token"] == claim for r in executions() if r["job_id"] == job_id)
        eventually(
            lambda: compose("logs", "--no-color", "worker").stdout,
            lambda text: any(
                job_id in line and "work_acknowledged" in line for line in text.splitlines()
            ),
        )
        if point == "during_execution":
            assert len([r for r in executions() if r["job_id"] == job_id]) >= 2
        else:
            # A saved outcome is replayed after a crash, without another handler invocation.
            assert len([r for r in executions() if r["job_id"] == job_id]) == 1
    finally:
        unhook("api", "reaper_tick")
        unhook("worker", point)
    assert finished(api, submit(api))["status"] == "completed"


def test_reaper_deadline_is_fixed_and_abandoned_token_cannot_update_successor(api, stopped_workers):
    job_id = submit(api, max_attempts=2)
    eventually(lambda: job(api, job_id), lambda j: j["status"] == "dispatched")
    old = token(api, job_id)
    inject("q.job-status", {"job_id": job_id, "claim_token": old, "event": "processing"})
    start = eventually(lambda: job(api, job_id), lambda j: j["status"] == "processing")
    progress = 0

    def keep_progressing():
        nonlocal progress
        progress = min(progress + 5, 95)
        inject(
            "q.job-status",
            {"job_id": job_id, "claim_token": old, "event": "progress", "progress": progress},
        )
        return logs(api, job_id)

    entries = eventually(
        keep_progressing,
        lambda entries: any(e["message"] == "attempt_reaped" for e in entries),
        timeout=8,
    )
    reaped = next(e for e in entries if e["message"] == "attempt_reaped")
    elapsed = (
        datetime.fromisoformat(reaped["created_at"])
        - datetime.fromisoformat(start["attempt_started_at"])
    ).total_seconds()
    assert 5.3 <= elapsed < 6.3
    eventually(lambda: job(api, job_id), lambda j: j["status"] == "dispatched")
    current = token(api, job_id)
    assert current != old
    assert start["attempt_started_at"] is not None
    inject(
        "q.job-status",
        {"job_id": job_id, "claim_token": old, "event": "completed", "result": {"stale": True}},
    )
    inject(
        "q.job-status",
        {
            "job_id": job_id,
            "claim_token": current,
            "event": "completed",
            "result": {"current": True},
        },
    )
    result = finished(api, job_id)
    assert result["result"] == {"current": True} and result["total_attempts"] == 2


def test_multiple_workers_and_apis_normal_delivery_and_status_failover(api):
    compose("up", "-d", "--scale", "worker=3", "--scale", "api=2", "--no-recreate", "worker", "api")
    try:
        ready(api, consumers=3)
        consumers = eventually(
            lambda: management("consumers/%2F"),
            lambda entries: len([e for e in entries if e["queue"]["name"] == "q.job-status"]) == 2,
        )
        status_consumers = [e for e in consumers if e["queue"]["name"] == "q.job-status"]
        assert sum(e["active"] for e in status_consumers) == 1
        workers = [e for e in consumers if e["queue"]["name"] == "q.jobs"]
        assert len(workers) == 3 and all(e["prefetch_count"] == 1 for e in workers)
        with ThreadPoolExecutor(max_workers=6) as pool:
            ids = list(
                pool.map(
                    lambda _: submit(
                        api, "batch", {"items": list(range(30)), "_test": {"delay": 0.02}}
                    ),
                    range(6),
                )
            )
        observed = eventually(
            lambda: [job(api, i) for i in ids],
            lambda jobs: sum(j["status"] == "processing" for j in jobs) >= 3,
        )
        assert len([j for j in observed if j["status"] == "processing"]) == 3
        for job_id in ids:
            assert finished(api, job_id)["total_attempts"] == 1
        runs = [r for r in executions() if r["job_id"] in ids]
        assert sorted(r["job_id"] for r in runs) == sorted(ids)
        assert len({r["worker"] for r in runs}) == 3
        ends = {
            r["job_id"]: datetime.fromisoformat(r["timestamp"])
            for r in executions("final_event_confirmed")
            if r["job_id"] in ids
        }
        intervals = [(datetime.fromisoformat(r["timestamp"]), ends[r["job_id"]]) for r in runs]
        assert (
            max(sum(start <= instant < end for start, end in intervals) for instant, _ in intervals)
            == 3
        )
        # First API connected first and owns SAC. Its exit promotes the standby.
        container = os.environ["COMPOSE_PROJECT_NAME"] + "-api-1"
        import subprocess

        subprocess.run(["docker", "stop", "-t", "10", container], check=True, capture_output=True)
        port = compose("port", "--index", "2", "api", "8000").stdout.strip().rsplit(":", 1)[1]
        import httpx

        with httpx.Client(
            base_url=f"http://127.0.0.1:{port}",
            timeout=10,
            headers={"Authorization": f"Bearer {os.environ['API_TOKEN']}"},
        ) as second:
            ready(second, consumers=3, refresh=False)
            assert finished(second, submit(second))["total_attempts"] == 1
            remaining = eventually(
                lambda: management("consumers/%2F"),
                lambda entries: (
                    len([e for e in entries if e["queue"]["name"] == "q.job-status"]) == 1
                    and any(e["active"] for e in entries if e["queue"]["name"] == "q.job-status")
                ),
            )
            assert next(e for e in remaining if e["queue"]["name"] == "q.job-status")["active"]
    finally:
        compose("start", "api")
        remove_replica("api", 2)
        remove_replica("worker", 2)
        remove_replica("worker", 3)
        ready(api, consumers=1)


def test_pre_start_dead_letter_does_not_invent_an_attempt(api, stopped_workers):
    job_id = submit(api)
    eventually(lambda: job(api, job_id), lambda j: j["status"] == "dispatched")
    inject("q.jobs.dlq", {"job_id": 123, "claim_token": None})
    inject("q.jobs.dlq", {"job_id": job_id, "claim_token": token(api, job_id)})
    result = finished(api, job_id, "failed")
    assert result["attempts"] == result["total_attempts"] == 0
    assert result["started_at"] is None and result["error"]["code"] == "dead_letter"
    assert [e["message"] for e in logs(api, job_id)] == ["submitted", "dispatched", "failed"]


def test_invalid_current_token_events_do_not_start_or_mutate_job(api, stopped_workers):
    job_id = submit(api)
    eventually(lambda: job(api, job_id), lambda j: j["status"] == "dispatched")
    base = {"job_id": job_id, "claim_token": token(api, job_id)}
    before = job(api, job_id)
    history = logs(api, job_id)
    for event in [
        {"event": "completed"},
        {"event": "failed"},
        {"event": "progress"},
        {"event": "progress", "progress": 101},
        {"event": "unknown"},
        {"event": "completed", "result": {"nested": ["nul\0value"]}},
        {"event": "completed", "result": {"nested": {"nul\0key": "value"}}},
        {"event": "failed", "error": {"code": "nul\0code", "message": "bad", "transient": False}},
        {"event": "failed", "error": {"code": "bad", "message": "nul\0text", "transient": False}},
    ]:
        inject("q.job-status", {**base, **event})
    inject(
        "q.job-status",
        (
            '{"job_id":"'
            + job_id
            + '","claim_token":"'
            + base["claim_token"]
            + '","event":"completed","result":{"invalid":NaN}}'
        ).encode(),
    )
    # A valid event after the poison messages proves useful consumption resumes.
    inject("q.job-status", {**base, "event": "progress", "progress": 50})
    next_id = submit(api)
    eventually(lambda: job(api, next_id), lambda j: j["status"] == "dispatched")
    inject(
        "q.job-status",
        {
            "job_id": next_id,
            "claim_token": token(api, next_id),
            "event": "completed",
            "result": {"alive": True},
        },
    )
    finished(api, next_id)
    assert job(api, job_id) == before and logs(api, job_id) == history
    inject("q.job-status", {**base, "event": "completed", "result": {"valid": True}})
    assert finished(api, job_id)["total_attempts"] == 1


def test_automatic_retry_resets_progress_and_clears_error_when_next_start_is_accepted(
    api, stopped_workers
):
    job_id = submit(api, "batch", {"items": ["a"]}, max_attempts=2)
    eventually(lambda: job(api, job_id), lambda j: j["status"] == "dispatched")
    old = token(api, job_id)
    base = {"job_id": job_id, "claim_token": old}
    inject("q.job-status", {**base, "event": "processing"})
    inject("q.job-status", {**base, "event": "progress", "progress": 80})
    inject(
        "q.job-status",
        {
            **base,
            "event": "failed",
            "error": {
                "code": "retry",
                "message": "Transient controlled failure",
                "transient": True,
            },
        },
    )
    waiting = eventually(lambda: job(api, job_id), lambda j: j["status"] == "pending")
    assert waiting["error"]["code"] == "retry" and waiting["progress"] == 80
    assert waiting["result"] is None
    eventually(lambda: job(api, job_id), lambda j: j["status"] == "dispatched")
    current = token(api, job_id)
    dispatched = job(api, job_id)
    assert current != old and dispatched["progress"] == 0
    assert dispatched["error"]["code"] == "retry"
    inject("q.job-status", {"job_id": job_id, "claim_token": current, "event": "processing"})
    started = eventually(lambda: job(api, job_id), lambda j: j["status"] == "processing")
    assert started["error"] is None and started["progress"] == 0
    assert started["attempts"] == started["total_attempts"] == 2
    assert started["started_at"] == waiting["started_at"]
    inject("q.job-status", {**base, "event": "progress", "progress": 99})
    inject(
        "q.job-status",
        {
            "job_id": job_id,
            "claim_token": current,
            "event": "completed",
            "result": {"processed_count": 1, "summary": ["a"]},
        },
    )
    assert finished(api, job_id)["progress"] == 100


def test_reaper_racing_terminal_outcome_preserves_one_guarded_winner(api, stopped_workers):
    for _ in range(3):
        job_id = submit(api, max_attempts=2)
        eventually(lambda job_id=job_id: job(api, job_id), lambda j: j["status"] == "dispatched")
        old = token(api, job_id)
        inject("q.job-status", {"job_id": job_id, "claim_token": old, "event": "processing"})
        started = eventually(
            lambda job_id=job_id: job(api, job_id), lambda j: j["status"] == "processing"
        )
        start = datetime.fromisoformat(started["attempt_started_at"])
        eventually(
            lambda start=start: (datetime.now(UTC) - start).total_seconds(), lambda t: t >= 5.25
        )
        inject(
            "q.job-status",
            {
                "job_id": job_id,
                "claim_token": old,
                "event": "completed",
                "result": {"winner": "execution"},
            },
        )
        outcome = eventually(
            lambda job_id=job_id: job(api, job_id), lambda j: j["status"] != "processing"
        )
        if outcome["status"] != "completed":
            eventually(
                lambda job_id=job_id: job(api, job_id), lambda j: j["status"] == "dispatched"
            )
            current = token(api, job_id)
            assert current != old
            inject(
                "q.job-status",
                {
                    "job_id": job_id,
                    "claim_token": current,
                    "event": "completed",
                    "result": {"winner": "recovery"},
                },
            )
        done = finished(api, job_id)
        history = logs(api, job_id)
        old_outcomes = [
            e["message"]
            for e in history
            if e["claim_token"] == old and e["message"] in ("completed", "attempt_reaped")
        ]
        assert old_outcomes in (["completed"], ["attempt_reaped"])
        assert done["total_attempts"] == (1 if old_outcomes == ["completed"] else 2)
        eventually(
            lambda start=start: (datetime.now(UTC) - start).total_seconds(), lambda t: t >= 6.3
        )
        assert job(api, job_id) == done and logs(api, job_id) == history
