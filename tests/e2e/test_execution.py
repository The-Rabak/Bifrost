"""Execution deduplication observed through the real broker and worker processes."""

import json

import pytest
from conftest import (
    compose,
    eventually,
    executions,
    finished,
    hook,
    inject,
    job,
    reached,
    ready,
    remove_replica,
    submit,
    token,
    unhook,
)
from test_lifecycle import logs


def delivery(api, job_id):
    row = job(api, job_id)
    return {
        "job_id": job_id,
        "claim_token": token(api, job_id),
        "type": row["type"],
        "payload": row["payload"],
        "priority": row["priority"],
        "cycle": int(row["manual_retry_used"]),
        "attempt": row["attempts"] or 1,
        "total_attempt": row["total_attempts"] or 1,
    }


@pytest.mark.parametrize("permanent", [False, True])
def test_completed_delivery_replays_outcome_without_executing_again(api, permanent):
    job_id = submit(
        api, payload={"to": "a@example.com", "_test": {"permanent": permanent}}, max_attempts=1
    )
    done = finished(api, job_id, "failed" if permanent else "completed")
    history = logs(api, job_id)
    inject("q.jobs", delivery(api, job_id))
    eventually(
        lambda: [r for r in executions("work_acknowledged") if r["job_id"] == job_id],
        lambda records: len(records) == 2,
    )
    assert len([r for r in executions() if r["job_id"] == job_id]) == 1
    assert job(api, job_id) == done
    assert logs(api, job_id) == history


def test_competing_workers_execute_duplicate_deliveries_once(api, stopped_workers):
    job_id = submit(api, "batch", {"items": list(range(100)), "_test": {"delay": 0.01}})
    eventually(lambda: job(api, job_id), lambda row: row["status"] == "dispatched")
    message = delivery(api, job_id)
    for _ in range(3):
        inject("q.jobs", message)
    compose("up", "-d", "--scale", "worker=2", "--no-recreate", "worker")
    try:
        ready(api, consumers=2)
        assert finished(api, job_id)["total_attempts"] == 1
        eventually(
            lambda: [r for r in executions("work_acknowledged") if r["job_id"] == job_id],
            lambda records: len(records) == 4,
        )
        assert len([r for r in executions() if r["job_id"] == job_id]) == 1
    finally:
        remove_replica("worker", 2)


def test_redis_restart_preserves_completed_outcomes(api):
    job_id = submit(api)
    finished(api, job_id)
    compose("kill", "-s", "SIGKILL", "redis")
    compose("start", "redis")
    inject("q.jobs", delivery(api, job_id))
    eventually(
        lambda: [r for r in executions("work_acknowledged") if r["job_id"] == job_id],
        lambda records: len(records) == 2,
    )
    assert len([r for r in executions() if r["job_id"] == job_id]) == 1


def test_redis_outage_leaves_work_unexecuted_and_recovers_without_dead_letter(api):
    compose("stop", "-t", "10", "redis")
    try:
        job_id = submit(api)
        eventually(
            lambda: compose("logs", "--no-color", "worker").stdout,
            lambda output: "dependency unavailable: ConnectionError" in output,
        )
        ready(api, consumers=0)
        assert job(api, job_id)["status"] == "dispatched"
        assert not any(r["job_id"] == job_id for r in executions())
        assert [entry["message"] for entry in logs(api, job_id)] == ["submitted", "dispatched"]
    finally:
        compose("start", "redis")
    assert finished(api, job_id)["total_attempts"] == 1


def test_expired_owner_cannot_overwrite_a_successor_outcome(api):
    hook("api", "reaper_tick", "reaper")
    job_id = submit(
        api,
        payload={
            "to": "a@example.com",
            "_test": {"permanent": True, "pause_at": "during_execution"},
        },
        max_attempts=1,
    )
    try:
        eventually(lambda: reached("worker", "during_execution", job_id), bool)
        # Simulate expiry while the old process is still alive. Only test controls
        # differ on the second delivery, forcing an observable conflicting outcome.
        key = compose("exec", "-T", "redis", "redis-cli", "--scan", "--pattern", f"*:{job_id}")
        assert len(key.stdout.strip().splitlines()) == 1
        compose("exec", "-T", "redis", "redis-cli", "DEL", key.stdout.strip())
        duplicate = delivery(api, job_id)
        duplicate["payload"]["_test"] = {"permanent": False}
        inject("q.jobs", duplicate)
        compose("up", "-d", "--scale", "worker=2", "--no-recreate", "worker")
        assert finished(api, job_id)["total_attempts"] == 1
        unhook("worker", "during_execution")
        eventually(
            lambda: [r for r in executions("work_acknowledged") if r["job_id"] == job_id],
            lambda records: len(records) == 2,
        )
        saved = json.loads(
            compose("exec", "-T", "redis", "redis-cli", "GET", key.stdout.strip()).stdout
        )
        assert saved["event"] == "completed" and saved["error"] is None
        assert job(api, job_id)["status"] == "completed"
        assert len([r for r in executions() if r["job_id"] == job_id]) == 2
    finally:
        unhook("worker", "during_execution")
        unhook("api", "reaper_tick")
        remove_replica("worker", 2)
