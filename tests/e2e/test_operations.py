import os
import subprocess
from datetime import UTC, datetime, timedelta

import httpx
from conftest import (
    compose,
    eventually,
    executions,
    finished,
    fixture_sql,
    health,
    inject,
    job,
    management,
    ready,
    submit,
    token,
)
from test_lifecycle import logs


def test_health_reports_dependencies_capacity_and_queue_depth(api, stopped_workers):
    assert health(api).get("database") == "connected"
    state = ready(api, consumers=0)
    assert state["status"] == "degraded"
    assert set(state["job_counts"]) == {
        "scheduled",
        "pending",
        "dispatched",
        "processing",
        "completed",
        "failed",
        "cancelled",
    }
    assert state["queues"]["q.jobs"]["ready"] >= 0
    assert state["queues"]["q.jobs"]["unacknowledged"] == 0
    assert state["queues"]["q.job-status"]["ready"] >= 0
    assert state["queues"]["q.jobs.dlq"]["ready"] >= 0
    queued = submit(api)
    eventually(lambda: job(api, queued), lambda j: j["status"] == "dispatched")
    for dependency in ["db", "broker", "broker"]:
        compose("stop", "-t", "10", dependency)
        try:
            response = api.get("/health")
            assert response.status_code == 503, response.text
            assert response.json()["database" if dependency == "db" else "broker"] == "unavailable"
        finally:
            compose("start", dependency)
        ready(api, consumers=0)
    compose("start", "worker")
    assert finished(api, queued)["status"] == "completed"
    assert finished(api, submit(api))["status"] == "completed"


def test_missing_management_stats_degrades_without_false_dependency_failure(api):
    compose("exec", "-T", "broker", "rabbitmq-plugins", "disable", "rabbitmq_management")
    try:
        response = api.get("/health")
        assert response.status_code == 200
        state = response.json()
        assert state["status"] == "degraded"
        assert state["broker"] == state["database"] == "connected"
        assert state["queues"] is None and state["statistics"] == "unavailable"
        assert finished(api, submit(api))["status"] == "completed"
    finally:
        compose("exec", "-T", "broker", "rabbitmq-plugins", "enable", "rabbitmq_management")
    ready(api, consumers=1)


def test_rate_limits_on_isolated_api_process(api):
    result = compose(
        "run",
        "-d",
        "--no-deps",
        "-p",
        "127.0.0.1::8000",
        "-e",
        "SUBMIT_LIMIT=2",
        "-e",
        "RETRY_LIMIT=1",
        "api",
    )
    container = result.stdout.strip().splitlines()[-1]
    try:
        port = subprocess.check_output(["docker", "port", container, "8000"], text=True).strip()
        with httpx.Client(
            base_url=f"http://{port}",
            timeout=10,
            headers={"Authorization": f"Bearer {os.environ['API_TOKEN']}"},
        ) as limited:
            ready(limited, refresh=False)
            for _ in range(2):
                submit(limited)
            response = limited.post(
                "/jobs", json={"type": "email", "payload": {"to": "a@example.com"}}
            )
            assert response.status_code == 429
            assert response.json()["error"]["code"] == "rate_limited"
            failed_id = submit(api, payload={"to": "a@example.com", "_test": {"permanent": True}})
            finished(api, failed_id, "failed")
            assert limited.post(f"/jobs/{failed_id}/retry").status_code == 202
            assert limited.post(f"/jobs/{failed_id}/retry").status_code == 429
    finally:
        subprocess.run(["docker", "rm", "-f", container], check=True, capture_output=True)


def test_graceful_worker_shutdown_finishes_current_and_stops_intake(api):
    job_id = submit(api, "batch", {"items": list(range(100)), "_test": {"delay": 0.02}})
    eventually(lambda: job(api, job_id), lambda j: j["status"] == "processing")
    next_id = submit(api)
    compose("stop", "-t", "10", "worker")
    try:
        assert finished(api, job_id)["total_attempts"] == 1
        assert job(api, next_id)["status"] in ("pending", "dispatched")
        assert not any(r["job_id"] == next_id for r in executions())
        state = ready(api, consumers=0)
        assert state["queues"]["q.jobs"]["unacknowledged"] == 0
    finally:
        compose("start", "worker")
    assert finished(api, next_id)["total_attempts"] == 1


def test_api_exits_before_worker_outcome_then_dependencies_restart_without_data_loss(api):
    job_id = submit(api, "batch", {"items": list(range(80)), "_test": {"delay": 0.02}})
    eventually(lambda: job(api, job_id), lambda j: j["status"] == "processing")
    compose("stop", "-t", "10", "api")
    eventually(
        lambda: compose("logs", "--no-color", "worker").stdout,
        lambda text: any(
            job_id in line and "work_acknowledged" in line for line in text.splitlines()
        ),
    )
    compose("start", "api")
    ready(api, consumers=1)
    done = finished(api, job_id)
    history = logs(api, job_id)
    compose("stop", "-t", "10", "worker")
    queued = submit(api)
    eventually(lambda: job(api, queued), lambda j: j["status"] == "dispatched")
    compose("stop", "-t", "10", "api", "broker", "db")
    compose("up", "-d", "--wait", "--wait-timeout", "120")
    ready(api, consumers=1)
    assert job(api, job_id) == done
    assert logs(api, job_id) == history
    assert finished(api, queued)["total_attempts"] == 1
    assert finished(api, submit(api))["total_attempts"] == 1


def test_malformed_work_does_not_block_next_valid_delivery(api):
    inject("q.jobs", b"not-json")
    inject("q.jobs", {"job_id": "bad", "type": "email"})
    job_id = submit(api)
    assert finished(api, job_id)["total_attempts"] == 1
    ready(api, consumers=1)


def test_cleanup_releases_only_old_terminal_keys_and_aged_logs(api):
    old = submit(api, idempotency_key="cleanup-old")
    young = submit(api, idempotency_key="cleanup-young")
    active = submit(
        api,
        idempotency_key="cleanup-active",
        scheduled_for=(datetime.now(UTC) + timedelta(days=60)).isoformat(),
    )
    finished(api, old)
    finished(api, young)
    fixture_sql(f"""
        UPDATE jobs SET created_at=now()-interval '31 days',
                        completed_at=now()-interval '31 days' WHERE id='{old}';
        UPDATE jobs SET created_at=now()-interval '31 days' WHERE id='{active}';
        UPDATE job_logs SET created_at=now()-interval '31 days' WHERE job_id='{old}';
    """)
    eventually(lambda: job(api, old), lambda j: j["idempotency_key"] is None)
    eventually(lambda: logs(api, old), lambda entries: len(entries) == 0)
    assert job(api, young)["idempotency_key"] == "cleanup-young"
    assert job(api, active)["idempotency_key"] == "cleanup-active"
    assert logs(api, young)
    replacement = submit(api, idempotency_key="cleanup-old")
    assert replacement != old
    assert finished(api, replacement)["status"] == "completed"
    assert api.post(f"/jobs/{active}/cancel").status_code == 200


def test_container_security_and_repeatable_initialization(api):
    for service in ["api", "worker"]:
        assert compose("exec", "-T", service, "id", "-u").stdout.strip() == "10001"
    configuration = compose("-f", "compose.yaml", "config", "--format", "json").stdout
    import json

    config = json.loads(configuration)
    assert "DATABASE_URL" not in config["services"]["worker"]["environment"]
    assert "REDIS_URL" not in config["services"]["api"]["environment"]
    assert not config["services"]["db"].get("ports")
    assert not config["services"]["broker"].get("ports")
    assert not config["services"]["redis"].get("ports")
    assert not (
        set(config["services"]["redis"]["networks"]) & set(config["services"]["api"]["networks"])
    )
    compose("restart", "api")
    ready(api, consumers=1)
    assert finished(api, submit(api))["total_attempts"] == 1


def test_live_workers_reconnect_after_temporary_broker_outage(api):
    ready(api, consumers=1)
    compose("stop", "-t", "10", "broker")
    try:
        assert api.get("/health").status_code == 503
    finally:
        compose("start", "broker")
    ready(api, consumers=1)
    assert finished(api, submit(api))["total_attempts"] == 1


def test_unexpected_background_error_is_visible_and_restart_recovers_reservation(api):
    import json

    from conftest import hook, unhook

    identifier = compose("ps", "-q", "api").stdout.strip()
    before = json.loads(subprocess.check_output(["docker", "inspect", identifier]))[0][
        "RestartCount"
    ]
    hook("api", "after_reserve", "raise")
    job_id = submit(api)
    eventually(
        lambda: json.loads(subprocess.check_output(["docker", "inspect", identifier]))[0][
            "RestartCount"
        ],
        lambda count: count > before,
    )
    ready(api, consumers=1)
    unhook("api", "after_reserve")
    assert finished(api, job_id)["total_attempts"] == 1
    output = compose("logs", "--no-color", "api").stdout
    assert "unexpected background failure" in output and "Test-injected programming error" in output


def test_graceful_api_shutdown_finishes_active_status_transaction_and_acknowledgement(
    api, stopped_workers
):
    from concurrent.futures import ThreadPoolExecutor

    from conftest import hook, reached, unhook

    job_id = submit(api)
    hook("api", "before_status_commit", job_id)
    eventually(lambda: job(api, job_id), lambda j: j["status"] == "dispatched")
    inject(
        "q.job-status",
        {
            "job_id": job_id,
            "claim_token": token(api, job_id),
            "event": "completed",
            "result": {"graceful": True},
        },
    )
    eventually(lambda: reached("api", "before_status_commit", job_id), bool)
    assert job(api, job_id)["status"] == "dispatched"
    eventually(
        lambda: management("queues/%2F/q.job-status")["messages_unacknowledged"],
        lambda count: count == 1,
    )
    since = datetime.now(UTC).isoformat()
    with ThreadPoolExecutor(max_workers=1) as pool:
        stopping = pool.submit(compose, "stop", "-t", "10", "api")
        eventually(
            lambda: compose("logs", "--since", since, "--no-color", "api").stdout,
            lambda output: "Waiting for application shutdown" in output,
        )
        unhook("api", "before_status_commit")
        stopping.result(timeout=15)
    eventually(
        lambda: management("queues/%2F/q.job-status"),
        lambda queue: queue["messages_ready"] + queue["messages_unacknowledged"] == 0,
    )
    compose("start", "api")
    ready(api, consumers=0)
    assert finished(api, job_id)["result"] == {"graceful": True}
    assert [e["message"] for e in logs(api, job_id)] == [
        "submitted",
        "dispatched",
        "processing",
        "completed",
    ]


def test_graceful_api_shutdown_finishes_reserved_handoff_and_leaves_backlog(api, stopped_workers):
    import json
    from concurrent.futures import ThreadPoolExecutor

    from conftest import reached, unhook

    ready(api, consumers=0)
    first = submit(api, payload={"to": "a@example.com", "_test": {"pause_at": "after_reserve"}})
    eventually(lambda: reached("api", "after_reserve", first), bool)
    backlog = [submit(api) for _ in range(3)]
    assert all(job(api, identifier)["status"] == "pending" for identifier in backlog)
    since = datetime.now(UTC).isoformat()
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            stopping = pool.submit(compose, "stop", "-t", "10", "api")
            try:
                eventually(
                    lambda: compose("logs", "--since", since, "--no-color", "api").stdout,
                    lambda output: "Waiting for application shutdown" in output,
                )
            finally:
                unhook("api", "after_reserve")
            stopping.result(timeout=15)
        identifier = compose("ps", "-aq", "api").stdout.strip()
        state = json.loads(subprocess.check_output(["docker", "inspect", identifier]))[0]["State"]
        assert state["ExitCode"] == 0, state
        eventually(
            lambda: management("queues/%2F/q.jobs")["messages_ready"],
            lambda count: count == 1,
        )
        output = compose("logs", "--no-color", "api").stdout
        assert not any(
            '"message": "dispatched"' in line and any(job_id in line for job_id in backlog)
            for line in output.splitlines()
        )
    finally:
        compose("start", "api")
        ready(api, consumers=0)
    compose("start", "worker")
    for job_id in [first, *backlog]:
        assert finished(api, job_id)["total_attempts"] == 1
