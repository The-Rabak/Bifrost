import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Barrier
from uuid import uuid4

import pytest
from conftest import eventually, finished, job, submit


def logs(api, job_id):
    return api.get(f"/jobs/{job_id}/logs?limit=100").json()["items"]


def test_idempotency_race_preserves_original_attributes_and_current_state(api):
    key = str(uuid4())
    schedule = (datetime.now(UTC) + timedelta(seconds=3)).isoformat()
    request = {
        "type": "email",
        "payload": {"to": "original@example.com"},
        "priority": 31,
        "max_attempts": 1,
        "scheduled_for": schedule,
        "idempotency_key": key,
    }
    requests = [
        {
            **request,
            "payload": {"to": f"race-{i}@example.com"},
            "priority": i,
            "max_attempts": i % 5 + 1,
        }
        for i in range(12)
    ]
    barrier = Barrier(12)

    def send(candidate):
        barrier.wait(timeout=10)
        return api.post("/jobs", json=candidate)

    with ThreadPoolExecutor(max_workers=12) as pool:
        responses = list(pool.map(send, requests))
    assert sorted(r.status_code for r in responses) == [200] * 11 + [202]
    assert len({r.json()["id"] for r in responses}) == 1
    request = requests[next(i for i, r in enumerate(responses) if r.status_code == 202)]
    job_id = responses[0].json()["id"]
    duplicate = api.post(
        "/jobs",
        json={**request, "payload": {"to": "other@example.com"}, "priority": 0, "max_attempts": 5},
    )
    assert duplicate.status_code == 200 and duplicate.json()["deduplicated"] is True
    original = job(api, job_id)
    assert original["payload"] == request["payload"]
    assert original["priority"] == request["priority"]
    assert original["max_attempts"] == request["max_attempts"]
    assert original["status"] == "scheduled" and original["attempts"] == 0
    done = finished(api, job_id)
    assert datetime.fromisoformat(done["attempt_started_at"]) >= datetime.fromisoformat(schedule)
    assert api.post("/jobs", json=request).json()["status"] == "completed"


def test_cancel_future_and_race_dispatch(api):
    job_id = submit(api, scheduled_for=(datetime.now(UTC) + timedelta(hours=1)).isoformat())
    assert api.post(f"/jobs/{job_id}/retry").status_code == 409
    assert api.post(f"/jobs/{job_id}/cancel").status_code == 200
    assert job(api, job_id)["status"] == "cancelled"
    assert api.post(f"/jobs/{job_id}/retry").status_code == 409
    assert api.post(f"/jobs/{job_id}/cancel").status_code == 409
    assert api.post(f"/jobs/{uuid4()}/cancel").status_code == 404
    for _ in range(8):
        job_id = submit(api)
        response = api.post(f"/jobs/{job_id}/cancel")
        assert response.status_code in (200, 409)
        final = finished(api, job_id, "cancelled" if response.status_code == 200 else "completed")
        if response.status_code == 200:
            assert final["attempts"] == 0 and final["started_at"] is None
            assert [e["message"] for e in logs(api, job_id)] == ["submitted", "cancelled"]
        else:
            assert final["attempts"] == 1


@pytest.mark.parametrize("budget", [1, 3, 5])
def test_exhausts_retry_budget_and_backoff(api, budget):
    job_id = submit(
        api,
        payload={"to": "a@example.com", "_test": {"fail_first": 10}},
        max_attempts=budget,
        priority=30,
    )
    result = finished(api, job_id, "failed", timeout=35)
    assert result["attempts"] == result["total_attempts"] == budget
    assert result["error"]["code"] == "mock_transient"
    assert result["priority"] == 30
    entries = logs(api, job_id)
    starts = [e for e in entries if e["message"] == "processing"]
    failures = [e for e in entries if e["message"] in ("retry_scheduled", "failed")]
    assert len(starts) == len(failures) == budget
    for index in range(budget - 1):
        delta = (
            datetime.fromisoformat(starts[index + 1]["created_at"])
            - datetime.fromisoformat(failures[index]["created_at"])
        ).total_seconds()
        assert delta >= 0.15 * 4**index - 0.01
    snapshot = (result["total_attempts"], len(entries))
    end = time.monotonic() + 0.15 * 4 ** (budget - 1) + 0.2
    while time.monotonic() < end:
        assert job(api, job_id)["status"] == "failed"
        time.sleep(0.05)
    assert (job(api, job_id)["total_attempts"], len(logs(api, job_id))) == snapshot


def test_manual_retry_race_grants_one_cycle_and_preserves_history(api):
    job_id = submit(api, payload={"to": "a@example.com", "_test": {"fail_first": 3}})
    failed = finished(api, job_id, "failed")
    assert failed["attempts"] == failed["total_attempts"] == 3
    barrier = Barrier(8)

    def retry(_):
        barrier.wait(timeout=10)
        return api.post(f"/jobs/{job_id}/retry")

    with ThreadPoolExecutor(max_workers=8) as pool:
        responses = list(pool.map(retry, range(8)))
    assert sorted(r.status_code for r in responses) == [202] + [409] * 7
    reset = next(r.json() for r in responses if r.status_code == 202)
    assert reset["attempts"] == 0 and reset["total_attempts"] == 3
    assert reset["result"] is None and reset["error"] is None and reset["progress"] == 0
    assert reset["completed_at"] is None and reset["attempt_started_at"] is None
    assert reset["started_at"] == failed["started_at"]
    done = finished(api, job_id)
    assert done["attempts"] == 1 and done["total_attempts"] == 4
    assert done["payload"] == failed["payload"] and done["priority"] == failed["priority"]
    assert done["started_at"] == failed["started_at"] and done["manual_retry_used"] is True
    assert done["error"] is None
    assert len([e for e in logs(api, job_id) if e["message"] == "processing"]) == 4
    assert api.post(f"/jobs/{job_id}/retry").status_code == 409
    assert api.post(f"/jobs/{uuid4()}/retry").status_code == 404


def test_permanent_failure_and_timeout(api):
    permanent = submit(api, payload={"to": "a@example.com", "_test": {"permanent": True}})
    assert finished(api, permanent, "failed")["attempts"] == 1
    assert api.post(f"/jobs/{permanent}/retry").status_code == 202
    assert finished(api, permanent, "failed")["total_attempts"] == 2
    assert api.post(f"/jobs/{permanent}/retry").status_code == 409
    for budget in (1, 2):
        timed = submit(
            api, payload={"to": "a@example.com", "_test": {"delay": 0.6}}, max_attempts=budget
        )
        result = finished(api, timed, "failed")
        assert result["error"]["code"] == "timeout" and result["attempts"] == budget


def test_progress_is_monotonic_and_failure_visible_during_backoff(api):
    job_id = submit(api, "batch", {"items": list(range(80)), "_test": {"delay": 0.01}})
    observed = []
    result = eventually(
        lambda: job(api, job_id),
        lambda j: observed.append(j["progress"]) is None and j["status"] == "completed",
    )
    assert observed == sorted(observed) and result["progress"] == 100
    failing = submit(api, payload={"to": "a@example.com", "_test": {"fail_first": 2}})
    waiting = eventually(
        lambda: job(api, failing), lambda j: j["status"] == "pending" and j["attempts"] >= 1
    )
    assert waiting["error"]["code"] == "mock_transient"
    assert waiting["result"] is None
    assert finished(api, failing)["attempts"] == 3


def test_both_default_cycles_exhaust_at_six_lifetime_attempts(api):
    job_id = submit(api, payload={"to": "a@example.com", "_test": {"fail_first": 10}})
    first = finished(api, job_id, "failed")
    assert first["attempts"] == first["total_attempts"] == 3
    assert api.post(f"/jobs/{job_id}/retry").status_code == 202
    second = finished(api, job_id, "failed")
    assert second["attempts"] == 3 and second["total_attempts"] == 6
    assert second["started_at"] == first["started_at"]
    assert len([e for e in logs(api, job_id) if e["message"] == "processing"]) == 6
    assert api.post(f"/jobs/{job_id}/retry").status_code == 409
