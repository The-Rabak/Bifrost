from uuid import uuid4

import pytest
from conftest import finished, job, submit


def test_submit_and_complete_all_handlers(api):
    cases = [
        ("email", {"to": "a@example.com"}, "message_id"),
        ("webhook", {"url": "https://example.com/hook", "_test": {"fail_first": 0}}, "status_code"),
        ("report", {"parameters": {"period": "monthly"}}, "file_url"),
        ("batch", {"items": ["a", "b"]}, "processed_count"),
        ("batch", {"items": []}, "summary"),
        ("batch", {"items": ["nullable"], "_test": {"delay": None}}, "processed_count"),
        ("batch", {"items": [r"\u0000", {r"\u0000": "literal"}]}, "summary"),
    ]
    for kind, payload, result_key in cases:
        job_id = submit(api, kind, payload)
        result = finished(api, job_id)
        assert result_key in result["result"]
        if kind == "email":
            assert result["result"] == {"message_id": f"mock-{job_id}"}
        elif kind == "report":
            assert result["result"] == {"file_url": f"https://reports.invalid/{job_id}.pdf"}
        elif kind == "webhook":
            assert result["result"] == {"status_code": 200}
        assert result["progress"] == 100
        assert result["attempts"] == result["total_attempts"] == 1
        assert result["started_at"] and result["completed_at"]
        if kind == "batch":
            assert result["result"] == {
                "processed_count": len(payload["items"]),
                "summary": payload["items"],
            }
        logs = api.get(f"/jobs/{job_id}/logs").json()
        assert [entry["message"] for entry in logs["items"]] == [
            "submitted",
            "dispatched",
            "processing",
            "completed",
        ]
        assert logs["items"] == sorted(logs["items"], key=lambda e: (e["created_at"], e["id"]))
        first = api.get(f"/jobs/{job_id}/logs?limit=2").json()
        second = api.get(f"/jobs/{job_id}/logs?limit=2&offset=2").json()
        assert first["items"] + second["items"] == logs["items"]
        assert first["total"] == second["total"] == 4
    assert api.get(f"/jobs/{uuid4()}").status_code == 404


@pytest.mark.parametrize(
    "change",
    [
        {"type": "unknown"},
        {"priority": -1},
        {"priority": 32},
        {"priority": True},
        {"max_attempts": 0},
        {"max_attempts": 6},
        {"max_attempts": 1.2},
        {"idempotency_key": ""},
        {"idempotency_key": "x" * 201},
        {"idempotency_key": "nul\0key"},
        {"payload": {"to": "a@example.com", "subject": "nul\0subject"}},
        {"payload": {"to": "a@example.com", "body": "nul\0body"}},
        {"type": "batch", "payload": {"items": [{"nested": ["nul\0value"]}]}},
        {"type": "batch", "payload": {"items": [{"nul\0key": "value"}]}},
        {"type": "webhook", "payload": {"url": "https://example.com", "data": {"x": "\0"}}},
        {"type": "webhook", "payload": {"url": "https://example.com", "data": {"\0": "x"}}},
        {"type": "report", "payload": {"parameters": {"nested": {"\0": "x"}}}},
        {"scheduled_for": "2030-01-01T00:00:00"},
        {"payload": {"to": "bad"}},
        {"payload": {"to": "a@example.com", "extra": 1}},
        {"type": "webhook", "payload": {"url": "ftp://example.com"}},
        {"type": "report", "payload": {"parameters": []}},
        {"type": "batch", "payload": {"items": "bad"}},
        {"payload": {"to": "a@example.com", "_test": None}},
    ],
)
def test_rejects_invalid_submission_without_creating_job(api, change):
    before = api.get("/jobs").json()["total"]
    response = api.post(
        "/jobs", json={"type": "email", "payload": {"to": "a@example.com"}, **change}
    )
    assert response.status_code == 422, response.text
    assert response.json()["error"]["code"] == "validation_error"
    assert api.get("/jobs").json()["total"] == before


def test_authentication_and_collection_validation(api):
    assert api.get("/jobs", headers={b"Authorization": b"Bearer \xff"}).status_code == 401
    for headers in [{}, {"Authorization": "Bearer wrong"}]:
        response = api.get("/jobs", headers={"Authorization": headers.get("Authorization", "")})
        assert response.status_code == 401
        assert response.json()["error"]["code"] == "unauthorized"
    assert api.post("/jobs", content="{").status_code == 422
    before = api.get("/jobs").json()["total"]
    assert (
        api.post(
            "/jobs",
            content='{"type":"batch","payload":{"items":[NaN]}}',
            headers={"Content-Type": "application/json"},
        ).status_code
        == 422
    )
    assert api.get("/jobs").json()["total"] == before
    for query in ["limit=101", "limit=0", "offset=-1", "status=nonsense", "type=bad"]:
        assert api.get("/jobs?" + query).status_code == 422
    assert api.get("/health", headers={"Authorization": ""}).status_code == 200


@pytest.mark.parametrize("containers", [62, 63, 200])
@pytest.mark.parametrize("shape", ["arrays", "objects", "mixed"])
def test_json_nesting_boundary_survives_work_and_result_envelopes(api, containers, shape):
    item = "leaf"
    for index in range(containers):
        item = {"child": item} if shape == "objects" or (shape == "mixed" and index % 2) else [item]
    payload = {"items": [item]}
    before = api.get("/jobs").json()["total"]
    response = api.post("/jobs", json={"type": "batch", "payload": payload})
    if containers > 62:  # Payload object and items array use the other two container levels.
        assert response.status_code == 422, response.text
        assert api.get("/jobs").json()["total"] == before
    else:
        assert response.status_code == 202, response.text
        result = finished(api, response.json()["id"])
        assert result["total_attempts"] == 1
        assert result["result"] == {"processed_count": 1, "summary": [item]}


def test_pagination_filtering_and_defaults(api):
    job_id = submit(api, "batch", {"items": []}, priority=0, max_attempts=5)
    finished(api, job_id)
    result = job(api, job_id)
    assert result["priority"] == 0 and result["max_attempts"] == 5
    assert result["manual_retry_used"] is False
    page = api.get("/jobs?type=batch&status=completed&limit=1").json()
    assert page["limit"] == 1 and page["offset"] == 0 and page["total"] >= 1
    assert page["items"][0]["id"] == job_id
    assert api.get("/jobs").json()["limit"] == 50
    rows = api.get("/jobs?limit=100").json()["items"]
    assert rows == sorted(rows, key=lambda j: (j["created_at"], j["id"]), reverse=True)
    assert api.get("/jobs?limit=1&offset=1").json()["items"] == rows[1:2]
    ids = [j["id"] for j in rows]
    assert len(ids) == len(set(ids))
    assert api.get(f"/jobs/{uuid4()}/logs").status_code == 404
