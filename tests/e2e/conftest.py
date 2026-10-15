import asyncio
import json
import os
import subprocess
import time
from collections.abc import Callable

import aio_pika
import httpx
import pytest


def compose(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", "compose", "-p", os.environ["COMPOSE_PROJECT_NAME"], *args],
        capture_output=True,
        text=True,
        check=check,
        timeout=300,
    )


def redact(value):
    for key in ["DB_PASSWORD", "BROKER_PASSWORD", "API_TOKEN"]:
        value = value.replace(os.environ[key], "[REDACTED]")
    return value


@pytest.fixture(scope="session")
def api():
    port = compose("port", "api", "8000").stdout.strip().rsplit(":", 1)[1]
    with httpx.Client(
        base_url=f"http://127.0.0.1:{port}",
        timeout=10,
        headers={"Authorization": f"Bearer {os.environ['API_TOKEN']}"},
    ) as client:
        yield client


def eventually(read: Callable, accept: Callable, timeout: float = 30):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = read()
        if accept(last):
            return last
        time.sleep(0.05)
    pytest.fail(f"Deadline {timeout}s expired; last observation: {last!r}")


def submit(api, kind="email", payload=None, **options):
    response = api.post(
        "/jobs",
        json={
            "type": kind,
            "payload": payload if payload is not None else {"to": "a@example.com"},
            **options,
        },
    )
    assert response.status_code == 202, response.text
    return response.json()["id"]


def job(api, job_id):
    response = api.get(f"/jobs/{job_id}")
    assert response.status_code == 200, response.text
    return response.json()


def finished(api, job_id, status="completed", timeout=30):
    return eventually(lambda: job(api, job_id), lambda j: j["status"] == status, timeout)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    if report.failed:
        directory = os.environ.get("ARTIFACT_DIR", "artifacts")
        os.makedirs(directory, exist_ok=True)
        for name, args in [
            ("logs", ("logs", "--no-color", "--tail", "400")),
            ("containers", ("ps", "-a", "--format", "json")),
        ]:
            output = compose(*args, check=False).stdout
            with open(f"{directory}/{item.name}-{name}.txt", "w") as file:
                file.write(redact(output))
        with open(f"{directory}/{item.name}-failure.json", "w") as file:
            json.dump({"test": item.nodeid, "failure": redact(str(report.longrepr))}, file)
        try:
            state = management("queues/%2F")
        except (httpx.HTTPError, subprocess.SubprocessError, ValueError):
            state = {"statistics": "unavailable"}
        with open(f"{directory}/{item.name}-queues.json", "w") as file:
            file.write(redact(json.dumps(state)))


def inside(service: str, source: str, index: int = 1):
    return compose(
        "exec", "-T", "--index", str(index), service, "python", "-c", source
    ).stdout.strip()


def remove_replica(service, index):
    name = f"{os.environ['COMPOSE_PROJECT_NAME']}-{service}-{index}"
    subprocess.run(
        ["docker", "stop", "-t", "10", name], check=True, capture_output=True, text=True, timeout=20
    )
    subprocess.run(["docker", "rm", name], check=True, capture_output=True, text=True, timeout=20)


def inject(queue, value):
    body = value if isinstance(value, bytes) else json.dumps(value).encode()
    port = compose("port", "broker", "5672").stdout.strip().rsplit(":", 1)[1]

    async def send():
        connection = await aio_pika.connect(
            host="127.0.0.1",
            port=int(port),
            login=os.environ["BROKER_USER"],
            password=os.environ["BROKER_PASSWORD"],
            timeout=5,
        )
        async with connection, connection.channel(publisher_confirms=True) as channel:
            await channel.default_exchange.publish(
                aio_pika.Message(body=body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
                routing_key=queue,
                mandatory=True,
                timeout=5,
            )

    asyncio.run(send())


def management(path):
    port = compose("port", "broker", "15672").stdout.strip().rsplit(":", 1)[1]
    response = httpx.get(
        f"http://127.0.0.1:{port}/api/{path}",
        timeout=5,
        auth=(os.environ["BROKER_USER"], os.environ["BROKER_PASSWORD"]),
    )
    response.raise_for_status()
    return response.json()


def hook(service, point, job_id="*", index=1):
    inside(
        service,
        f"from pathlib import Path; Path('/tmp/hook-{point}').write_text({job_id!r})",
        index,
    )


def unhook(service, point, index=1):
    inside(
        service,
        f"from pathlib import Path; Path('/tmp/hook-{point}').unlink(missing_ok=True)",
        index,
    )


def reached(service, point, job_id, index=1):
    return (
        inside(
            service,
            f"from pathlib import Path; print(Path('/tmp/reached-{point}-{job_id}').exists())",
            index,
        )
        == "True"
    )


def token(api, job_id):
    entries = api.get(f"/jobs/{job_id}/logs?limit=100").json()["items"]
    return next(e["claim_token"] for e in reversed(entries) if e["message"] == "dispatched")


def health(api):
    try:
        return api.get("/health").json()
    except httpx.TransportError:
        return {"status": "unreachable"}


def ready(api, consumers=None, refresh=True):
    def observe():
        if refresh:
            mapping = compose("port", "api", "8000", check=False)
            if mapping.returncode or not mapping.stdout.strip():
                return {"status": "unreachable"}
            port = mapping.stdout.strip().rsplit(":", 1)[1]
            api.base_url = f"http://127.0.0.1:{port}"
        return health(api)

    return eventually(
        observe,
        lambda h: (
            h.get("database") == "connected"
            and h.get("broker") == "connected"
            and (
                consumers is None
                or (h.get("queues") or {}).get("q.jobs", {}).get("consumers") == consumers
            )
        ),
        timeout=90,
    )


def executions(message="physical_execution_started"):
    output = compose("logs", "--no-color", "--no-log-prefix", "worker").stdout
    records = []
    for line in output.splitlines():
        try:
            record = json.loads(line)
        except ValueError:
            continue
        if record.get("message") == message:
            records.append(record)
    return records


def fixture_sql(sql):
    # Only explicitly aged housekeeping fixtures; behavioral assertions still use HTTP.
    return inside(
        "api",
        f"""
import asyncio, asyncpg, os
async def main():
    connection = await asyncpg.connect(os.environ['DATABASE_URL'])
    try:
        await connection.execute({sql!r})
    finally:
        await connection.close()
asyncio.run(main())
""",
    )


@pytest.fixture
def stopped_workers():
    compose("stop", "-t", "10", "worker")
    yield
    compose("start", "worker")
