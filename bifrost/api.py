import asyncio
import secrets
import time
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from typing import Annotated
from uuid import UUID

import aio_pika
import httpx
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from bifrost import store
from bifrost.background import dead_letters, dispatch, reap, statuses
from bifrost.broker import topology
from bifrost.config import settings
from bifrost.models import JobType, Status, Submission, public_job, utcnow
from bifrost.runtime import configure_logging, dependency_failure, supervise


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    config = settings()
    if not config.database_url or not config.api_token:
        raise ValueError("API requires DATABASE_URL and API_TOKEN")
    pool = await store.open_pool(config.database_url)
    await store.initialize(pool)
    broker = await aio_pika.connect_robust(
        config.broker_url, timeout=5, fail_fast=False, reconnect_interval=1
    )
    channel = await broker.channel(publisher_confirms=True)
    await topology(channel)
    stop = asyncio.Event()
    app.state.pool, app.state.broker = pool, broker
    app.state.management = httpx.AsyncClient(
        base_url=config.management_url,
        auth=(config.broker_user, config.broker_password),
        timeout=2,
    )
    tasks = [
        asyncio.create_task(
            supervise(
                "scheduler", lambda: dispatch(pool, channel, config), stop, config.tick_seconds
            )
        ),
        asyncio.create_task(
            supervise("status", lambda: statuses(pool, broker, config, stop), stop)
        ),
        asyncio.create_task(
            supervise("reaper", lambda: reap(pool, config), stop, config.reaper_seconds)
        ),
        asyncio.create_task(
            supervise("dlq", lambda: dead_letters(pool, broker, config, stop), stop)
        ),
        asyncio.create_task(
            supervise("cleanup", lambda: store.cleanup(pool), stop, config.cleanup_seconds)
        ),
    ]
    try:
        yield
    finally:
        stop.set()
        await asyncio.gather(*tasks)
        await channel.close()
        await broker.close()
        await app.state.management.aclose()
        await pool.close()


app = FastAPI(lifespan=lifespan)


def error_response(code, message, status, details=None):
    body = {"code": code, "message": message}
    if details is not None:
        body["details"] = details
    return JSONResponse({"error": body}, status_code=status)


@app.exception_handler(HTTPException)
async def http_error(request, error):
    codes = {401: "unauthorized", 404: "not_found", 409: "conflict", 429: "rate_limited"}
    return error_response(
        codes.get(error.status_code, "request_error"), error.detail, error.status_code
    )


@app.exception_handler(RequestValidationError)
async def validation_error(request, error):
    details = [{"location": list(item["loc"]), "message": item["msg"]} for item in error.errors()]
    return error_response("validation_error", "Invalid request", 422, details)


async def authenticate(request: Request):
    expected = "Bearer " + settings().api_token
    if not secrets.compare_digest(
        request.headers.get("Authorization", "").encode(), expected.encode()
    ):
        raise HTTPException(401, "Bearer token required")


Auth = Depends(authenticate)
hits: dict[str, deque] = defaultdict(deque)


def rate_limit(action: str, limit: int):
    now = time.monotonic()
    window = hits[action]
    while window and window[0] <= now - 60:
        window.popleft()
    if len(window) >= limit:
        raise HTTPException(429, "Per-process rate limit exceeded")
    window.append(now)


@app.post("/jobs", dependencies=[Auth])
async def submit(request: Request, body: Submission):
    rate_limit("submit", settings().submit_limit)
    row, duplicate = await store.submit(request.app.state.pool, body)
    return JSONResponse(
        {"id": str(row["id"]), "status": row["status"], "deduplicated": duplicate},
        status_code=200 if duplicate else 202,
    )


@app.get("/jobs", dependencies=[Auth])
async def listing(
    request: Request,
    status: Status | None = None,
    type: JobType | None = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
):
    pool = request.app.state.pool
    where = "WHERE ($1::text IS NULL OR status=$1) AND ($2::text IS NULL OR type=$2)"
    async with pool.acquire() as connection, connection.transaction(isolation="repeatable_read"):
        total = await connection.fetchval("SELECT count(*) FROM jobs " + where, status, type)
        rows = await connection.fetch(
            "SELECT * FROM jobs " + where + " ORDER BY created_at DESC,id DESC LIMIT $3 OFFSET $4",
            status,
            type,
            limit,
            offset,
        )
    return {
        "items": [public_job(row) for row in rows],
        "limit": limit,
        "offset": offset,
        "total": total,
    }


async def require_job(pool, job_id):
    row = await pool.fetchrow("SELECT * FROM jobs WHERE id=$1", job_id)
    if not row:
        raise HTTPException(404, "Job not found")
    return row


@app.get("/jobs/{job_id}", dependencies=[Auth])
async def get_job(request: Request, job_id: UUID):
    return public_job(await require_job(request.app.state.pool, job_id))


@app.get("/jobs/{job_id}/logs", dependencies=[Auth])
async def logs(
    request: Request,
    job_id: UUID,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
):
    async with request.app.state.pool.acquire() as connection:
        await require_job(connection, job_id)
        async with connection.transaction(isolation="repeatable_read"):
            total = await connection.fetchval(
                "SELECT count(*) FROM job_logs WHERE job_id=$1", job_id
            )
            rows = await connection.fetch(
                """SELECT * FROM job_logs WHERE job_id=$1
                ORDER BY created_at,id LIMIT $2 OFFSET $3""",
                job_id,
                limit,
                offset,
            )
    return {"items": [dict(row) for row in rows], "limit": limit, "offset": offset, "total": total}


@app.post("/jobs/{job_id}/cancel", dependencies=[Auth])
async def cancel(request: Request, job_id: UUID):
    async with request.app.state.pool.acquire() as connection, connection.transaction():
        row = await connection.fetchrow("SELECT * FROM jobs WHERE id=$1 FOR UPDATE", job_id)
        if not row:
            raise HTTPException(404, "Job not found")
        if row["status"] not in ("pending", "scheduled"):
            raise HTTPException(409, "Job has passed the cancellation cutoff")
        row = await store.change(
            connection, row, {"status": "cancelled", "completed_at": utcnow()}, "cancelled"
        )
    return public_job(row)


@app.post("/jobs/{job_id}/retry", dependencies=[Auth], status_code=202)
async def retry(request: Request, job_id: UUID):
    rate_limit("retry", settings().retry_limit)
    async with request.app.state.pool.acquire() as connection, connection.transaction():
        row = await connection.fetchrow("SELECT * FROM jobs WHERE id=$1 FOR UPDATE", job_id)
        if not row:
            raise HTTPException(404, "Job not found")
        if row["status"] != "failed" or row["manual_retry_used"]:
            raise HTTPException(409, "One manual retry is allowed from failed")
        row = await store.change(
            connection,
            row,
            {
                "status": "pending",
                "manual_retry_used": True,
                "attempts": 0,
                "result": None,
                "error": None,
                "progress": 0,
                "completed_at": None,
                "claim_token": None,
                "recover_at": None,
                "publish_confirmed": False,
                "attempt_started_at": None,
                "attempt_deadline": None,
                "next_attempt_at": utcnow(),
            },
            "manual_retry",
        )
    return public_job(row)


@app.get("/health")
async def health(request: Request):
    database, broker = "connected", "connected"
    counts = {state.value: 0 for state in Status}
    try:
        async with request.app.state.pool.acquire(timeout=2) as connection:
            rows = await connection.fetch(
                "SELECT status,count(*) AS count FROM jobs GROUP BY status", timeout=2
            )
            counts.update({row["status"]: row["count"] for row in rows})
    except Exception as error:
        if not dependency_failure(error):
            raise
        database = "unavailable"
    try:
        async with asyncio.timeout(2):
            await request.app.state.broker.ready()
            async with request.app.state.broker.channel() as channel:
                await channel.declare_queue("q.jobs", passive=True)
    except Exception as error:
        if not dependency_failure(error):
            raise
        broker = "unavailable"
    queues = None
    statistics_error = None
    try:
        response = await request.app.state.management.get("/api/queues/%2F")
        response.raise_for_status()
        queues = {
            queue["name"]: {
                "ready": queue["messages_ready"],
                "unacknowledged": queue["messages_unacknowledged"],
                "consumers": queue["consumers"],
            }
            for queue in response.json()
            if queue["name"] in ("q.jobs", "q.job-status", "q.jobs.dlq")
        }
        if len(queues) != 3:
            queues = None
            statistics_error = "queue_statistics_incomplete"
    except (httpx.HTTPError, ValueError, KeyError, TypeError) as error:
        statistics_error = (
            f"missing_{error.args[0]}" if isinstance(error, KeyError) else type(error).__name__
        )
    failed = database == "unavailable" or broker == "unavailable"
    degraded = queues is None or queues["q.jobs"]["consumers"] == 0
    return JSONResponse(
        {
            "status": "unavailable" if failed else "degraded" if degraded else "ok",
            "database": database,
            "broker": broker,
            "job_counts": counts if database == "connected" else None,
            "statistics": "available" if queues is not None else "unavailable",
            "statistics_error": statistics_error,
            "queues": queues,
        },
        status_code=503 if failed else 200,
    )
