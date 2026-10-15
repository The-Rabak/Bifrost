import json
import logging
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import asyncpg

from bifrost.config import Settings
from bifrost.models import Submission, encode_json, utcnow


async def codec(connection):
    await connection.set_type_codec(
        "jsonb", schema="pg_catalog", encoder=encode_json, decoder=json.loads, format="text"
    )


async def open_pool(url: str):
    return await asyncpg.create_pool(
        url, min_size=0, max_size=10, init=codec, command_timeout=10, timeout=5
    )


async def initialize(pool):
    sql = Path(__file__).with_name("schema.sql").read_text()
    async with pool.acquire() as connection, connection.transaction():
        await connection.execute("SELECT pg_advisory_xact_lock(8162734)")
        # The initial DDL commits atomically. Avoid DDL table locks on ordinary replica startup.
        if await connection.fetchval("SELECT to_regclass('public.jobs') IS NOT NULL"):
            return
        await connection.execute(sql)


async def audit(connection, row, message, metadata=None, level="info"):
    logging.getLogger("lifecycle").log(
        {"info": logging.INFO, "warning": logging.WARNING, "error": logging.ERROR}[level],
        message,
        extra={
            "context": {
                "job_id": row["id"],
                "type": row["type"],
                "cycle": int(row["manual_retry_used"]),
                "attempt": row["attempts"],
                "claim_token": row["claim_token"],
            }
        },
    )
    await connection.execute(
        """
        INSERT INTO job_logs (job_id, level, message, metadata, cycle, attempt, claim_token)
        VALUES ($1,$2,$3,$4,$5,$6,$7)
    """,
        row["id"],
        level,
        message,
        metadata or {},
        int(row["manual_retry_used"]),
        row["attempts"],
        row["claim_token"],
    )


async def change(connection, row, updates: dict, message=None, metadata=None, level="info"):
    # Column names are exclusively supplied by lifecycle code; values stay parameterized.
    updates["updated_at"] = utcnow()
    columns = ", ".join(f"{name} = ${index}" for index, name in enumerate(updates, 2))
    changed = await connection.fetchrow(
        f"UPDATE jobs SET {columns} WHERE id=$1 RETURNING *", row["id"], *updates.values()
    )
    if message:
        # End-of-attempt audit keeps the invalidated token as historical context.
        context = dict(changed)
        context["claim_token"] = changed["claim_token"] or row["claim_token"]
        await audit(connection, context, message, metadata, level)
    return changed


async def submit(pool, request: Submission):
    now = utcnow()
    due = max(request.scheduled_for or now, now)
    async with pool.acquire() as connection, connection.transaction():
        while True:
            row = await connection.fetchrow(
                """
                INSERT INTO jobs (id,type,payload,status,priority,max_attempts,idempotency_key,
                                  scheduled_for,next_attempt_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                ON CONFLICT (idempotency_key) DO NOTHING RETURNING *
            """,
                uuid4(),
                request.type,
                request.payload,
                "scheduled" if due > now else "pending",
                request.priority,
                request.max_attempts,
                request.idempotency_key,
                request.scheduled_for,
                due,
            )
            if row:
                await audit(connection, row, "submitted")
                return row, False
            row = await connection.fetchrow(
                "SELECT * FROM jobs WHERE idempotency_key=$1 FOR KEY SHARE", request.idempotency_key
            )
            if row:
                return row, True
            # Cleanup released the old key between statements; new jobs cannot expire yet.


async def reserve(pool, config: Settings):
    now = utcnow()
    async with pool.acquire() as connection, connection.transaction():
        row = await connection.fetchrow(
            """
            SELECT * FROM jobs WHERE status IN ('scheduled','pending') AND next_attempt_at <= $1
            ORDER BY priority DESC,next_attempt_at,id FOR UPDATE SKIP LOCKED LIMIT 1
        """,
            now,
        )
        if not row:
            return None
        return await change(
            connection,
            row,
            {
                "status": "dispatched",
                "claim_token": uuid4(),
                "publish_confirmed": False,
                "recover_at": now + timedelta(seconds=config.recovery_seconds),
                "attempt_started_at": None,
                "attempt_deadline": None,
                "progress": 0,
            },
            "dispatched",
        )


async def recover_publication(pool, config: Settings):
    now = utcnow()
    async with pool.acquire() as connection, connection.transaction():
        row = await connection.fetchrow(
            """
            SELECT * FROM jobs WHERE status='dispatched' AND NOT publish_confirmed
            AND recover_at <= $1 ORDER BY recover_at,id FOR UPDATE SKIP LOCKED LIMIT 1
        """,
            now,
        )
        if row is None:
            return None
        return await change(
            connection,
            row,
            {
                "recover_at": now + timedelta(seconds=config.recovery_seconds),
            },
        )


async def cleanup(pool):
    async with pool.acquire() as connection, connection.transaction():
        await connection.execute("""
            WITH old AS (
                SELECT id FROM job_logs WHERE created_at < now()-interval '30 days'
                ORDER BY created_at,id FOR UPDATE SKIP LOCKED LIMIT 500
            ) DELETE FROM job_logs USING old WHERE job_logs.id=old.id
        """)
        await connection.execute("""
            WITH old AS (
                SELECT id FROM jobs WHERE status IN ('completed','failed','cancelled')
                AND idempotency_key IS NOT NULL
                AND created_at < now()-interval '30 days'
                AND completed_at < now()-interval '30 days'
                ORDER BY completed_at,id FOR UPDATE SKIP LOCKED LIMIT 500
            ) UPDATE jobs SET idempotency_key=NULL FROM old WHERE jobs.id=old.id
        """)
