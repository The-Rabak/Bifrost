import asyncio
import logging
from contextlib import aclosing

from pydantic import ValidationError

from bifrost.broker import deliveries, publish, topology
from bifrost.config import Settings
from bifrost.lifecycle import begin, failure, matches_current_attempt
from bifrost.models import DispatchIdentity, Event, Failure, Work, utcnow
from bifrost.runtime import pause
from bifrost.store import change, recover_publication, reserve

log = logging.getLogger("status")


def work_message(row):
    return Work(
        job_id=row["id"],
        claim_token=row["claim_token"],
        type=row["type"],
        payload=row["payload"],
        priority=row["priority"],
        cycle=int(row["manual_retry_used"]),
        attempt=row["attempts"] + 1,
        total_attempt=row["total_attempts"] + 1,
    )


async def dispatch(pool, channel, config: Settings):
    for _ in range(50):
        row = await recover_publication(pool, config)
        recovering = row is not None
        if row is None:
            row = await reserve(pool, config)
        if row is None:
            break
        if not recovering:
            await pause("after_reserve", row["id"], row["payload"])
        await publish(channel, "q.jobs", work_message(row), row["priority"])
        await pause("after_publish", row["id"], row["payload"])
        await pool.execute(
            """
            UPDATE jobs SET publish_confirmed=true
            WHERE id=$1 AND claim_token=$2 AND status='dispatched'
        """,
            row["id"],
            row["claim_token"],
        )


async def apply_event(pool, event: Event, config: Settings):
    async with pool.acquire() as connection, connection.transaction():
        row = await connection.fetchrow("SELECT * FROM jobs WHERE id=$1 FOR UPDATE", event.job_id)
        if not matches_current_attempt(row, event.claim_token):
            return
        if event.event != "progress":
            row = await begin(connection, row, config)
        if event.event == "progress":
            if row["status"] == "processing" and event.progress is not None:
                await change(connection, row, {"progress": max(row["progress"], event.progress)})
        elif event.event == "completed":
            await change(
                connection,
                row,
                {
                    "status": "completed",
                    "result": event.result,
                    "error": None,
                    "progress": 100,
                    "completed_at": utcnow(),
                    "claim_token": None,
                    "attempt_deadline": None,
                    "recover_at": None,
                },
                "completed",
            )
        elif event.event == "failed":
            assert event.error is not None  # Event validation requires failure data.
            await failure(connection, row, event.error, config)
        await pause("before_status_commit", event.job_id)


async def statuses(pool, broker, config: Settings, stop):
    async with asyncio.timeout(5):
        await broker.ready()
    async with broker.channel(publisher_confirms=True) as channel:
        await channel.set_qos(prefetch_count=1)
        _, queue, _ = await topology(channel)
        async with aclosing(deliveries(queue, stop)) as messages:
            async for message in messages:
                try:
                    event = Event.model_validate_json(message.body)
                except ValidationError:
                    log.warning("discarding invalid status message")
                    await message.reject(requeue=False)
                    continue
                await apply_event(pool, event, config)
                await pause("after_status_commit", event.job_id)
                await message.ack()


async def reap(pool, config: Settings):
    await pause("reaper_tick", "reaper")
    for _ in range(50):
        async with pool.acquire() as connection, connection.transaction():
            row = await connection.fetchrow(
                """
                SELECT * FROM jobs WHERE status='processing' AND attempt_deadline <= $1
                ORDER BY attempt_deadline,id FOR UPDATE SKIP LOCKED LIMIT 1
            """,
                utcnow(),
            )
            if row is None:
                break
            await failure(
                connection,
                row,
                Failure(
                    code="abandoned",
                    message="Processing deadline expired; attempt abandoned",
                    transient=True,
                ),
                config,
                message="attempt_reaped",
            )


async def dead_letters(pool, broker, config: Settings, stop):
    async with asyncio.timeout(5):
        await broker.ready()
    async with broker.channel(publisher_confirms=True) as channel:
        await channel.set_qos(prefetch_count=1)
        _, _, queue = await topology(channel)
        async with aclosing(deliveries(queue, stop)) as messages:
            async for message in messages:
                try:
                    identity = DispatchIdentity.model_validate_json(message.body, extra="ignore")
                except ValidationError:
                    log.warning("discarding unidentifiable dead letter")
                    await message.ack()
                    continue
                async with pool.acquire() as connection, connection.transaction():
                    row = await connection.fetchrow(
                        "SELECT * FROM jobs WHERE id=$1 FOR UPDATE", identity.job_id
                    )
                    if matches_current_attempt(row, identity.claim_token):
                        await failure(
                            connection,
                            row,
                            Failure(
                                code="dead_letter",
                                message="Work dead-lettered after infrastructure failure",
                                transient=False,
                            ),
                            config,
                        )
                await message.ack()
