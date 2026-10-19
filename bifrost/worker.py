import asyncio
import logging
import signal
import socket
from contextlib import aclosing

import aio_pika
from pydantic import ValidationError
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from bifrost.broker import deliveries, publish, topology
from bifrost.config import Settings, settings
from bifrost.execution import ExecutionService
from bifrost.execution_store import RedisExecutionStore
from bifrost.models import Event, Work
from bifrost.runtime import DEPENDENCY_ERRORS, configure_logging, pause_for_test, supervise

log = logging.getLogger("worker")


async def handle(message, channel, execution: ExecutionService, stop: asyncio.Event):
    try:
        work = Work.model_validate_json(message.body)
    except ValidationError:
        log.warning("dead-lettering malformed work")
        await message.reject(requeue=False)
        return
    context = {
        "worker": socket.gethostname(),
        "job_id": str(work.job_id),
        "type": work.type,
        "cycle": work.cycle,
        "attempt": work.attempt,
        "claim_token": str(work.claim_token),
    }

    async def event(kind, **values):
        await publish(
            channel,
            "q.job-status",
            Event(job_id=work.job_id, claim_token=work.claim_token, event=kind, **values),
        )

    async def progress(value):
        await event("progress", progress=value)

    async def started():
        await event("processing")
        log.info("physical_execution_started", extra={"context": context})
        await pause_for_test("during_execution", work.job_id, work.payload)

    try:
        await pause_for_test("before_processing", work.job_id, work.payload)
        outcome = await execution.run(work, started, progress, stop)
        if outcome is None:
            await message.reject(requeue=True)
            return
        await pause_for_test("after_outcome_saved", work.job_id, work.payload)
        await publish(channel, "q.job-status", outcome)
        log.info("final_event_confirmed", extra={"context": context})
        await pause_for_test("after_final", work.job_id, work.payload)
        await message.ack()
        log.info("work_acknowledged", extra={"context": context})
    except (RedisConnectionError, RedisTimeoutError):
        # Closing the channel requeues unacknowledged work. Redis outages must
        # never run an unclaimed handler or turn valid work into a dead letter.
        raise
    except DEPENDENCY_ERRORS:
        if not channel.is_closed:
            await message.reject(requeue=False)
        raise


async def consume(config: Settings, store: RedisExecutionStore, stop):
    # Do not repeatedly take/requeue work during a Redis outage: quorum queues
    # have a delivery limit. Reconnect to Redis before reopening consumption.
    await store.ping()
    execution = ExecutionService(store, config)
    connection = await aio_pika.connect(config.broker_url, timeout=5)
    async with connection, connection.channel(publisher_confirms=True) as channel:
        await channel.set_qos(prefetch_count=1)
        work, _, _ = await topology(channel)
        async with aclosing(deliveries(work, stop)) as messages:
            async for message in messages:
                await handle(message, channel, execution, stop)


async def main():
    configure_logging()
    config = settings()
    stop = asyncio.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        asyncio.get_running_loop().add_signal_handler(signum, stop.set)
    store = RedisExecutionStore(config.redis_url, config.execution_namespace)
    try:
        await supervise("worker", lambda: consume(config, store, stop), stop)
    finally:
        await store.close()


if __name__ == "__main__":
    asyncio.run(main())
