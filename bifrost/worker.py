import asyncio
import logging
import signal
import socket
from contextlib import aclosing

import aio_pika
from pydantic import ValidationError

from bifrost.broker import deliveries, publish, topology
from bifrost.config import Settings, settings
from bifrost.handlers import HandlerFailure, execute
from bifrost.models import TIMEOUTS, Event, Failure, Work
from bifrost.runtime import DEPENDENCY_ERRORS, configure_logging, pause, supervise

log = logging.getLogger("worker")


async def handle(message, channel, config: Settings):
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

    try:
        await pause("before_processing", work.job_id, work.payload)
        await event("processing")
        log.info("physical_execution_started", extra={"context": context})
        await pause("during_execution", work.job_id, work.payload)
        try:
            async with asyncio.timeout(TIMEOUTS[work.type] * config.time_scale) as deadline:
                result = await execute(work, progress, config)
        except TimeoutError:
            if not deadline.expired():
                raise
            await event(
                "failed", error=Failure(code="timeout", message="Handler timed out", transient=True)
            )
        except HandlerFailure as error:
            await event("failed", error=error.failure)
        else:
            await event("completed", result=result)
        log.info("final_event_confirmed", extra={"context": context})
        await pause("after_final", work.job_id, work.payload)
        await message.ack()
        log.info("work_acknowledged", extra={"context": context})
    except DEPENDENCY_ERRORS:
        if not channel.is_closed:
            await message.reject(requeue=False)
        raise


async def consume(config: Settings, stop):
    connection = await aio_pika.connect(config.broker_url, timeout=5)
    async with connection, connection.channel(publisher_confirms=True) as channel:
        await channel.set_qos(prefetch_count=1)
        work, _, _ = await topology(channel)
        async with aclosing(deliveries(work, stop)) as messages:
            async for message in messages:
                await handle(message, channel, config)


async def main():
    configure_logging()
    config = settings()
    stop = asyncio.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        asyncio.get_running_loop().add_signal_handler(signum, stop.set)
    await supervise("worker", lambda: consume(config, stop), stop)


if __name__ == "__main__":
    asyncio.run(main())
