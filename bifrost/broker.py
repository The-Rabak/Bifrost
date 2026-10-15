import asyncio

import aio_pika
from aio_pika.abc import AbstractChannel
from aio_pika.exceptions import AMQPConnectionError
from pydantic import BaseModel


async def topology(channel: AbstractChannel):
    dlx = await channel.declare_exchange("dlx.jobs", aio_pika.ExchangeType.DIRECT, durable=True)
    dlq = await channel.declare_queue(
        "q.jobs.dlq", durable=True, arguments={"x-queue-type": "quorum"}
    )
    await dlq.bind(dlx, routing_key="dead")
    work = await channel.declare_queue(
        "q.jobs",
        durable=True,
        arguments={
            "x-queue-type": "quorum",
            "x-dead-letter-exchange": "dlx.jobs",
            "x-dead-letter-routing-key": "dead",
            "x-delivery-limit": 5,
        },
    )
    status = await channel.declare_queue(
        "q.job-status",
        durable=True,
        arguments={
            "x-queue-type": "classic",
            "x-single-active-consumer": True,
        },
    )
    return work, status, dlq


async def publish(channel: AbstractChannel, queue: str, body: BaseModel, priority=4):
    async with asyncio.timeout(5):
        await channel.default_exchange.publish(
            aio_pika.Message(
                body=body.model_dump_json().encode(),
                content_type="application/json",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                priority=priority,
            ),
            routing_key=queue,
            mandatory=True,
            timeout=5,
        )


async def deliveries(queue, stop: asyncio.Event):
    inbox = asyncio.Queue(maxsize=1)
    tag = await queue.consume(inbox.put)

    async def cancel_on_stop():
        await stop.wait()
        async with asyncio.timeout(5):
            await queue.cancel(tag)

    cancellation = asyncio.create_task(cancel_on_stop())
    stopped = asyncio.create_task(stop.wait())
    closed = asyncio.ensure_future(queue.channel.closed())
    try:
        while not stop.is_set():
            received = asyncio.create_task(inbox.get())
            done, _ = await asyncio.wait(
                {received, stopped, closed}, return_when=asyncio.FIRST_COMPLETED
            )
            if stopped in done:
                if received.done():
                    if not queue.channel.is_closed:
                        await received.result().reject(requeue=True)
                else:
                    received.cancel()
                    await asyncio.gather(received, return_exceptions=True)
                break
            if closed in done:
                received.cancel()
                await asyncio.gather(received, return_exceptions=True)
                raise AMQPConnectionError("Consumer channel closed")
            yield received.result()
    finally:
        if stop.is_set():
            await cancellation
        else:
            cancellation.cancel()
            await asyncio.gather(cancellation, return_exceptions=True)
            if not queue.channel.is_closed:
                async with asyncio.timeout(5):
                    await queue.cancel(tag)
        stopped.cancel()
        await asyncio.gather(stopped, return_exceptions=True)
        # Prefetched deliveries not handled before shutdown must return to the queue.
        while not inbox.empty():
            message = inbox.get_nowait()
            if not queue.channel.is_closed:
                await message.reject(requeue=True)
