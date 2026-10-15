import asyncio
import random
from collections.abc import Awaitable, Callable

from bifrost.config import Settings
from bifrost.models import Failure, Work

Progress = Callable[[int], Awaitable[None]]


class HandlerFailure(Exception):
    def __init__(self, failure: Failure):
        self.failure = failure
        super().__init__(failure.message)


async def email(work: Work, progress: Progress, scale: float):
    await asyncio.sleep(random.uniform(1, 3) * scale)
    return {"message_id": f"mock-{work.job_id}"}


async def webhook(work: Work, progress: Progress, scale: float):
    await asyncio.sleep(random.uniform(1, 2) * scale)
    if "_test" not in work.payload and random.random() < 0.2:
        raise HandlerFailure(
            Failure(code="mock_transient", message="Mock webhook unavailable", transient=True)
        )
    return {"status_code": 200}


async def report(work: Work, progress: Progress, scale: float):
    await asyncio.sleep(random.uniform(3, 5) * scale)
    return {"file_url": f"https://reports.invalid/{work.job_id}.pdf"}


async def batch(work: Work, progress: Progress, scale: float):
    items = work.payload["items"]
    delay = work.payload.get("_test", {}).get("delay")
    if delay is None:
        delay = 0.1 * scale
    reported = 0
    for index, _ in enumerate(items, 1):
        await asyncio.sleep(delay)
        percentage = int(index / len(items) * 100)
        if percentage >= reported + 10:
            await progress(percentage)
            reported = percentage
    return {"processed_count": len(items), "summary": items}


HANDLERS = {"email": email, "webhook": webhook, "report": report, "batch": batch}


async def execute(work: Work, progress: Progress, config: Settings):
    control = work.payload.get("_test", {})
    if config.test_mode:
        if work.type != "batch" and control.get("delay") is not None:
            await asyncio.sleep(control["delay"])
        if control.get("permanent"):
            raise HandlerFailure(
                Failure(code="mock_permanent", message="Mock permanent failure", transient=False)
            )
        if work.total_attempt <= control.get("fail_first", 0):
            raise HandlerFailure(
                Failure(code="mock_transient", message="Mock transient failure", transient=True)
            )
    return await HANDLERS[work.type](work, progress, config.time_scale)
