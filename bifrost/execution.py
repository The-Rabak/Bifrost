"""Claim an attempt, execute it once while owned, and retain its outcome for replay."""

import asyncio
from collections.abc import Awaitable, Callable
from uuid import uuid4

from bifrost.config import Settings
from bifrost.execution_store import RedisExecutionStore
from bifrost.handlers import HandlerFailure, Progress, execute
from bifrost.models import TIMEOUTS, Event, Failure, Work
from bifrost.runtime import wait_stop


class ExecutionService:
    def __init__(self, store: RedisExecutionStore, config: Settings):
        self.store = store
        self.config = config

    async def run(
        self,
        work: Work,
        started: Callable[[], Awaitable[None]],
        progress: Progress,
        stop: asyncio.Event,
    ) -> Event | None:
        owner = f"claimed:{uuid4()}"
        timeout = TIMEOUTS[work.type] * self.config.time_scale
        # Covers processing publication, the bounded handler, and saving its outcome.
        lease_seconds = timeout + 15
        while True:
            if stop.is_set():
                return None
            if await self.store.claim(work, owner, lease_seconds):
                break
            outcome = await self.store.outcome(work)
            if outcome is not None:
                return outcome
            # Keep this delivery unacknowledged until an outcome exists or the
            # dead owner's lease expires. Stop waiting promptly on shutdown.
            await wait_stop(stop, 0.1)

        await started()
        try:
            async with asyncio.timeout(timeout) as deadline:
                result = await execute(work, progress, self.config)
        except TimeoutError:
            if not deadline.expired():
                raise
            outcome = Event(
                job_id=work.job_id,
                claim_token=work.claim_token,
                event="failed",
                error=Failure(code="timeout", message="Handler timed out", transient=True),
            )
        except HandlerFailure as error:
            outcome = Event(
                job_id=work.job_id,
                claim_token=work.claim_token,
                event="failed",
                error=error.failure,
            )
        else:
            outcome = Event(
                job_id=work.job_id, claim_token=work.claim_token, event="completed", result=result
            )
        if not await self.store.finish(work, owner, outcome):
            return None  # Ownership expired; never publish this execution's stale outcome.
        return outcome
