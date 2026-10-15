import asyncio
import json
import logging
import os
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path

import asyncpg
from aio_pika.exceptions import AMQPException, ChannelInvalidStateError, ChannelPreconditionFailed

from bifrost.config import settings

DEPENDENCY_ERRORS = (
    OSError,
    TimeoutError,
    AMQPException,
    ChannelInvalidStateError,
    asyncpg.PostgresConnectionError,
    asyncpg.CannotConnectNowError,
    asyncpg.ConnectionDoesNotExistError,
    asyncpg.AdminShutdownError,
    asyncpg.CrashShutdownError,
)


class JsonFormatter(logging.Formatter):
    def format(self, record):
        data = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname.lower(),
            "component": record.name,
            "message": record.getMessage(),
        }
        data.update(getattr(record, "context", {}))
        if record.exc_info:
            data["exception"] = self.formatException(record.exc_info)
        return json.dumps(data, default=str)


def configure_logging():
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(handlers=[handler], level=logging.INFO, force=True)
    logging.getLogger("httpx").setLevel(logging.WARNING)


def dependency_failure(error: Exception) -> bool:
    if isinstance(error, ChannelPreconditionFailed):
        return False
    if isinstance(error, DEPENDENCY_ERRORS):
        return True
    if isinstance(error, asyncpg.InterfaceError):
        return "underlying connection is closed" in str(error)
    # Server termination can surface through asyncpg's protocol state machine.
    # Other InternalClientError variants, including pool misuse, remain fatal.
    return isinstance(error, asyncpg.InternalClientError) and (
        str(error).startswith("cannot switch to state ")
        and "; another operation (2) is in progress" in str(error)
    )


def hook_directive(gate: Path, marker: Path, job_id, automatic: bool):
    # Restart/redelivery can proceed after release without arming the same pause again.
    if automatic and not marker.exists():
        gate.write_text(str(job_id))
    return gate.read_text().strip() if gate.exists() else None


async def pause(point: str, job_id, payload: dict | None = None):
    if not settings().test_mode:
        return
    gate = Path(f"/tmp/hook-{point}")
    marker = Path(f"/tmp/reached-{point}-{job_id}")
    automatic = (payload or {}).get("_test", {}).get("pause_at") == point
    directive = await asyncio.to_thread(hook_directive, gate, marker, job_id, automatic)
    if directive not in ("*", str(job_id), "raise"):
        return
    await asyncio.to_thread(marker.touch)
    logging.getLogger("fault-hook").info(point, extra={"context": {"job_id": str(job_id)}})
    if directive == "raise":
        raise RuntimeError(f"Test-injected programming error at {point}")
    # The external test process removes the gate; no in-process event can signal it.
    while await asyncio.to_thread(gate.exists):  # noqa: ASYNC110
        await asyncio.sleep(0.05)


async def wait_stop(stop: asyncio.Event, delay: float):
    try:
        await asyncio.wait_for(stop.wait(), delay)
    except TimeoutError:
        pass


async def supervise(
    name: str, operation: Callable[[], Awaitable], stop: asyncio.Event, interval: float = 0
):
    log = logging.getLogger(name)
    backoff = 0.2
    while not stop.is_set():
        try:
            await operation()
            backoff = 0.2
            await wait_stop(stop, interval)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if dependency_failure(error):
                log.warning("dependency unavailable: %s", type(error).__name__)
                await wait_stop(stop, backoff)
                backoff = min(backoff * 2, 5)
            else:
                log.exception("unexpected background failure")
                # A dead background task must never leave a healthy-looking API process.
                os._exit(1)
