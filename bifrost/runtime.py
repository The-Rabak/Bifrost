import asyncio
import json
import logging
import os
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path

import asyncpg
from aio_pika.exceptions import AMQPException, ChannelInvalidStateError, ChannelPreconditionFailed
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

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
    RedisConnectionError,
    RedisTimeoutError,
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


def _read_pause_command(
    pause_file: Path, reached_file: Path, job_id: str, requested_by_job: bool
) -> str | None:
    """Create a job-requested pause once, then read the test's command."""
    # The marker survives container restart. Once released, the same job must not
    # recreate its pause when RabbitMQ redelivers it or the API recovers it.
    if requested_by_job and not reached_file.exists():
        pause_file.write_text(job_id)

    if not pause_file.exists():
        return None
    return pause_file.read_text().strip()


async def pause_for_test(point: str, job_id, payload: dict | None = None):
    """Wait at a test breakpoint until the external test deletes its pause file.

    The pause file contains a job ID, '*' for any job, or 'raise' to inject an
    error. A separate reached file tells the test execution got this far.
    Jobs can request their own breakpoint with payload._test.pause_at.
    """
    if not settings().test_mode:
        return

    job_id = str(job_id)
    pause_file = Path(f"/tmp/hook-{point}")
    reached_file = Path(f"/tmp/reached-{point}-{job_id}")
    test_controls = payload.get("_test", {}) if payload else {}
    requested_by_job = test_controls.get("pause_at") == point

    # File operations run in a thread so they do not block the asyncio event loop.
    command = await asyncio.to_thread(
        _read_pause_command, pause_file, reached_file, job_id, requested_by_job
    )
    if command is None:
        return
    if command != job_id and command not in ("*", "raise"):
        return

    await asyncio.to_thread(reached_file.touch)
    logging.getLogger("fault-hook").info(point, extra={"context": {"job_id": job_id}})
    if command == "raise":
        raise RuntimeError(f"Test-injected programming error at {point}")

    # Only this coroutine waits; HTTP requests and other background tasks keep
    # running. The host test releases it by deleting the file via docker exec.
    while await asyncio.to_thread(pause_file.exists):  # noqa: ASYNC110
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
