from datetime import timedelta

from bifrost.config import Settings
from bifrost.models import TIMEOUTS, Failure, utcnow
from bifrost.store import change


def matches_current_attempt(row, claim_token) -> bool:
    return (
        row is not None
        and row["claim_token"] == claim_token
        and row["status"] in ("dispatched", "processing")
    )


async def begin(connection, row, config: Settings):
    if row["status"] != "dispatched":
        return row
    now = utcnow()
    return await change(
        connection,
        row,
        {
            "status": "processing",
            "attempts": row["attempts"] + 1,
            "total_attempts": row["total_attempts"] + 1,
            "started_at": row["started_at"] or now,
            "attempt_started_at": now,
            "attempt_deadline": now
            + timedelta(
                seconds=TIMEOUTS[row["type"]] * config.time_scale + config.recovery_allowance
            ),
            "progress": 0,
            "result": None,
            "error": None,
        },
        "processing",
    )


async def failure(connection, row, error: Failure, config: Settings, message=None):
    retry = error.transient and row["attempts"] < row["max_attempts"]
    now = utcnow()
    updates = {
        "status": "pending" if retry else "failed",
        "error": error.model_dump(),
        "result": None,
        "claim_token": None,
        "recover_at": None,
        "publish_confirmed": False,
        "attempt_deadline": None,
        "completed_at": None if retry else now,
    }
    if retry:
        updates["next_attempt_at"] = now + timedelta(
            seconds=config.retry_base * 4 ** max(row["attempts"] - 1, 0)
        )
    return await change(
        connection,
        row,
        updates,
        message or ("retry_scheduled" if retry else "failed"),
        {"error": error.model_dump()},
        "warning" if retry else "error",
    )
