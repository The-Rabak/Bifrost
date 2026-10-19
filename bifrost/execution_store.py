"""Redis storage for worker-owned attempt claims and immutable outcomes."""

from math import ceil

from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff

from bifrost.models import Event, Work

# Compare and replace together: an expired owner cannot overwrite its successor.
SAVE_OUTCOME = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('SET', KEYS[1], ARGV[2], 'EX', 86400)
return 1
"""


class RedisExecutionStore:
    def __init__(self, url: str, namespace: str):
        self.namespace = namespace
        self.redis = Redis.from_url(
            url,
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
            retry=Retry(NoBackoff(), 0),
        )

    def key(self, work: Work) -> str:
        # The namespace belongs to a service, never an individual container.
        # Different logical attempts must not reuse a cached failure.
        return f"{self.namespace}:{work.type}:{work.claim_token}:{work.job_id}"

    async def ping(self):
        await self.redis.ping()

    async def claim(self, work: Work, owner: str, lease_seconds: float) -> bool:
        return bool(
            await self.redis.set(self.key(work), owner, nx=True, px=ceil(lease_seconds * 1000))
        )

    async def outcome(self, work: Work) -> Event | None:
        value = await self.redis.get(self.key(work))
        if value is None or value.startswith("claimed:"):
            return None
        return Event.model_validate_json(value)

    async def finish(self, work: Work, owner: str, outcome: Event) -> bool:
        return bool(
            await self.redis.eval(SAVE_OUTCOME, 1, self.key(work), owner, outcome.model_dump_json())
        )

    async def close(self):
        await self.redis.aclose()
