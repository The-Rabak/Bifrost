from functools import lru_cache

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")
    broker_url: str
    database_url: str = ""
    api_token: str = ""
    management_url: str = "http://broker:15672"
    broker_user: str = ""
    broker_password: str = ""
    redis_url: str = "redis://redis:6379/0"
    execution_namespace: str = Field(default="bifrost:worker", min_length=1)
    test_mode: bool = False
    time_scale: float = Field(default=1, gt=0, le=1)
    retry_base: float = Field(default=30, gt=0)
    tick_seconds: float = Field(default=2, gt=0)
    recovery_seconds: float = Field(default=10, gt=0)
    reaper_seconds: float = Field(default=10, gt=0)
    recovery_allowance: float = Field(default=5, gt=0)
    cleanup_seconds: float = Field(default=3600, gt=0)
    submit_limit: int = Field(default=120, gt=0)
    retry_limit: int = Field(default=30, gt=0)

    @model_validator(mode="after")
    def test_timings(self):
        if not self.test_mode and (self.time_scale != 1 or self.retry_base != 30):
            raise ValueError("Shortened handler/retry timing requires TEST_MODE=true")
        return self


@lru_cache
def settings() -> Settings:
    return Settings()
