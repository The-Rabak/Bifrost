import json
from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    EmailStr,
    Field,
    HttpUrl,
    ValidationInfo,
    field_validator,
    model_validator,
)

from bifrost.config import settings


class JobType(StrEnum):
    email = "email"
    webhook = "webhook"
    report = "report"
    batch = "batch"


class Status(StrEnum):
    scheduled = "scheduled"
    pending = "pending"
    dispatched = "dispatched"
    processing = "processing"
    completed = "completed"
    failed = "failed"
    cancelled = "cancelled"


TERMINAL = {Status.completed, Status.failed, Status.cancelled}
TIMEOUTS = {JobType.email: 15, JobType.webhook: 15, JobType.report: 60, JobType.batch: 180}


def encode_json(value) -> str:
    return json.dumps(value, allow_nan=False)


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class MockControls(StrictModel):
    fail_first: Annotated[int, Field(strict=True, ge=0, le=10)] = 0
    permanent: Annotated[bool, Field(strict=True)] = False
    delay: Annotated[float, Field(ge=0, le=200)] | None = None
    pause_at: (
        Literal[
            "after_reserve", "after_publish", "before_processing", "during_execution", "after_final"
        ]
        | None
    ) = None


class Payload(StrictModel):
    controls: MockControls | None = Field(default=None, alias="_test")

    @field_validator("controls")
    @classmethod
    def test_only(cls, value):
        if not settings().test_mode:
            raise ValueError("_test requires TEST_MODE=true")
        if value is None:
            raise ValueError("_test must be an object")
        return value


class EmailPayload(Payload):
    to: EmailStr
    subject: str = Field(default="", max_length=200)
    body: str = Field(default="", max_length=32000)


class WebhookPayload(Payload):
    url: HttpUrl
    data: dict[str, Any] = Field(default_factory=dict)


class ReportPayload(Payload):
    parameters: dict[str, Any]


class BatchPayload(Payload):
    items: list[Any] = Field(max_length=1000)


PAYLOADS = {
    JobType.email: EmailPayload,
    JobType.webhook: WebhookPayload,
    JobType.report: ReportPayload,
    JobType.batch: BatchPayload,
}


class Submission(StrictModel):
    type: JobType
    payload: dict[str, Any]
    priority: Annotated[int, Field(strict=True, ge=0, le=31)] = 4
    max_attempts: Annotated[int, Field(strict=True, ge=1, le=5)] = 3
    scheduled_for: AwareDatetime | None = None
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=200)

    @field_validator("payload")
    @classmethod
    def validate_payload(cls, value, info: ValidationInfo):
        kind = info.data.get("type")
        if kind is None:
            return value
        encode_json(value)
        # The same contract validates HTTP submissions and broker work.
        validated = PAYLOADS[kind].model_validate(value)
        payload = validated.model_dump(mode="json", by_alias=True, exclude_unset=True)
        if len(encode_json(payload).encode()) > 65536:
            raise ValueError("payload exceeds 64 KiB")
        return payload


class DispatchIdentity(StrictModel):
    job_id: UUID
    claim_token: UUID


class Work(DispatchIdentity):
    type: JobType
    payload: dict[str, Any]
    priority: Annotated[int, Field(strict=True, ge=0, le=31)]
    cycle: Annotated[int, Field(strict=True, ge=0, le=1)]
    attempt: Annotated[int, Field(strict=True, ge=1, le=5)]
    total_attempt: Annotated[int, Field(strict=True, ge=1, le=10)]

    @field_validator("payload")
    @classmethod
    def validate_payload(cls, value, info: ValidationInfo):
        return Submission.validate_payload(value, info)


class Failure(StrictModel):
    code: str = Field(min_length=1, max_length=100)
    message: str = Field(min_length=1, max_length=2000)
    transient: Annotated[bool, Field(strict=True)]


class Event(DispatchIdentity):
    event: Literal["processing", "progress", "completed", "failed"]
    progress: Annotated[int, Field(strict=True, ge=0, le=100)] | None = None
    result: dict[str, Any] | None = None
    error: Failure | None = None

    @field_validator("result")
    @classmethod
    def valid_json_result(cls, value):
        if value is not None:
            encode_json(value)
        return value

    @model_validator(mode="after")
    def required_event_data(self):
        required = {"progress": self.progress, "completed": self.result, "failed": self.error}
        if self.event in required and required[self.event] is None:
            raise ValueError(f"{self.event} requires its outcome data")
        return self


def public_job(row) -> dict:
    internal = {"claim_token", "publish_confirmed", "recover_at", "attempt_deadline"}
    return {key: value for key, value in dict(row).items() if key not in internal}


def utcnow() -> datetime:
    from datetime import UTC

    return datetime.now(UTC)
