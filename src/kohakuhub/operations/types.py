"""Typed operation snapshots and state values."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import os
from typing import Any, Literal
from uuid import UUID


class RetryableOperationError(Exception):
    """A handler failure that is safe to retry from its durable checkpoint."""

    def __init__(
        self,
        message: str = "operation dependency is temporarily unavailable",
        *,
        error_code: str = "retryable_failure",
        error_summary: str | None = None,
    ) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.error_summary = error_summary or message


def operation_max_attempts() -> int:
    """Return the process-wide durable attempt budget for one step."""

    value = int(os.getenv("KOHAKU_HUB_OPERATION_MAX_RETRIES", "3"))
    if value < 1:
        raise ValueError("KOHAKU_HUB_OPERATION_MAX_RETRIES must be positive")
    return value


OperationState = Literal[
    "accepted",
    "running",
    "cancel_requested",
    "dispatch_started",
    "succeeded",
    "failed",
    "cancelled",
    "uncertain",
    "cleanup_pending",
]
StepState = Literal[
    "pending",
    "running",
    "dispatch_started",
    "observing",
    "succeeded",
    "failed",
    "cancelled",
    "uncertain",
]
CommitIntentState = Literal[
    "prepared",
    "dispatch_started",
    "committed",
    "finalized",
    "reconciliation_required",
    "uncertain",
    "abandoned",
]


@dataclass(frozen=True)
class OperationRecord:
    id: UUID
    kind: str
    handler_version: str
    repository_id: int | None
    resource_key: str
    requested_by_user_id: int | None
    trigger: str
    idempotency_key: str | None
    request_hash: str
    state: OperationState
    phase: str
    progress_current: int
    progress_total: int | None
    progress_message: str | None
    expected_head: str | None
    cancel_requested_at: datetime | None
    started_at: datetime | None
    heartbeat_at: datetime | None
    finished_at: datetime | None
    dispatch_started_at: datetime | None
    remote_deadline_at: datetime | None
    observe_not_before: datetime | None
    observation_cursor: str | None
    result_json: dict[str, Any] | None
    error_code: str | None
    error_summary: str | None
    created_at: datetime
    updated_at: datetime
    version: int

    def public_dict(self) -> dict[str, Any]:
        return {
            "id": str(self.id),
            "kind": self.kind,
            "state": self.state,
            "phase": self.phase,
            "progress": {
                "current": self.progress_current,
                "total": self.progress_total,
                "message": self.progress_message,
            },
            "result": self.result_json,
            "error": (
                {"code": self.error_code, "message": self.error_summary}
                if self.error_code or self.error_summary
                else None
            ),
            "cancel_requested": self.cancel_requested_at is not None,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
        }


@dataclass(frozen=True)
class StepRecord:
    id: int
    operation_id: UUID
    sequence: int
    step_name: str
    step_version: str
    state: StepState
    attempt: int
    delivery_key: str
    procrastinate_job_id: int | None
    input_json: dict[str, Any]
    checkpoint_json: dict[str, Any]
    external_marker: str | None
    expected_source: str | None
    expected_target: str | None
    artifact_key: str | None
    artifact_checksum: str | None
    artifact_length: int | None
    error_code: str | None
    error_summary: str | None
    started_at: datetime | None
    heartbeat_at: datetime | None
    finished_at: datetime | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class StepResult:
    """Result of one bounded handler quantum."""

    state: Literal["succeeded", "failed", "cancelled", "uncertain"]
    progress_current: int | None = None
    progress_total: int | None = None
    progress_message: str | None = None
    result_json: dict[str, Any] | None = None
    error_code: str | None = None
    error_summary: str | None = None
    retryable: bool = False
    # True only when the registered handler has completed every external
    # effect in this quantum and has an idempotent recovery contract.
    external_effect_confirmed: bool = False
    next_step_input: dict[str, Any] | None = None
    next_step_name: str | None = None
    next_step_version: str | None = None


    @classmethod
    def succeeded(
        cls,
        *,
        progress_current: int | None = None,
        progress_total: int | None = None,
        progress_message: str | None = None,
        result_json: dict[str, Any] | None = None,
    ) -> "StepResult":
        return cls(
            state="succeeded",
            progress_current=progress_current,
            progress_total=progress_total,
            progress_message=progress_message,
            result_json=result_json,
        )

    @classmethod
    def continue_with(
        cls,
        *,
        input_json: dict[str, Any],
        progress_current: int | None = None,
        progress_total: int | None = None,
        progress_message: str | None = None,
    ) -> "StepResult":
        return cls(
            state="succeeded",
            progress_current=progress_current,
            progress_total=progress_total,
            progress_message=progress_message,
            next_step_input=input_json,
        )

    @classmethod
    def retry_with(
        cls,
        *,
        input_json: dict[str, Any],
        progress_current: int | None = None,
        progress_total: int | None = None,
        progress_message: str | None = None,
        error_code: str = "retryable_failure",
        error_summary: str = "operation will retry from the last checkpoint",
    ) -> "StepResult":
        """Retry the same bounded quantum through a durable successor step."""

        return cls(
            state="succeeded",
            progress_current=progress_current,
            progress_total=progress_total,
            progress_message=progress_message,
            error_code=error_code,
            error_summary=error_summary,
            next_step_input=input_json,
            retryable=True,
        )


@dataclass(frozen=True)
class CommitIntentRecord:
    id: UUID
    operation_id: UUID | None
    observation_operation_id: UUID | None
    repository_id: int
    requested_by_user_id: int | None
    ref: str
    base_head: str
    marker: str
    idempotency_key: str | None
    request_hash: str | None
    prepared_deadline_at: datetime | None
    dispatch_started_at: datetime | None
    remote_deadline_at: datetime | None
    observe_not_before: datetime | None
    observation_head: str | None
    observation_cursor: str | None
    payload_hash: str
    payload_json: dict[str, Any]
    state: CommitIntentState
    lakefs_commit_id: str | None
    result_json: dict[str, Any] | None
    error_code: str | None
    error_summary: str | None
    created_at: datetime
    updated_at: datetime
    finalized_at: datetime | None
    version: int
