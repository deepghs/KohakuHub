"""Typed operation snapshots and state values."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal
from uuid import UUID


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
