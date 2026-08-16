"""DDL owned by the durable operation kernel."""

OPERATION_SCHEMA_VERSION = 2

OPERATION_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS khub_repository_operations (
    id UUID PRIMARY KEY,
    kind TEXT NOT NULL,
    handler_version TEXT NOT NULL,
    repository_id BIGINT,
    resource_key TEXT NOT NULL,
    requested_by_user_id BIGINT,
    trigger TEXT NOT NULL,
    idempotency_key TEXT,
    request_hash TEXT NOT NULL,
    state TEXT NOT NULL,
    phase TEXT NOT NULL,
    progress_current BIGINT NOT NULL DEFAULT 0,
    progress_total BIGINT,
    progress_message TEXT,
    expected_head TEXT,
    cancel_requested_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    heartbeat_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    dispatch_started_at TIMESTAMPTZ,
    remote_deadline_at TIMESTAMPTZ,
    observe_not_before TIMESTAMPTZ,
    result_json JSONB,
    error_code TEXT,
    error_summary TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    version BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT khub_operation_state_ck CHECK (
        state IN (
            'accepted', 'running', 'cancel_requested', 'succeeded',
            'failed', 'cancelled', 'dispatch_started', 'uncertain', 'cleanup_pending'
        )
    ),
    CONSTRAINT khub_operation_trigger_ck CHECK (
        trigger IN ('api', 'schedule', 'reconcile', 'system')
    ),
    CONSTRAINT khub_operation_progress_ck CHECK (
        progress_current >= 0
        AND (progress_total IS NULL OR progress_total >= progress_current)
    )
);

ALTER TABLE khub_repository_operations
    DROP CONSTRAINT IF EXISTS khub_operation_state_ck;

ALTER TABLE khub_repository_operations
    ADD CONSTRAINT khub_operation_state_ck CHECK (
        state IN (
            'accepted', 'running', 'cancel_requested', 'succeeded',
            'failed', 'cancelled', 'dispatch_started', 'uncertain', 'cleanup_pending'
        )
    );

CREATE UNIQUE INDEX IF NOT EXISTS khub_operation_idempotency_uidx
    ON khub_repository_operations (requested_by_user_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS khub_operation_active_resource_uidx
    ON khub_repository_operations (resource_key)
    WHERE state IN ('accepted', 'running', 'cancel_requested', 'dispatch_started', 'uncertain');

CREATE INDEX IF NOT EXISTS khub_operation_owner_idx
    ON khub_repository_operations (requested_by_user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS khub_operation_repository_idx
    ON khub_repository_operations (repository_id, created_at DESC);

CREATE TABLE IF NOT EXISTS khub_operation_steps (
    id BIGSERIAL PRIMARY KEY,
    operation_id UUID NOT NULL REFERENCES khub_repository_operations(id) ON DELETE CASCADE,
    sequence INTEGER NOT NULL,
    step_name TEXT NOT NULL,
    step_version TEXT NOT NULL,
    state TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 0,
    delivery_key TEXT NOT NULL,
    procrastinate_job_id BIGINT,
    input_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    checkpoint_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    external_marker TEXT,
    expected_source TEXT,
    expected_target TEXT,
    artifact_key TEXT,
    artifact_checksum TEXT,
    artifact_length BIGINT,
    error_code TEXT,
    error_summary TEXT,
    started_at TIMESTAMPTZ,
    heartbeat_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT khub_step_state_ck CHECK (
        state IN (
            'pending', 'running', 'dispatch_started', 'observing',
            'succeeded', 'failed', 'cancelled', 'uncertain'
        )
    ),
    CONSTRAINT khub_step_attempt_ck CHECK (attempt >= 0),
    CONSTRAINT khub_step_sequence_uidx UNIQUE (operation_id, sequence),
    CONSTRAINT khub_step_delivery_uidx UNIQUE (delivery_key)
);

CREATE INDEX IF NOT EXISTS khub_operation_steps_operation_idx
    ON khub_operation_steps (operation_id, sequence);

CREATE INDEX IF NOT EXISTS khub_operation_steps_state_idx
    ON khub_operation_steps (state, updated_at);
"""


def operation_table_columns() -> dict[str, tuple[str, ...]]:
    """Return the columns created by this schema version."""

    return {
        "khub_repository_operations": (
            "cancel_requested_at",
            "created_at",
            "dispatch_started_at",
            "error_code",
            "error_summary",
            "expected_head",
            "finished_at",
            "handler_version",
            "heartbeat_at",
            "id",
            "idempotency_key",
            "kind",
            "observe_not_before",
            "phase",
            "progress_current",
            "progress_message",
            "progress_total",
            "remote_deadline_at",
            "repository_id",
            "request_hash",
            "requested_by_user_id",
            "resource_key",
            "result_json",
            "started_at",
            "state",
            "trigger",
            "updated_at",
            "version",
        ),
        "khub_operation_steps": (
            "artifact_checksum",
            "artifact_key",
            "artifact_length",
            "attempt",
            "checkpoint_json",
            "created_at",
            "delivery_key",
            "error_code",
            "error_summary",
            "expected_source",
            "expected_target",
            "external_marker",
            "finished_at",
            "heartbeat_at",
            "id",
            "input_json",
            "operation_id",
            "procrastinate_job_id",
            "sequence",
            "started_at",
            "state",
            "step_name",
            "step_version",
            "updated_at",
        ),
    }
