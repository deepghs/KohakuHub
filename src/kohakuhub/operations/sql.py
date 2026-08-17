"""DDL owned by the durable operation kernel."""

OPERATION_SCHEMA_VERSION = 8

# This is the schema shipped by the first durable-worker commit.  It is kept
# as an immutable migration input so a database created by that release can
# be upgraded without guessing from the current table shape.
HISTORICAL_OPERATION_TABLE_COLUMNS_V2: dict[str, tuple[str, ...]] = {
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

# Version 3 added commit intents.  Its table contract is kept separately from
# the current schema because the migration ledger may contain this released
# shape even after the running code has moved on to a later kernel version.
HISTORICAL_OPERATION_TABLE_COLUMNS_V3: dict[str, tuple[str, ...]] = {
    **HISTORICAL_OPERATION_TABLE_COLUMNS_V2,
    "khub_commit_intents": (
        "base_head",
        "created_at",
        "error_code",
        "error_summary",
        "finalized_at",
        "id",
        "lakefs_commit_id",
        "marker",
        "operation_id",
        "payload_hash",
        "payload_json",
        "ref",
        "repository_id",
        "result_json",
        "state",
        "updated_at",
        "version",
    ),
}

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
    observation_cursor TEXT,
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
    ADD COLUMN IF NOT EXISTS observation_cursor TEXT;

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

CREATE TABLE IF NOT EXISTS khub_commit_intents (
    id UUID PRIMARY KEY,
    operation_id UUID,
    observation_operation_id UUID,
    repository_id BIGINT NOT NULL,
    requested_by_user_id BIGINT,
    ref TEXT NOT NULL,
    base_head TEXT NOT NULL,
    marker TEXT NOT NULL,
    idempotency_key TEXT,
    request_hash TEXT,
    prepared_deadline_at TIMESTAMPTZ,
    dispatch_started_at TIMESTAMPTZ,
    remote_deadline_at TIMESTAMPTZ,
    observe_not_before TIMESTAMPTZ,
    observation_head TEXT,
    payload_hash TEXT NOT NULL,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    state TEXT NOT NULL,
    lakefs_commit_id TEXT,
    result_json JSONB,
    error_code TEXT,
    error_summary TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finalized_at TIMESTAMPTZ,
    version BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT khub_commit_intent_state_ck CHECK (
        state IN ('prepared', 'dispatch_started', 'committed', 'finalized', 'reconciliation_required', 'uncertain', 'abandoned')
    ),
    CONSTRAINT khub_commit_intent_marker_ck CHECK (
        marker LIKE 'khub:v1:%'
    ),
    CONSTRAINT khub_commit_intent_payload_hash_ck CHECK (
        payload_hash ~ '^[0-9a-f]{64}$'
    )
);

ALTER TABLE khub_commit_intents
    DROP CONSTRAINT IF EXISTS khub_commit_intent_state_ck;
ALTER TABLE khub_commit_intents
    ADD CONSTRAINT khub_commit_intent_state_ck CHECK (
        state IN ('prepared', 'dispatch_started', 'committed', 'finalized', 'reconciliation_required', 'uncertain', 'abandoned')
    );

CREATE UNIQUE INDEX IF NOT EXISTS khub_commit_intent_marker_uidx
    ON khub_commit_intents (marker);

CREATE UNIQUE INDEX IF NOT EXISTS khub_commit_intent_operation_uidx
    ON khub_commit_intents (operation_id)
    WHERE operation_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS khub_commit_intent_recovery_idx
    ON khub_commit_intents (state, updated_at);

CREATE INDEX IF NOT EXISTS khub_commit_intent_repo_ref_idx
    ON khub_commit_intents (repository_id, ref, created_at DESC);

ALTER TABLE khub_commit_intents
    ADD COLUMN IF NOT EXISTS requested_by_user_id BIGINT;
ALTER TABLE khub_commit_intents
    ADD COLUMN IF NOT EXISTS idempotency_key TEXT;
ALTER TABLE khub_commit_intents
    ADD COLUMN IF NOT EXISTS request_hash TEXT;
ALTER TABLE khub_commit_intents
    ADD COLUMN IF NOT EXISTS dispatch_started_at TIMESTAMPTZ;
ALTER TABLE khub_commit_intents
    ADD COLUMN IF NOT EXISTS remote_deadline_at TIMESTAMPTZ;
ALTER TABLE khub_commit_intents
    ADD COLUMN IF NOT EXISTS observe_not_before TIMESTAMPTZ;
ALTER TABLE khub_commit_intents
    ADD COLUMN IF NOT EXISTS observation_operation_id UUID;
ALTER TABLE khub_commit_intents
    ADD COLUMN IF NOT EXISTS prepared_deadline_at TIMESTAMPTZ;
ALTER TABLE khub_commit_intents
    ADD COLUMN IF NOT EXISTS observation_cursor TEXT;
ALTER TABLE khub_commit_intents
    ADD COLUMN IF NOT EXISTS observation_head TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS khub_commit_intent_request_uidx
    ON khub_commit_intents (requested_by_user_id, repository_id, ref, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS khub_commit_intent_observation_operation_uidx
    ON khub_commit_intents (observation_operation_id)
    WHERE observation_operation_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS khub_quota_reservations (
    id UUID PRIMARY KEY,
    intent_id UUID NOT NULL REFERENCES khub_commit_intents(id) ON DELETE CASCADE,
    scope_type TEXT NOT NULL,
    scope_id BIGINT NOT NULL,
    is_private BOOLEAN NOT NULL,
    reserved_bytes BIGINT NOT NULL,
    state TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMPTZ,
    CONSTRAINT khub_quota_reservation_scope_ck CHECK (
        scope_type IN ('repository', 'namespace')
    ),
    CONSTRAINT khub_quota_reservation_bytes_ck CHECK (reserved_bytes > 0),
    CONSTRAINT khub_quota_reservation_state_ck CHECK (
        state IN ('reserved', 'consumed', 'released')
    ),
    CONSTRAINT khub_quota_reservation_unique_scope_uidx
        UNIQUE (intent_id, scope_type)
);

CREATE INDEX IF NOT EXISTS khub_quota_reservation_active_idx
    ON khub_quota_reservations (scope_type, scope_id, is_private, state);
CREATE INDEX IF NOT EXISTS khub_quota_reservation_intent_idx
    ON khub_quota_reservations (intent_id, state);
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
            "observation_cursor",
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
        "khub_commit_intents": (
            "base_head",
            "created_at",
            "dispatch_started_at",
            "error_code",
            "error_summary",
            "finalized_at",
            "id",
            "lakefs_commit_id",
            "marker",
            "idempotency_key",
            "operation_id",
            "observation_operation_id",
            "observe_not_before",
            "observation_head",
            "observation_cursor",
            "payload_hash",
            "payload_json",
            "ref",
            "repository_id",
            "result_json",
            "request_hash",
            "requested_by_user_id",
            "prepared_deadline_at",
            "remote_deadline_at",
            "state",
            "updated_at",
            "version",
        ),
        "khub_quota_reservations": (
            "created_at",
            "finished_at",
            "id",
            "intent_id",
            "is_private",
            "reserved_bytes",
            "scope_id",
            "scope_type",
            "state",
            "updated_at",
        ),
    }


def operation_table_columns_for_version(version: int) -> dict[str, tuple[str, ...]]:
    """Return an immutable column signature for a supported kernel version."""

    if version == 2:
        return HISTORICAL_OPERATION_TABLE_COLUMNS_V2
    if version == 3:
        return HISTORICAL_OPERATION_TABLE_COLUMNS_V3
    if version == 6:
        # v6 was used by the unreleased predecessor of this migration.  It had
        # the current commit-intent/quota tables but not the observation cursor.
        current = operation_table_columns()
        return {
            **current,
            "khub_commit_intents": tuple(
                column
                for column in current["khub_commit_intents"]
                if column != "observation_cursor"
            ),
        }
    if version == 7:
        current = operation_table_columns()
        return {
            **current,
            "khub_commit_intents": tuple(
                column
                for column in current["khub_commit_intents"]
                if column != "observation_head"
            ),
        }
    if version == OPERATION_SCHEMA_VERSION:
        return operation_table_columns()
    raise ValueError(f"unsupported historical operation schema version {version}")


def operation_column_contract() -> dict[str, dict[str, tuple[str, bool, str | None]]]:
    """Return the type/nullability/default contract for every kernel column."""

    timestamp = ("timestamp with time zone", True, None)
    return {
        "khub_repository_operations": {
            "id": ("uuid", False, None),
            "kind": ("text", False, None),
            "handler_version": ("text", False, None),
            "repository_id": ("bigint", True, None),
            "resource_key": ("text", False, None),
            "requested_by_user_id": ("bigint", True, None),
            "trigger": ("text", False, None),
            "idempotency_key": ("text", True, None),
            "request_hash": ("text", False, None),
            "state": ("text", False, None),
            "phase": ("text", False, None),
            "progress_current": ("bigint", False, "0"),
            "progress_total": ("bigint", True, None),
            "progress_message": ("text", True, None),
            "expected_head": ("text", True, None),
            "cancel_requested_at": timestamp,
            "started_at": timestamp,
            "heartbeat_at": timestamp,
            "finished_at": timestamp,
            "dispatch_started_at": timestamp,
            "remote_deadline_at": timestamp,
            "observe_not_before": timestamp,
            "observation_cursor": ("text", True, None),
            "result_json": ("jsonb", True, None),
            "error_code": ("text", True, None),
            "error_summary": ("text", True, None),
            "created_at": ("timestamp with time zone", False, "CURRENT_TIMESTAMP"),
            "updated_at": ("timestamp with time zone", False, "CURRENT_TIMESTAMP"),
            "version": ("bigint", False, "0"),
        },
        "khub_operation_steps": {
            "id": ("bigint", False, "nextval('khub_operation_steps_id_seq'::regclass)"),
            "operation_id": ("uuid", False, None),
            "sequence": ("integer", False, None),
            "step_name": ("text", False, None),
            "step_version": ("text", False, None),
            "state": ("text", False, None),
            "attempt": ("integer", False, "0"),
            "delivery_key": ("text", False, None),
            "procrastinate_job_id": ("bigint", True, None),
            "input_json": ("jsonb", False, "'{}'::jsonb"),
            "checkpoint_json": ("jsonb", False, "'{}'::jsonb"),
            "external_marker": ("text", True, None),
            "expected_source": ("text", True, None),
            "expected_target": ("text", True, None),
            "artifact_key": ("text", True, None),
            "artifact_checksum": ("text", True, None),
            "artifact_length": ("bigint", True, None),
            "error_code": ("text", True, None),
            "error_summary": ("text", True, None),
            "started_at": timestamp,
            "heartbeat_at": timestamp,
            "finished_at": timestamp,
            "created_at": ("timestamp with time zone", False, "CURRENT_TIMESTAMP"),
            "updated_at": ("timestamp with time zone", False, "CURRENT_TIMESTAMP"),
        },
        "khub_commit_intents": {
            "id": ("uuid", False, None),
            "operation_id": ("uuid", True, None),
            "observation_operation_id": ("uuid", True, None),
            "repository_id": ("bigint", False, None),
            "requested_by_user_id": ("bigint", True, None),
            "ref": ("text", False, None),
            "base_head": ("text", False, None),
            "marker": ("text", False, None),
            "idempotency_key": ("text", True, None),
            "request_hash": ("text", True, None),
            "prepared_deadline_at": timestamp,
            "dispatch_started_at": timestamp,
            "remote_deadline_at": timestamp,
            "observe_not_before": timestamp,
            "observation_head": ("text", True, None),
            "observation_cursor": ("text", True, None),
            "payload_hash": ("text", False, None),
            "payload_json": ("jsonb", False, "'{}'::jsonb"),
            "state": ("text", False, None),
            "lakefs_commit_id": ("text", True, None),
            "result_json": ("jsonb", True, None),
            "error_code": ("text", True, None),
            "error_summary": ("text", True, None),
            "created_at": ("timestamp with time zone", False, "CURRENT_TIMESTAMP"),
            "updated_at": ("timestamp with time zone", False, "CURRENT_TIMESTAMP"),
            "finalized_at": timestamp,
            "version": ("bigint", False, "0"),
        },
        "khub_quota_reservations": {
            "id": ("uuid", False, None),
            "intent_id": ("uuid", False, None),
            "scope_type": ("text", False, None),
            "scope_id": ("bigint", False, None),
            "is_private": ("boolean", False, None),
            "reserved_bytes": ("bigint", False, None),
            "state": ("text", False, None),
            "created_at": ("timestamp with time zone", False, "CURRENT_TIMESTAMP"),
            "updated_at": ("timestamp with time zone", False, "CURRENT_TIMESTAMP"),
            "finished_at": timestamp,
        },
    }
