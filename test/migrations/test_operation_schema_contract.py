from __future__ import annotations

from kohakuhub.migrations.schema import (
    EXPECTED_OPERATION_CONSTRAINT_DEFINITIONS,
    EXPECTED_OPERATION_INDEX_DEFINITIONS,
    KERNEL_COLUMN_CONTRACT,
    operation_schema_object_diff,
    procrastinate_schema_diff,
    signature_digest,
)
from kohakuhub.operations.sql import (
    operation_column_contract,
    operation_table_columns,
    operation_table_columns_for_version,
)
from scripts.db_migrations._017_schema import (
    HISTORICAL_OPERATION_TABLE_COLUMNS_V2,
    HISTORICAL_OPERATION_TABLE_COLUMNS_V3,
    OPERATION_SCHEMA_SQL_V8,
)
from scripts.khub_migrate import OPERATION_SCHEMA_VERSION_WORKER
from scripts.khub_migrate import (
    _apply_operation_schema,
    _historical_operation_schema_checksum,
    _schema_checksum,
)


RELEASED_V2_LEDGER_CHECKSUM = (
    "edc67a3141b85e4b5dfff264609764e236d192d7086ba2d9b0571b49c381d23e"
)


class _CatalogConnection:
    def __init__(
        self, *, bad_index: str | None = None, bad_constraint: str | None = None
    ):
        self.bad_index = bad_index
        self.bad_constraint = bad_constraint

    def execute(self, query, params=None):
        if "FROM pg_indexes" in query:
            rows = [
                (name, definition, True)
                for name, definition in EXPECTED_OPERATION_INDEX_DEFINITIONS.items()
            ]
            if self.bad_index:
                rows = [
                    (
                        name,
                        "CREATE INDEX broken ON public.other (value)"
                        if name == self.bad_index
                        else definition,
                        valid,
                    )
                    for name, definition, valid in rows
                ]
            return _Rows(rows)
        if "FROM pg_constraint" in query:
            rows = [
                (name, kind, True, definition)
                for name, (
                    kind,
                    definition,
                ) in EXPECTED_OPERATION_CONSTRAINT_DEFINITIONS.items()
            ]
            if self.bad_constraint:
                rows = [
                    (
                        name,
                        kind,
                        True,
                        "CHECK (false)" if name == self.bad_constraint else definition,
                    )
                    for name, kind, valid, definition in rows
                ]
            return _Rows(rows)
        raise AssertionError(f"unexpected query: {query}")


class _Rows:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows


class _OperationUpgradeConnection:
    def __init__(self):
        self.ledger = {
            "khub-operation-kernel": (2, RELEASED_V2_LEDGER_CHECKSUM),
        }
        self.schema_statements = []

    def execute(self, query, params=None):
        normalized = " ".join(query.split())
        if (
            "migration_name = 'khub-operation-kernel'" in normalized
            and "migration_name LIKE" in normalized
        ):
            return _Rows(
                [
                    (name, version, checksum)
                    for name, (version, checksum) in self.ledger.items()
                    if name == "khub-operation-kernel"
                    or name.startswith("khub-operation-kernel-v")
                ]
            )
        if "WHERE migration_name = %s" in normalized:
            return _Row(self.ledger.get(params[0]))
        if "information_schema.tables" in normalized:
            return _Rows([])
        if "information_schema.columns" in normalized:
            return _Rows([])
        if normalized.startswith("INSERT INTO khub_schema_migrations"):
            name, version, checksum = params
            self.ledger.setdefault(name, (version, checksum))
            return _Row(None)
        raise AssertionError(f"unexpected query: {query}")

    def cursor(self):
        return _SchemaCursor(self.schema_statements)


class _Row:
    def __init__(self, row):
        self.row = row

    def fetchone(self):
        return self.row


class _SchemaCursor:
    def __init__(self, statements):
        self.statements = statements

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def execute(self, statement):
        self.statements.append(statement)


def test_v2_operation_signature_is_preserved_for_upgrade():
    assert (
        operation_table_columns_for_version(2) == HISTORICAL_OPERATION_TABLE_COLUMNS_V2
    )
    assert operation_table_columns_for_version(2) != operation_table_columns()
    assert _historical_operation_schema_checksum(2) == RELEASED_V2_LEDGER_CHECKSUM
    assert _historical_operation_schema_checksum(2) != signature_digest(
        HISTORICAL_OPERATION_TABLE_COLUMNS_V2
    )


def test_real_v2_ledger_is_upgraded_to_current_operation_schema():
    connection = _OperationUpgradeConnection()

    _apply_operation_schema(connection)

    assert connection.schema_statements == [OPERATION_SCHEMA_SQL_V8]
    assert connection.ledger[
        f"khub-operation-kernel-v{OPERATION_SCHEMA_VERSION_WORKER}"
    ] == (
        OPERATION_SCHEMA_VERSION_WORKER,
        _schema_checksum(),
    )
    assert connection.ledger["khub-operation-kernel"] == (
        2,
        RELEASED_V2_LEDGER_CHECKSUM,
    )


def test_newer_versioned_operation_ledger_is_used_as_upgrade_baseline():
    connection = _OperationUpgradeConnection()
    connection.ledger["khub-operation-kernel"] = (
        3,
        "legacy checksum from an older release",
    )
    connection.ledger["khub-operation-kernel-v7"] = (
        7,
        signature_digest(operation_table_columns_for_version(7)),
    )

    _apply_operation_schema(connection)

    assert connection.ledger[
        f"khub-operation-kernel-v{OPERATION_SCHEMA_VERSION_WORKER}"
    ] == (
        OPERATION_SCHEMA_VERSION_WORKER,
        _schema_checksum(),
    )


def test_v3_operation_signature_is_preserved_for_upgrade():
    assert (
        operation_table_columns_for_version(3) == HISTORICAL_OPERATION_TABLE_COLUMNS_V3
    )
    assert operation_table_columns_for_version(3) != operation_table_columns()
    assert _historical_operation_schema_checksum(3) == signature_digest(
        HISTORICAL_OPERATION_TABLE_COLUMNS_V3
    )


def test_current_kernel_contract_covers_every_operation_column():
    assert set(KERNEL_COLUMN_CONTRACT) == set(operation_table_columns())
    assert KERNEL_COLUMN_CONTRACT == operation_column_contract()
    for table, columns in operation_table_columns().items():
        assert set(columns) == set(KERNEL_COLUMN_CONTRACT[table])


def test_operation_schema_rejects_same_named_but_wrong_index_definition():
    result = operation_schema_object_diff(
        _CatalogConnection(bad_index="khub_operation_active_resource_uidx")
    )
    assert result["missing_indexes"] == []
    assert result["invalid_indexes"] == ["khub_operation_active_resource_uidx"]


def test_operation_schema_rejects_same_named_but_wrong_constraint_definition():
    result = operation_schema_object_diff(
        _CatalogConnection(bad_constraint="khub_step_delivery_uidx")
    )
    assert result["missing_constraints"] == []
    assert result["invalid_constraints"] == ["khub_step_delivery_uidx"]


def test_procrastinate_schema_contract_rejects_missing_index():
    class ProcrastinateCatalog(_CatalogConnection):
        def execute(self, query, params=None):
            if "information_schema.columns" in query:
                return _Rows(
                    (table, column)
                    for table, columns in {
                        "procrastinate_workers": {"id", "last_heartbeat"},
                        "procrastinate_jobs": {
                            "id",
                            "queue_name",
                            "task_name",
                            "priority",
                            "lock",
                            "queueing_lock",
                            "args",
                            "status",
                            "scheduled_at",
                            "attempts",
                            "abort_requested",
                            "worker_id",
                        },
                        "procrastinate_periodic_defers": {
                            "id",
                            "task_name",
                            "defer_timestamp",
                            "job_id",
                            "periodic_id",
                        },
                        "procrastinate_events": {"id", "job_id", "type", "at"},
                    }.items()
                    for column in columns
                )
            if "FROM pg_indexes" in query:
                return _Rows([])
            if "FROM pg_type" in query:
                return _Rows([])
            raise AssertionError(query)

    result = procrastinate_schema_diff(ProcrastinateCatalog())
    assert "procrastinate_jobs_queue_name_idx_v1" in result["missing_indexes"]
    assert result["missing_columns"] == []
