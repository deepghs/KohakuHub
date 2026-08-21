"""Real-DB migration matrix from released schemas through migration 017."""

from __future__ import annotations

import pytest

from scripts.khub_migrate import (
    APPLICATION_TABLE_COLUMNS_WORKER,
    _expected_worker_ledger_records,
)
from test.migrations.support.migration_history_database import (
    CURRENT_RUNNER,
    IsolatedPostgresDatabase,
    MigrationHistoryError,
    PreparedMigrationHistory,
    ROOT_DIR,
    _insert_row,
    _complete_main_016_backfill,
    _initialize_archived_schema,
    _run_archived_migrations,
    _run_script,
    application_table_columns,
    archive_parent,
    archive_release,
    foreign_key_contract_missing,
    create_sqlite_database,
    invitation_created_by_nullable,
    ledger_rows,
    membership_foreign_key_points_to_user,
    membership_relationships,
    postgres_dsn,
    postgres_column_types,
    prepare_migration_history,
    repository_lakefs_values,
    seed_database,
    sqlite_foreign_key_violations,
    snapshot_database,
)
from test.migrations.support.migration_history_manifest import (
    HISTORICAL_RELEASES,
    MAIN_RELEASE,
    migration_history_releases,
)


def _matrix_parameters():
    parameters = []
    for release in migration_history_releases():
        parameters.append(
            pytest.param((release, "sqlite"), id=f"{release.slug}-sqlite")
        )
        parameters.append(
            pytest.param((release, "postgres"), id=f"{release.slug}-postgres")
        )
    return parameters


@pytest.fixture(scope="module", params=_matrix_parameters())
def migration_history_case(request, tmp_path_factory) -> PreparedMigrationHistory:
    release, backend = request.param
    workdir = tmp_path_factory.mktemp(f"migration-history-{release.slug}-{backend}")

    if backend == "sqlite":
        database = create_sqlite_database(workdir, release)
        yield prepare_migration_history(release, database, workdir)
        return

    dsn = postgres_dsn()
    if dsn is None:
        pytest.skip(
            "set KOHAKU_HUB_MIGRATION_HISTORY_DSN to enable isolated PostgreSQL matrix cases"
        )
    try:
        with IsolatedPostgresDatabase(dsn) as isolated:
            yield prepare_migration_history(release, isolated, workdir)
    except MigrationHistoryError as exc:
        raise AssertionError(str(exc)) from exc


def test_real_fresh_sqlite_database_reaches_current_schema(tmp_path):
    database = create_sqlite_database(tmp_path, MAIN_RELEASE)

    result = _run_script(CURRENT_RUNNER, database, cwd=ROOT_DIR)

    assert result.succeeded, result.diagnostic()
    assert "Running 017_durable_worker_schema..." in result.stdout
    columns = application_table_columns(database)
    for table, expected_columns in APPLICATION_TABLE_COLUMNS_WORKER.items():
        assert set(expected_columns).issubset(columns[table])


def test_real_fresh_postgres_database_reaches_017():
    dsn = postgres_dsn()
    if dsn is None:
        pytest.skip(
            "set KOHAKU_HUB_MIGRATION_HISTORY_DSN to enable fresh PostgreSQL coverage"
        )

    with IsolatedPostgresDatabase(dsn) as database:
        result = _run_script(CURRENT_RUNNER, database, cwd=ROOT_DIR)

        assert result.succeeded, result.diagnostic()
        assert set(ledger_rows(database)) == {
            (name, version, checksum)
            for name, (version, checksum) in _expected_worker_ledger_records().items()
        }


def test_postgres_017_rejects_extra_application_schema_and_rolls_back(tmp_path):
    dsn = postgres_dsn()
    if dsn is None:
        pytest.skip(
            "set KOHAKU_HUB_MIGRATION_HISTORY_DSN to enable PostgreSQL rollback coverage"
        )

    import psycopg

    with IsolatedPostgresDatabase(dsn) as database:
        archive_root = archive_release(MAIN_RELEASE, tmp_path / "main")
        _initialize_archived_schema(archive_root, database)
        seed_database(database)
        _complete_main_016_backfill(database)

        with psycopg.connect(database.url) as connection:
            # Force 017's compatibility bootstrap to do real work before the
            # boundary check rejects the intentionally unexpected table.
            connection.execute("DROP TABLE fallbacksource")
            connection.execute(
                "CREATE TABLE unexpected_application_table (id BIGINT PRIMARY KEY)"
            )
            connection.commit()

        before = (
            snapshot_database(database),
            application_table_columns(database),
            ledger_rows(database),
        )
        result = _run_script(CURRENT_RUNNER, database, cwd=ROOT_DIR)
        after = (
            snapshot_database(database),
            application_table_columns(database),
            ledger_rows(database),
        )

        assert not result.succeeded
        assert "extra_tables" in result.stdout
        assert after == before


@pytest.mark.parametrize(
    ("mutation", "diagnostic"),
    (
        ("type", "semantic_columns"),
        ("nullable", "semantic_columns"),
        ("index", "invalid_indexes"),
        ("primary_key", "missing_constraints"),
    ),
)
def test_postgres_017_rejects_application_semantic_drift(
    tmp_path, mutation, diagnostic
):
    dsn = postgres_dsn()
    if dsn is None:
        pytest.skip(
            "set KOHAKU_HUB_MIGRATION_HISTORY_DSN to enable PostgreSQL schema coverage"
        )

    import psycopg
    from psycopg import sql

    with IsolatedPostgresDatabase(dsn) as database:
        archive_root = archive_release(MAIN_RELEASE, tmp_path / "main")
        _initialize_archived_schema(archive_root, database)
        seed_database(database)
        _complete_main_016_backfill(database)

        with psycopg.connect(database.url) as connection:
            if mutation == "type":
                connection.execute(
                    'ALTER TABLE "user" ALTER COLUMN username TYPE TEXT'
                )
            elif mutation == "nullable":
                connection.execute(
                    'ALTER TABLE "user" ALTER COLUMN username DROP NOT NULL'
                )
            elif mutation == "index":
                connection.execute('DROP INDEX "user_is_org"')
                connection.execute(
                    'CREATE INDEX "user_is_org" ON "user" ("is_org") '
                    'WHERE "is_org" = TRUE'
                )
            else:
                constraint_name = connection.execute(
                    """
                    SELECT conname
                    FROM pg_constraint
                    WHERE conrelid = 'emailverification'::regclass
                      AND contype = 'p'
                    LIMIT 1
                    """
                ).fetchone()[0]
                connection.execute(
                    sql.SQL("ALTER TABLE emailverification DROP CONSTRAINT {}")
                    .format(sql.Identifier(constraint_name))
                )
            # Release the DDL lock before the runner opens its own connection.
            connection.commit()

            if mutation == "primary_key":
                from scripts.khub_migrate import _application_schema_semantic_diff

                with psycopg.connect(database.url) as connection:
                    semantic_diff = _application_schema_semantic_diff(connection)
                assert semantic_diff["missing_constraints"]

        before = (_postgres_schema_snapshot(database), ledger_rows(database))
        result = _run_script(CURRENT_RUNNER, database, cwd=ROOT_DIR)
        after = (_postgres_schema_snapshot(database), ledger_rows(database))

        assert not result.succeeded, result.diagnostic()
        assert diagnostic in result.stdout
        assert after == before


def _postgres_schema_snapshot(database):
    import psycopg

    with psycopg.connect(database.url) as connection:
        columns = connection.execute(
            """
            SELECT relation.relname, attribute.attname,
                   format_type(attribute.atttypid, attribute.atttypmod),
                   attribute.attnotnull,
                   pg_get_expr(default_value.adbin, default_value.adrelid)
            FROM pg_class relation
            JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
            JOIN pg_attribute attribute ON attribute.attrelid = relation.oid
            LEFT JOIN pg_attrdef default_value
              ON default_value.adrelid = relation.oid
             AND default_value.adnum = attribute.attnum
            WHERE namespace.nspname = current_schema()
              AND relation.relkind = 'r'
              AND attribute.attnum > 0
              AND NOT attribute.attisdropped
            ORDER BY relation.relname, attribute.attnum
            """
        ).fetchall()
        constraints = connection.execute(
            """
            SELECT constraint_info.conrelid::regclass::text,
                   constraint_info.conname,
                   constraint_info.contype,
                   pg_get_constraintdef(constraint_info.oid)
            FROM pg_constraint constraint_info
            WHERE constraint_info.connamespace = current_schema()::regnamespace
            ORDER BY 1, 2
            """
        ).fetchall()
        indexes = connection.execute(
            """
            SELECT tablename, indexname, indexdef
            FROM pg_indexes
            WHERE schemaname = current_schema()
            ORDER BY tablename, indexname
            """
        ).fetchall()
    return columns, constraints, indexes


def test_postgres_008_failure_rolls_back_schema_and_data(tmp_path):
    dsn = postgres_dsn()
    if dsn is None:
        pytest.skip(
            "set KOHAKU_HUB_MIGRATION_HISTORY_DSN to enable PostgreSQL rollback coverage"
        )

    import psycopg

    release = next(item for item in HISTORICAL_RELEASES if item.slug == "v007")
    with IsolatedPostgresDatabase(dsn) as database:
        archive_root = archive_release(MAIN_RELEASE, tmp_path / "main")
        base_root = archive_parent(release, tmp_path / "base")
        _initialize_archived_schema(base_root, database)
        seed_database(database)
        legacy_run = _run_archived_migrations(
            archive_root, database, release.migration_numbers[-1]
        )
        assert legacy_run.succeeded, legacy_run.diagnostic()

        with psycopg.connect(database.url) as connection:
            _insert_row(
                connection,
                database,
                "user",
                {
                    "username": "migration_owner",
                    "email": "migration-conflict@example.test",
                    "password_hash": "migration-password-hash",
                    "email_verified": False,
                    "is_active": True,
                    "private_used_bytes": 0,
                    "public_used_bytes": 0,
                    "created_at": "2025-01-02 03:04:05",
                },
            )
            connection.commit()

        before = snapshot_database(database), _postgres_schema_snapshot(database)
        result = _run_script(CURRENT_RUNNER, database, cwd=ROOT_DIR)
        after = snapshot_database(database), _postgres_schema_snapshot(database)

        assert not result.succeeded
        assert "legacy names collide" in result.stdout
        assert "Running 017_durable_worker_schema..." not in result.stdout
        assert after == before


def test_current_runner_reaches_017(
    migration_history_case: PreparedMigrationHistory,
):
    case = migration_history_case

    assert case.first_run.succeeded, case.first_run.diagnostic()
    assert "Running 017_durable_worker_schema..." in case.first_run.stdout
    if case.release == MAIN_RELEASE:
        assert "Running 001_repository_schema..." in case.first_run.stdout


def test_historical_checkpoint_was_built_by_archived_migrations(
    migration_history_case: PreparedMigrationHistory,
):
    case = migration_history_case
    if not case.release.migration_numbers:
        pytest.skip("main is the post-legacy boundary, not a numbered checkpoint")

    assert case.legacy_run.succeeded, case.legacy_run.diagnostic()
    for number in case.release.migration_numbers:
        assert f"Running archived {number:03d}_" in case.legacy_run.stdout


def _require_successful_upgrade(case: PreparedMigrationHistory) -> None:
    if not case.first_run.succeeded:
        pytest.skip(
            "the first upgrade failed; the completion test reports its diagnostic"
        )


def test_historical_data_survives_current_runner_upgrade(
    migration_history_case: PreparedMigrationHistory,
):
    case = migration_history_case
    _require_successful_upgrade(case)

    assert case.after_first == case.before


def test_current_runner_produces_the_released_application_schema(
    migration_history_case: PreparedMigrationHistory,
):
    case = migration_history_case
    _require_successful_upgrade(case)

    columns = application_table_columns(case.database)
    for table, expected_columns in APPLICATION_TABLE_COLUMNS_WORKER.items():
        assert table in columns
        assert set(expected_columns).issubset(columns[table])


def test_large_file_size_columns_are_bigint_on_postgres(
    migration_history_case: PreparedMigrationHistory,
):
    case = migration_history_case
    _require_successful_upgrade(case)
    if case.database.backend != "postgres":
        pytest.skip("PostgreSQL is required to distinguish BIGINT from INTEGER")

    types = postgres_column_types(
        case.database,
        (
            ("file", "size"),
            ("stagingupload", "size"),
            ("lfsobjecthistory", "size"),
        ),
    )
    assert types == {
        ("file", "size"): "bigint",
        ("stagingupload", "size"): "bigint",
        ("lfsobjecthistory", "size"): "bigint",
    }


def test_repository_lakefs_id_is_backfilled(
    migration_history_case: PreparedMigrationHistory,
):
    case = migration_history_case
    _require_successful_upgrade(case)

    from kohakuhub.utils.lakefs import lakefs_repo_name

    rows = dict(repository_lakefs_values(case.database))
    assert rows["migration-owner/preserved-repository"] == lakefs_repo_name(
        "model", "migration-owner/preserved-repository"
    )


def test_current_runner_repairs_relationship_constraints(
    migration_history_case: PreparedMigrationHistory,
):
    case = migration_history_case
    _require_successful_upgrade(case)

    assert invitation_created_by_nullable(case.database) is True
    assert membership_foreign_key_points_to_user(case.database)
    assert all(
        bool(is_org)
        for _user_id, _organization_id, is_org in membership_relationships(
            case.database
        )
    )
    assert foreign_key_contract_missing(case.database) == ()
    assert sqlite_foreign_key_violations(case.database) == ()


def test_postgres_runner_records_the_017_worker_ledger(
    migration_history_case: PreparedMigrationHistory,
):
    case = migration_history_case
    _require_successful_upgrade(case)
    if case.database.backend != "postgres":
        pytest.skip("migration 017 worker ledger is PostgreSQL-only")

    assert set(ledger_rows(case.database)) == {
        (name, version, checksum)
        for name, (version, checksum) in _expected_worker_ledger_records().items()
    }


def test_sqlite_runner_does_not_create_the_postgres_017_ledger(
    migration_history_case: PreparedMigrationHistory,
):
    case = migration_history_case
    _require_successful_upgrade(case)
    if case.database.backend != "sqlite":
        pytest.skip("SQLite has no migration 017 worker ledger")

    assert ledger_rows(case.database) == ()


def test_current_runner_is_idempotent_after_historical_upgrade(
    migration_history_case: PreparedMigrationHistory,
):
    case = migration_history_case
    _require_successful_upgrade(case)

    before_second_run = (
        snapshot_database(case.database),
        application_table_columns(case.database),
        ledger_rows(case.database),
    )
    second_run = case.run_current_runner()
    after_second_run = (
        snapshot_database(case.database),
        application_table_columns(case.database),
        ledger_rows(case.database),
    )

    assert second_run.succeeded, second_run.diagnostic()
    assert after_second_run == before_second_run
