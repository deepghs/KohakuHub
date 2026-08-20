"""Real database support for the archive-backed migration-history matrix."""

from __future__ import annotations

import io
import os
from dataclasses import dataclass
from pathlib import Path
import subprocess
import sys
import tarfile
from typing import Any
from urllib.parse import urlsplit, urlunsplit
import uuid

from .migration_history_manifest import HistoricalRelease, MAIN_RELEASE


ROOT_DIR = Path(__file__).resolve().parents[3]
CURRENT_RUNNER = ROOT_DIR / "scripts" / "run_migrations.py"
PYTHON = sys.executable
POSTGRES_DSN_ENV = "KOHAKU_HUB_MIGRATION_HISTORY_DSN"
POSTGRES_DATABASE_PREFIX = "khub_migration_history_"
FIXED_TIMESTAMP = "2025-01-02 03:04:05"

FOREIGN_KEY_CONTRACT = (
    ("emailverification", "user_id", "user", "CASCADE"),
    ("session", "user_id", "user", "CASCADE"),
    ("token", "user_id", "user", "CASCADE"),
    ("repository", "owner_id", "user", "CASCADE"),
    ("file", "repository_id", "repository", "CASCADE"),
    ("file", "owner_id", "user", "CASCADE"),
    ("stagingupload", "repository_id", "repository", "CASCADE"),
    ("stagingupload", "uploader_id", "user", "SET NULL"),
    ("userorganization", "user_id", "user", "CASCADE"),
    ("userorganization", "organization_id", "user", "CASCADE"),
    ("commit", "repository_id", "repository", "CASCADE"),
    ("commit", "author_id", "user", "CASCADE"),
    ("commit", "owner_id", "user", "CASCADE"),
    ("lfsobjecthistory", "repository_id", "repository", "CASCADE"),
    ("lfsobjecthistory", "file_id", "file", "SET NULL"),
    ("sshkey", "user_id", "user", "CASCADE"),
    ("invitation", "created_by_id", "user", "CASCADE"),
    ("invitation", "used_by_id", "user", "SET NULL"),
)


class MigrationHistoryError(RuntimeError):
    """Raised when a matrix database cannot be prepared safely."""


@dataclass(frozen=True, slots=True)
class CommandResult:
    """Captured runner result with enough context to diagnose a failed case."""

    command: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str

    @property
    def succeeded(self) -> bool:
        return self.returncode == 0

    def diagnostic(self) -> str:
        return (
            f"command={' '.join(self.command)}\n"
            f"returncode={self.returncode}\n"
            f"stdout:\n{self.stdout[-12000:]}\n"
            f"stderr:\n{self.stderr[-12000:]}"
        )


@dataclass(frozen=True, slots=True)
class MatrixDatabase:
    """A database URL and backend name owned by one matrix case."""

    backend: str
    url: str
    sqlite_path: Path | None = None
    postgres_name: str | None = None


@dataclass(slots=True)
class PreparedMigrationHistory:
    """State produced by one legacy-checkpoint-to-current upgrade attempt."""

    release: HistoricalRelease
    database: MatrixDatabase
    archive_root: Path
    legacy_run: CommandResult
    first_run: CommandResult
    before: dict[str, tuple[tuple[Any, ...], ...]]
    after_first: dict[str, tuple[tuple[Any, ...], ...]] | None

    def run_current_runner(self) -> CommandResult:
        """Run the checked-out numbered runner against this case again."""

        return _run_script(
            CURRENT_RUNNER,
            self.database,
            cwd=ROOT_DIR,
        )


def postgres_dsn() -> str | None:
    """Return the explicit matrix DSN, if the caller enabled PostgreSQL tests."""

    value = os.environ.get(POSTGRES_DSN_ENV, "").strip()
    return value or None


def archive_release(release: HistoricalRelease, destination: Path) -> Path:
    """Extract a trusted Git archive into an isolated temporary directory."""

    return archive_commit(release.commit, release.label, destination)


def archive_commit(commit: str, label: str, destination: Path) -> Path:
    """Extract one repository commit into an isolated temporary directory."""

    destination.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(
        ["git", "archive", "--format=tar", commit],
        cwd=ROOT_DIR,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        raise MigrationHistoryError(
            f"unable to archive {label} ({commit}): "
            f"{result.stderr.decode(errors='replace')}"
        )
    with tarfile.open(fileobj=io.BytesIO(result.stdout), mode="r:") as archive:
        archive.extractall(destination)
    return destination


def archive_parent(release: HistoricalRelease, destination: Path) -> Path:
    """Extract the schema-producing commit immediately before a release."""

    result = subprocess.run(
        ["git", "rev-parse", f"{release.commit}^"],
        cwd=ROOT_DIR,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.returncode != 0:
        raise MigrationHistoryError(
            f"unable to resolve parent of {release.label} ({release.commit}): "
            f"{result.stderr}"
        )
    return archive_commit(result.stdout.strip(), f"{release.label}-parent", destination)


def prepare_migration_history(
    release: HistoricalRelease,
    database: MatrixDatabase,
    workdir: Path,
) -> PreparedMigrationHistory:
    """Build a real legacy checkpoint, then run the checked-out chain."""

    archive_root = archive_release(MAIN_RELEASE, workdir / "main")
    if release.migration_numbers:
        base_root = archive_parent(release, workdir / "base")
        _initialize_archived_schema(base_root, database)
        seed_database(database)
        legacy_run = _run_archived_migrations(
            archive_root,
            database,
            release.migration_numbers[-1],
        )
        if not legacy_run.succeeded:
            raise MigrationHistoryError(
                f"failed to build archived {release.label} migration checkpoint:\n"
                f"{legacy_run.diagnostic()}"
            )
    else:
        _initialize_archived_schema(archive_root, database)
        seed_database(database)
        if release.slug == "main":
            _complete_main_016_backfill(database)
        legacy_run = CommandResult(("archived-migrations",), 0, "", "")
    before = snapshot_database(database)
    first_run = _run_script(CURRENT_RUNNER, database, cwd=ROOT_DIR)
    after_first = snapshot_database(database) if first_run.succeeded else None
    return PreparedMigrationHistory(
        release=release,
        database=database,
        archive_root=archive_root,
        legacy_run=legacy_run,
        first_run=first_run,
        before=before,
        after_first=after_first,
    )


def create_sqlite_database(workdir: Path, release: HistoricalRelease) -> MatrixDatabase:
    """Create a per-case SQLite file path without opening a shared database."""

    path = workdir / f"{release.slug}.sqlite3"
    return MatrixDatabase("sqlite", f"sqlite:///{path}", sqlite_path=path)


class IsolatedPostgresDatabase:
    """Create and destroy one uniquely named PostgreSQL database."""

    def __init__(self, base_dsn: str):
        self.base_dsn = base_dsn
        self.name = f"{POSTGRES_DATABASE_PREFIX}{uuid.uuid4().hex[:20]}"
        self._dsn: str | None = None

    def __enter__(self) -> MatrixDatabase:
        try:
            import psycopg
            from psycopg import sql
            from psycopg.conninfo import conninfo_to_dict, make_conninfo
        except ImportError as exc:  # pragma: no cover - dependency is project-owned
            raise MigrationHistoryError(
                "psycopg is required for PostgreSQL matrix tests"
            ) from exc

        connection_info = conninfo_to_dict(self.base_dsn)
        if not connection_info.get("host") and not connection_info.get("service"):
            raise MigrationHistoryError("PostgreSQL matrix DSN must identify a server")
        admin_info = dict(connection_info)
        admin_info["dbname"] = (
            "template1" if connection_info.get("dbname") == "postgres" else "postgres"
        )
        # psycopg accepts keyword-style libpq conninfo, but the archived
        # application versions parse ``KOHAKU_HUB_DATABASE_URL`` as a URI.
        # Keep the admin connection as conninfo and expose a URI to every
        # subprocess so the matrix exercises the released code unchanged.
        if self.base_dsn.startswith(("postgresql://", "postgres://")):
            parsed = urlsplit(self.base_dsn)
            self._dsn = urlunsplit(
                (
                    parsed.scheme,
                    parsed.netloc,
                    f"/{self.name}",
                    parsed.query,
                    parsed.fragment,
                )
            )
        else:
            raise MigrationHistoryError(
                "PostgreSQL matrix DSN must be a postgresql:// URI so archived "
                "releases can consume it"
            )
        try:
            with psycopg.connect(
                make_conninfo(**admin_info), autocommit=True
            ) as connection:
                connection.execute(
                    sql.SQL("CREATE DATABASE {}").format(sql.Identifier(self.name))
                )
        except Exception as exc:
            raise MigrationHistoryError(
                f"cannot create isolated PostgreSQL database {self.name}: {exc}"
            ) from exc
        return MatrixDatabase("postgres", self._dsn, postgres_name=self.name)

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        if self._dsn is None:
            return
        try:
            import psycopg
            from psycopg import sql
            from psycopg.conninfo import conninfo_to_dict, make_conninfo

            connection_info = conninfo_to_dict(self.base_dsn)
            admin_info = dict(connection_info)
            admin_info["dbname"] = (
                "template1"
                if connection_info.get("dbname") == "postgres"
                else "postgres"
            )
            with psycopg.connect(
                make_conninfo(**admin_info), autocommit=True
            ) as connection:
                connection.execute(
                    "SELECT pg_terminate_backend(pid) "
                    "FROM pg_stat_activity "
                    "WHERE datname = %s AND pid <> pg_backend_pid()",
                    (self.name,),
                )
                connection.execute(
                    sql.SQL("DROP DATABASE IF EXISTS {}").format(
                        sql.Identifier(self.name)
                    )
                )
        except Exception:
            # Cleanup must not replace the migration failure. The database name
            # is generated above and the next test run cannot reuse it.
            pass


def _subprocess_environment(database: MatrixDatabase) -> dict[str, str]:
    environment = os.environ.copy()
    for key in list(environment):
        if key.startswith("KOHAKU_HUB_"):
            environment.pop(key)
    environment.update(
        {
            "KOHAKU_HUB_DB_BACKEND": database.backend,
            "KOHAKU_HUB_DATABASE_URL": database.url,
            "KOHAKU_HUB_AUTO_MIGRATE": "true",
            "KOHAKU_HUB_S3_PUBLIC_ENDPOINT": "http://127.0.0.1:39001",
            "KOHAKU_HUB_S3_ENDPOINT": "http://127.0.0.1:39001",
            "KOHAKU_HUB_S3_ACCESS_KEY": "migration-history-access",
            "KOHAKU_HUB_S3_SECRET_KEY": "migration-history-secret",
            "KOHAKU_HUB_S3_BUCKET": "migration-history-bucket",
            "KOHAKU_HUB_S3_REGION": "us-east-1",
            "KOHAKU_HUB_LAKEFS_ENDPOINT": "http://127.0.0.1:39002",
            "KOHAKU_HUB_LAKEFS_ACCESS_KEY": "migration-history-access",
            "KOHAKU_HUB_LAKEFS_SECRET_KEY": "migration-history-secret",
            "KOHAKU_HUB_LAKEFS_REPO_NAMESPACE": "history",
            "KOHAKU_HUB_DATABASE_KEY": "migration-history-key",
            "PYTHONPATH": "",
        }
    )
    return environment


def _initialize_archived_schema(archive_root: Path, database: MatrixDatabase) -> None:
    script = r"""
import importlib
import sys

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib
    sys.modules["tomllib"] = tomllib

sys.path.insert(0, sys.argv[1])
module = importlib.import_module("kohakuhub.db")
module.init_db()
module.db.close()
"""
    result = subprocess.run(
        [PYTHON, "-c", script, str(archive_root / "src")],
        cwd=archive_root,
        env=_subprocess_environment(database),
        text=True,
        capture_output=True,
        timeout=120,
    )
    if result.returncode != 0:
        raise MigrationHistoryError(
            f"failed to initialize archived {archive_root.name} schema:\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )


def _run_archived_migrations(
    archive_root: Path,
    database: MatrixDatabase,
    migration_number: int,
) -> CommandResult:
    """Execute archived migration scripts through one real release boundary.

    The historical runner has no stop-at-version option and always discovers
    every script present in its checkout.  This small subprocess explicitly
    stops at the requested checkpoint.  The parent release has already
    initialized the database; deliberately avoiding another archived
    ``init_db()`` keeps later model columns from contaminating the checkpoint.
    The scripts themselves are loaded from the archived release, so this is
    not a replay using current migration code.
    """

    script = r"""
import importlib.util
from pathlib import Path
import sys

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib
    sys.modules["tomllib"] = tomllib

archive_root = Path(sys.argv[1])
target = int(sys.argv[2])
sys.path.insert(0, str(archive_root / "src"))
sys.path.insert(0, str(archive_root / "scripts"))
sys.path.insert(0, str(archive_root / "scripts" / "db_migrations"))

from kohakuhub.config import cfg
from kohakuhub.db import db, init_db

if cfg.app.db_backend == "sqlite":
    # Historical migrations 005+ explicitly call Peewee commit after SQLite
    # DDL.  Depending on the driver version, the DDL has already ended the
    # transaction and that otherwise harmless COMMIT raises.  Keep the
    # archived script behavior/data while normalizing this driver quirk.
    _legacy_commit = db.commit

    def _commit_legacy_sqlite():
        try:
            _legacy_commit()
        except Exception as exc:
            if "no transaction is active" not in str(exc).lower():
                raise
            print("Tolerated SQLite COMMIT with no active transaction.")

    db.commit = _commit_legacy_sqlite

migration_dir = archive_root / "scripts" / "db_migrations"
paths = []
for path in migration_dir.glob("*.py"):
    number, separator, _ = path.stem.partition("_")
    if separator and number.isdecimal() and int(number) <= target:
        paths.append((int(number), path))

for number, path in sorted(paths):
    module_name = f"archived_migration_{number:03d}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load archived migration {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if not hasattr(module, "run"):
        raise RuntimeError(f"archived migration {path.name} has no run()")
    print(f"Running archived {path.stem}...")
    try:
        success = module.run()
    except Exception:
        if number == 14:
            # The released 014 script passes (db, cfg, number) to a helper
            # whose released signature is (number, db, cfg), so it aborts
            # before changing the database.  The current 014 migration is
            # responsible for taking over this still-valid v013 state.
            print(
                "Archived 014 stopped at its argument-order bug; preserving "
                "the unchanged legacy state for current repair."
            )
            continue
        raise
    if not success:
        # Commit ffd17d7 removed migrate_repository_schema.py while the
        # archived 001 script still imported it.  On SQLite the same script
        # also unconditionally attempts that import even when the desired
        # non-unique schema is already present.  Preserve the real legacy
        # state and continue only for this documented historical defect.
        if (
            number == 1
            and not (archive_root / "scripts" / "migrate_repository_schema.py").exists()
        ):
            print(
                "Archived 001 failed because its removed helper is absent; "
                "the base schema already represents the intended 001 state."
            )
            continue
        if number == 8:
            # The released 008 script has backend-specific defects after the
            # merge has partially committed: SQLite inserts organization rows
            # before rebuilding nullable credentials, while PostgreSQL refers
            # to ``userorganization.organization`` instead of Peewee's actual
            # ``organization_id`` column. Keep that committed partial state so
            # the checked-out 008 repair path is tested against what the old
            # script actually left behind.
            print(
                "Archived 008 stopped at its known legacy backend bug; "
                "preserving the partial state for current repair."
            )
            continue
        raise RuntimeError(f"archived migration {path.name} returned failure")

db.close()
"""
    result = subprocess.run(
        [PYTHON, "-c", script, str(archive_root), str(migration_number)],
        cwd=archive_root,
        env=_subprocess_environment(database),
        text=True,
        capture_output=True,
        timeout=300,
    )
    return CommandResult(
        (PYTHON, "-c", "<archived migration runner>"),
        result.returncode,
        result.stdout,
        result.stderr,
    )


def _run_script(script: Path, database: MatrixDatabase, *, cwd: Path) -> CommandResult:
    command = (PYTHON, str(script))
    result = subprocess.run(
        command,
        cwd=cwd,
        env=_subprocess_environment(database),
        text=True,
        capture_output=True,
        timeout=300,
    )
    return CommandResult(command, result.returncode, result.stdout, result.stderr)


def _connect(database: MatrixDatabase):
    if database.backend == "sqlite":
        import sqlite3

        if database.sqlite_path is None:
            raise MigrationHistoryError("SQLite matrix database has no file path")
        connection = sqlite3.connect(database.sqlite_path)
        connection.execute("PRAGMA foreign_keys = ON")
        return connection
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - dependency is project-owned
        raise MigrationHistoryError(
            "psycopg is required for PostgreSQL matrix tests"
        ) from exc
    return psycopg.connect(database.url)


def _quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _table_columns(connection, database: MatrixDatabase, table: str) -> tuple[str, ...]:
    if database.backend == "sqlite":
        rows = connection.execute(
            f"PRAGMA table_info({_quote_identifier(table)})"
        ).fetchall()
        return tuple(row[1] for row in rows)
    rows = connection.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = current_schema() AND table_name = %s "
        "ORDER BY ordinal_position",
        (table,),
    ).fetchall()
    return tuple(row[0] for row in rows)


def _table_exists(connection, database: MatrixDatabase, table: str) -> bool:
    if database.backend == "sqlite":
        return (
            connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
                (table,),
            ).fetchone()
            is not None
        )
    return (
        connection.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = current_schema() AND table_name = %s",
            (table,),
        ).fetchone()
        is not None
    )


def _execute(connection, database: MatrixDatabase, query: str, params=()):
    if database.backend == "sqlite":
        return connection.execute(query, params)
    return connection.execute(query, params)


def _candidate_row(columns: tuple[str, ...], values: dict[str, Any]) -> dict[str, Any]:
    return {column: values[column] for column in columns if column in values}


def _insert_row(
    connection, database: MatrixDatabase, table: str, values: dict[str, Any]
) -> None:
    columns = _table_columns(connection, database, table)
    row = _candidate_row(columns, values)
    if not row:
        raise MigrationHistoryError(
            f"no seed columns matched {table}: {sorted(values)}"
        )
    names = tuple(row)
    placeholders = ", ".join(
        "?" if database.backend == "sqlite" else "%s" for _ in names
    )
    statement = (
        f"INSERT INTO {_quote_identifier(table)} "
        f"({', '.join(_quote_identifier(name) for name in names)}) "
        f"VALUES ({placeholders})"
    )
    try:
        _execute(connection, database, statement, tuple(row[name] for name in names))
    except Exception as exc:
        raise MigrationHistoryError(f"failed to seed {table}: {exc}") from exc


_SEED_ROWS: tuple[tuple[str, dict[str, Any]], ...] = (
    (
        "user",
        {
            "id": 1,
            "username": "migration-owner",
            "normalized_name": "migrationowner",
            "is_org": False,
            "email": "migration-owner@example.test",
            "password_hash": "migration-password-hash",
            "email_verified": True,
            "is_active": True,
            "private_quota_bytes": 100000,
            "public_quota_bytes": 200000,
            "private_used_bytes": 101,
            "public_used_bytes": 202,
            "full_name": "Migration Owner",
            "bio": "historical user",
            "description": "historical user",
            "website": "https://example.test/owner",
            "social_media": '{"github":"migration-owner"}',
            "avatar": b"history-avatar",
            "avatar_updated_at": FIXED_TIMESTAMP,
            "created_at": FIXED_TIMESTAMP,
        },
    ),
    (
        "user",
        {
            "id": 20,
            "username": "migration-org",
            "normalized_name": "migrationorg",
            "is_org": True,
            "email": "migration-org@example.test",
            "password_hash": "migration-org-password-hash",
            "email_verified": False,
            "is_active": True,
            "private_quota_bytes": 300000,
            "public_quota_bytes": 400000,
            "private_used_bytes": 303,
            "public_used_bytes": 404,
            "description": "historical organization",
            "created_at": FIXED_TIMESTAMP,
        },
    ),
    (
        "organization",
        {
            "id": 20,
            "name": "migration-org",
            "username": "migration-org",
            "normalized_name": "migrationorg",
            "is_org": True,
            "description": "historical organization",
            "bio": "historical organization",
            "website": "https://example.test/org",
            "social_media": '{"github":"migration-org"}',
            "avatar": b"history-org-avatar",
            "avatar_updated_at": FIXED_TIMESTAMP,
            "private_quota_bytes": 300000,
            "public_quota_bytes": 400000,
            "private_used_bytes": 303,
            "public_used_bytes": 404,
            "created_at": FIXED_TIMESTAMP,
        },
    ),
    (
        "repository",
        {
            "id": 1,
            "repo_type": "model",
            "namespace": "migration-owner",
            "name": "preserved-repository",
            "full_id": "migration-owner/preserved-repository",
            "lakefs_repo": None,
            "private": False,
            "owner_id": 1,
            "owner": 1,
            "quota_bytes": 500000,
            "used_bytes": 505,
            "lfs_threshold_bytes": 600,
            "lfs_keep_versions": 7,
            "lfs_suffix_rules": '[".bin"]',
            "downloads": 8,
            "likes_count": 9,
            "created_at": FIXED_TIMESTAMP,
        },
    ),
    (
        "file",
        {
            "id": 1,
            "repo_full_id": "migration-owner/preserved-repository",
            "repository_id": 1,
            "path_in_repo": "weights.bin",
            "size": 700,
            "sha256": "history-file-sha256",
            "lfs": True,
            "is_deleted": False,
            "owner_id": 1,
            "owner": 1,
            "created_at": FIXED_TIMESTAMP,
            "updated_at": FIXED_TIMESTAMP,
        },
    ),
    (
        "stagingupload",
        {
            "id": 1,
            "repo_full_id": "migration-owner/preserved-repository",
            "repository_id": 1,
            "repo_type": "model",
            "revision": "main",
            "path_in_repo": "pending.bin",
            "sha256": "history-upload-sha256",
            "size": 701,
            "upload_id": "history-upload",
            "storage_key": "history/storage-key",
            "lfs": True,
            "uploader_id": 1,
            "uploader": 1,
            "created_at": FIXED_TIMESTAMP,
        },
    ),
    (
        "commit",
        {
            "id": 1,
            "commit_id": "history-commit-id",
            "repo_full_id": "migration-owner/preserved-repository",
            "repository_id": 1,
            "repo_type": "model",
            "branch": "main",
            "user_id": 1,
            "author_id": 1,
            "author": 1,
            "owner_id": 1,
            "owner": 1,
            "username": "migration-owner",
            "message": "preserve this commit",
            "description": "historical commit",
            "created_at": FIXED_TIMESTAMP,
        },
    ),
    (
        "lfsobjecthistory",
        {
            "id": 1,
            "repo_full_id": "migration-owner/preserved-repository",
            "repository_id": 1,
            "path_in_repo": "weights.bin",
            "sha256": "history-file-sha256",
            "size": 700,
            "commit_id": "history-commit-id",
            "file_id": 1,
            "file": 1,
            "created_at": FIXED_TIMESTAMP,
        },
    ),
    (
        "userorganization",
        {
            "id": 1,
            "user_id": 1,
            "user": 1,
            "organization_id": 20,
            "organization": 20,
            "role": "admin",
            "created_at": FIXED_TIMESTAMP,
        },
    ),
    (
        "emailverification",
        {
            "id": 1,
            "user_id": 1,
            "user": 1,
            "token": "history-email-token",
            "expires_at": FIXED_TIMESTAMP,
            "created_at": FIXED_TIMESTAMP,
        },
    ),
    (
        "session",
        {
            "id": 1,
            "session_id": "history-session",
            "user_id": 1,
            "user": 1,
            "secret": "history-session-secret",
            "expires_at": FIXED_TIMESTAMP,
            "created_at": FIXED_TIMESTAMP,
        },
    ),
    (
        "token",
        {
            "id": 1,
            "user_id": 1,
            "user": 1,
            "token_hash": "history-token-hash",
            "name": "history-token",
            "last_used": FIXED_TIMESTAMP,
            "created_at": FIXED_TIMESTAMP,
        },
    ),
    (
        "sshkey",
        {
            "id": 1,
            "user_id": 1,
            "user": 1,
            "key_type": "ssh-ed25519",
            "public_key": "ssh-ed25519 AAAAhistory",
            "fingerprint": "history-fingerprint",
            "title": "history-key",
            "last_used": FIXED_TIMESTAMP,
            "created_at": FIXED_TIMESTAMP,
        },
    ),
    (
        "invitation",
        {
            "id": 1,
            "token": "history-invitation",
            "action": "join_org",
            "parameters": '{"role":"admin"}',
            "created_by": 1,
            "created_by_id": 1,
            "expires_at": FIXED_TIMESTAMP,
            "max_usage": 4,
            "usage_count": 1,
            "used_at": None,
            "used_by": None,
            "used_by_id": None,
            "created_at": FIXED_TIMESTAMP,
        },
    ),
    (
        "repositorylike",
        {
            "id": 1,
            "repository_id": 1,
            "repository": 1,
            "user_id": 1,
            "user": 1,
            "created_at": FIXED_TIMESTAMP,
        },
    ),
    (
        "downloadsession",
        {
            "id": 1,
            "repository_id": 1,
            "repository": 1,
            "user_id": 1,
            "user": 1,
            "session_id": "history-download-session",
            "time_bucket": 100,
            "file_count": 2,
            "first_file": "weights.bin",
            "first_download_at": FIXED_TIMESTAMP,
            "last_download_at": FIXED_TIMESTAMP,
        },
    ),
    (
        "dailyrepostats",
        {
            "id": 1,
            "repository_id": 1,
            "repository": 1,
            "date": "2025-01-02",
            "download_sessions": 3,
            "authenticated_downloads": 2,
            "anonymous_downloads": 1,
            "total_files": 4,
            "created_at": FIXED_TIMESTAMP,
        },
    ),
    (
        "fallbacksource",
        {
            "id": 1,
            "namespace": "",
            "url": "https://history.example.test",
            "token": "history-source-token",
            "priority": 10,
            "name": "history-source",
            "source_type": "kohakuhub",
            "enabled": True,
            "created_at": FIXED_TIMESTAMP,
            "updated_at": FIXED_TIMESTAMP,
        },
    ),
    (
        "userexternaltoken",
        {
            "id": 1,
            "user_id": 1,
            "user": 1,
            "url": "https://history.example.test",
            "encrypted_token": "history-encrypted-token",
            "created_at": FIXED_TIMESTAMP,
            "updated_at": FIXED_TIMESTAMP,
        },
    ),
    (
        "confirmationtoken",
        {
            "id": 1,
            "token": "history-confirmation-token",
            "action_type": "history-action",
            "action_data": '{"marker":"history"}',
            "created_at": FIXED_TIMESTAMP,
            "expires_at": FIXED_TIMESTAMP,
        },
    ),
)


def seed_database(database: MatrixDatabase) -> tuple[str, ...]:
    """Insert one marked row into every released table that exists."""

    connection = _connect(database)
    inserted = []
    try:
        for table, values in _SEED_ROWS:
            if not _table_exists(connection, database, table):
                continue
            if (
                table == "user"
                and values.get("id") == 20
                and _table_exists(connection, database, "organization")
            ):
                continue
            _insert_row(connection, database, table, values)
            inserted.append(table)
        if database.backend == "postgres":
            # Seed rows use stable explicit IDs so snapshots can compare
            # releases.  Advance serial/identity sequences before archived
            # migrations insert generated rows (notably Organization -> User
            # in migration 008).
            for table in dict.fromkeys(inserted):
                sequence = connection.execute(
                    "SELECT pg_get_serial_sequence(%s, %s)",
                    (table, "id"),
                ).fetchone()[0]
                if sequence is None:
                    continue
                maximum = connection.execute(
                    f"SELECT MAX(id) FROM {_quote_identifier(table)}"
                ).fetchone()[0]
                if maximum is not None:
                    connection.execute(
                        "SELECT setval(%s, %s, true)",
                        (sequence, maximum),
                    )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
    return tuple(inserted)


def _complete_main_016_backfill(database: MatrixDatabase) -> None:
    """Make the unnumbered main checkpoint represent a completed 016 run."""

    from kohakuhub.utils.lakefs import lakefs_repo_name

    connection = _connect(database)
    try:
        rows = connection.execute(
            "SELECT id, repo_type, namespace, name "
            "FROM repository WHERE lakefs_repo IS NULL"
        ).fetchall()
        placeholder = "?" if database.backend == "sqlite" else "%s"
        for repository_id, repo_type, namespace, name in rows:
            connection.execute(
                f"UPDATE repository SET lakefs_repo = {placeholder} "
                f"WHERE id = {placeholder}",
                (
                    lakefs_repo_name(repo_type, f"{namespace}/{name}"),
                    repository_id,
                ),
            )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


_SNAPSHOT_FIELDS = {
    "user": (
        ("username", ("username",), False),
        ("is_org", ("is_org",), False),
        ("email", ("email",), None),
        ("private_used_bytes", ("private_used_bytes",), None),
        ("public_used_bytes", ("public_used_bytes",), None),
    ),
    "organization": (
        ("username", ("name", "username"), "migration-org"),
        ("is_org", ("is_org",), True),
        ("email", ("email",), None),
        ("private_used_bytes", ("private_used_bytes",), None),
        ("public_used_bytes", ("public_used_bytes",), None),
    ),
    "repository": (
        ("full_id", ("full_id",), None),
        ("repo_type", ("repo_type",), None),
        ("private", ("private",), False),
        ("quota_bytes", ("quota_bytes",), None),
        ("used_bytes", ("used_bytes",), None),
    ),
    "file": (
        ("path_in_repo", ("path_in_repo",), None),
        ("size", ("size",), None),
        ("sha256", ("sha256",), None),
        ("lfs", ("lfs",), False),
        ("is_deleted", ("is_deleted",), False),
    ),
    "stagingupload": (
        ("upload_id", ("upload_id",), None),
        ("path_in_repo", ("path_in_repo",), None),
        ("size", ("size",), None),
        ("sha256", ("sha256",), None),
        ("lfs", ("lfs",), False),
    ),
    "commit": (
        ("commit_id", ("commit_id",), None),
        ("message", ("message",), None),
        ("description", ("description",), None),
        ("branch", ("branch",), None),
        ("username", ("username",), None),
    ),
    "lfsobjecthistory": (
        ("sha256", ("sha256",), None),
        ("path_in_repo", ("path_in_repo",), None),
        ("size", ("size",), None),
        ("commit_id", ("commit_id",), None),
    ),
    "userorganization": (("role", ("role",), None),),
    "emailverification": (("token", ("token",), None),),
    "session": (
        ("session_id", ("session_id",), None),
        ("secret", ("secret",), None),
    ),
    "token": (
        ("token_hash", ("token_hash",), None),
        ("name", ("name",), None),
    ),
    "sshkey": (
        ("fingerprint", ("fingerprint",), None),
        ("title", ("title",), None),
    ),
    "invitation": (
        ("token", ("token",), None),
        ("action", ("action",), None),
        ("parameters", ("parameters",), None),
        ("max_usage", ("max_usage",), None),
        ("usage_count", ("usage_count",), None),
    ),
    "repositorylike": (("id", ("id",), None),),
    "downloadsession": (
        ("session_id", ("session_id",), None),
        ("file_count", ("file_count",), None),
        ("first_file", ("first_file",), None),
    ),
    "dailyrepostats": (
        ("date", ("date",), None),
        ("download_sessions", ("download_sessions",), None),
        ("total_files", ("total_files",), None),
    ),
    "fallbacksource": (
        ("url", ("url",), None),
        ("name", ("name",), None),
        ("priority", ("priority",), None),
        ("source_type", ("source_type",), None),
    ),
    "userexternaltoken": (
        ("url", ("url",), None),
        ("encrypted_token", ("encrypted_token",), None),
    ),
    "confirmationtoken": (
        ("token", ("token",), None),
        ("action_type", ("action_type",), None),
        ("action_data", ("action_data",), None),
    ),
}


def _resolve_column(
    columns: tuple[str, ...], candidates: tuple[str, ...]
) -> str | None:
    return next((candidate for candidate in candidates if candidate in columns), None)


def _normalize_snapshot_value(value: Any, field: tuple[str, ...]) -> Any:
    if field[0] in ("is_org", "private", "lfs", "is_deleted"):
        return bool(value)
    return value


def snapshot_database(
    database: MatrixDatabase,
) -> dict[str, tuple[tuple[Any, ...], ...]]:
    """Read semantic marked rows, tolerating historical column renames."""

    connection = _connect(database)
    snapshot: dict[str, tuple[tuple[Any, ...], ...]] = {}
    try:
        for table, fields in _SNAPSHOT_FIELDS.items():
            if not _table_exists(connection, database, table):
                continue
            columns = _table_columns(connection, database, table)
            resolved = tuple(_resolve_column(columns, field[1]) for field in fields)
            select_columns = tuple(column for column in resolved if column is not None)
            statement = (
                f"SELECT {', '.join(_quote_identifier(column) for column in select_columns)} "
                f"FROM {_quote_identifier(table)} ORDER BY 1"
            )
            if not select_columns:
                continue
            rows = _execute(connection, database, statement).fetchall()
            if not rows:
                continue
            selected_indexes = {
                index: select_columns.index(column)
                for index, column in enumerate(resolved)
                if column is not None
            }
            snapshot[table] = tuple(
                tuple(
                    _normalize_snapshot_value(
                        row[selected_indexes[index]]
                        if index in selected_indexes
                        else default,
                        (canonical,),
                    )
                    for index, (canonical, _candidates, default) in enumerate(fields)
                )
                for row in rows
            )

        if "organization" in snapshot:
            organization_rows = snapshot.pop("organization")
            snapshot["user"] = tuple(
                sorted(set(snapshot.get("user", ())) | set(organization_rows))
            )
        return snapshot
    finally:
        connection.close()


def application_table_columns(database: MatrixDatabase) -> dict[str, tuple[str, ...]]:
    """Return the post-run application table/column shape for assertions."""

    connection = _connect(database)
    try:
        tables = set()
        if database.backend == "sqlite":
            rows = connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
            tables = {row[0] for row in rows}
        else:
            rows = connection.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = current_schema()"
            ).fetchall()
            tables = {row[0] for row in rows}
        return {
            table: _table_columns(connection, database, table)
            for table in sorted(tables)
            if not table.startswith("sqlite_")
        }
    finally:
        connection.close()


def ledger_rows(database: MatrixDatabase) -> tuple[tuple[Any, ...], ...]:
    """Return the 017 worker ledger rows, or an empty tuple for SQLite."""

    connection = _connect(database)
    try:
        if not _table_exists(connection, database, "khub_schema_migrations"):
            return ()
        return tuple(
            _execute(
                connection,
                database,
                "SELECT migration_name, version, checksum "
                "FROM khub_schema_migrations ORDER BY migration_name",
            ).fetchall()
        )
    finally:
        connection.close()


def invitation_created_by_nullable(database: MatrixDatabase) -> bool | None:
    """Return the actual nullability of the 012 column, if the table exists."""

    connection = _connect(database)
    try:
        if not _table_exists(connection, database, "invitation"):
            return None
        if database.backend == "sqlite":
            rows = connection.execute("PRAGMA table_info(invitation)").fetchall()
            for _cid, name, _type, not_null, _default, _pk in rows:
                if name == "created_by_id":
                    return not bool(not_null)
            return None
        row = connection.execute(
            "SELECT is_nullable FROM information_schema.columns "
            "WHERE table_schema = current_schema() "
            "AND table_name = 'invitation' AND column_name = 'created_by_id'"
        ).fetchone()
        return row is not None and row[0] == "YES"
    finally:
        connection.close()


def postgres_column_types(
    database: MatrixDatabase,
    columns: tuple[tuple[str, str], ...],
) -> dict[tuple[str, str], str | None]:
    """Return PostgreSQL information-schema types for selected columns."""

    if database.backend != "postgres":
        return {column: None for column in columns}
    connection = _connect(database)
    try:
        result = {}
        for table, column in columns:
            row = connection.execute(
                "SELECT data_type FROM information_schema.columns "
                "WHERE table_schema = current_schema() "
                "AND table_name = %s AND column_name = %s",
                (table, column),
            ).fetchone()
            result[(table, column)] = row[0] if row else None
        return result
    finally:
        connection.close()


def repository_lakefs_values(
    database: MatrixDatabase,
) -> tuple[tuple[Any, ...], ...]:
    """Return repository IDs and persisted LakeFS IDs after migration 016."""

    connection = _connect(database)
    try:
        if not _table_exists(connection, database, "repository"):
            return ()
        columns = _table_columns(connection, database, "repository")
        if "lakefs_repo" not in columns:
            return ()
        return tuple(
            connection.execute(
                "SELECT full_id, lakefs_repo FROM repository ORDER BY id"
            ).fetchall()
        )
    finally:
        connection.close()


def sqlite_foreign_key_violations(
    database: MatrixDatabase,
) -> tuple[tuple[Any, ...], ...]:
    """Return SQLite's complete FK validation result, or empty for PostgreSQL."""

    if database.backend != "sqlite":
        return ()
    connection = _connect(database)
    try:
        return tuple(connection.execute("PRAGMA foreign_key_check").fetchall())
    finally:
        connection.close()


def membership_relationships(
    database: MatrixDatabase,
) -> tuple[tuple[Any, ...], ...]:
    """Return membership IDs and the merged user's organization flag."""

    connection = _connect(database)
    try:
        if database.backend == "sqlite":
            query = (
                "SELECT uo.user_id, uo.organization_id, u.is_org "
                "FROM userorganization AS uo "
                'JOIN "user" AS u ON u.id = uo.organization_id '
                "ORDER BY uo.id"
            )
        else:
            query = (
                "SELECT uo.user_id, uo.organization_id, u.is_org "
                "FROM userorganization AS uo "
                'JOIN "user" AS u ON u.id = uo.organization_id '
                "ORDER BY uo.id"
            )
        return tuple(connection.execute(query).fetchall())
    finally:
        connection.close()


def membership_foreign_key_points_to_user(database: MatrixDatabase) -> bool:
    """Check that userorganization.organization_id targets the merged user."""

    connection = _connect(database)
    try:
        if database.backend == "sqlite":
            rows = connection.execute(
                "PRAGMA foreign_key_list(userorganization)"
            ).fetchall()
            return any(row[2] == "user" and row[3] == "organization_id" for row in rows)
        row = connection.execute(
            """
            SELECT 1
            FROM information_schema.referential_constraints AS rc
            JOIN information_schema.table_constraints AS tc
              ON tc.constraint_schema = rc.constraint_schema
             AND tc.constraint_name = rc.constraint_name
            JOIN information_schema.key_column_usage AS kcu
              ON kcu.constraint_schema = tc.constraint_schema
             AND kcu.constraint_name = tc.constraint_name
            JOIN information_schema.constraint_column_usage AS ccu
              ON ccu.constraint_schema = tc.constraint_schema
             AND ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_schema = current_schema()
              AND tc.table_name = 'userorganization'
              AND tc.constraint_type = 'FOREIGN KEY'
              AND kcu.column_name = 'organization_id'
              AND ccu.table_name = 'user'
              AND ccu.column_name = 'id'
            """
        ).fetchone()
        return row is not None
    finally:
        connection.close()


def foreign_key_contract_missing(
    database: MatrixDatabase,
) -> tuple[tuple[str, str, str, str], ...]:
    """Return relation contracts absent from the upgraded database."""

    connection = _connect(database)
    try:
        if database.backend == "sqlite":
            actual = set()
            for table, _column, _parent, _on_delete in FOREIGN_KEY_CONTRACT:
                pragma_table = _quote_identifier(table)
                for row in connection.execute(
                    f"PRAGMA foreign_key_list({pragma_table})"
                ).fetchall():
                    actual.add((table, row[3], row[2], row[6].upper()))
        else:
            actual = set(
                connection.execute(
                    """
                    SELECT source.relname, source_attribute.attname,
                           target.relname,
                           CASE foreign_key.confdeltype
                               WHEN 'a' THEN 'NO ACTION'
                               WHEN 'c' THEN 'CASCADE'
                               WHEN 'd' THEN 'SET DEFAULT'
                               WHEN 'n' THEN 'SET NULL'
                               WHEN 'r' THEN 'RESTRICT'
                           END
                    FROM pg_constraint AS foreign_key
                    JOIN pg_class AS source
                      ON source.oid = foreign_key.conrelid
                    JOIN pg_namespace AS source_namespace
                      ON source_namespace.oid = source.relnamespace
                    JOIN pg_class AS target
                      ON target.oid = foreign_key.confrelid
                    JOIN pg_attribute AS source_attribute
                      ON source_attribute.attrelid = source.oid
                     AND source_attribute.attnum = foreign_key.conkey[1]
                    JOIN pg_attribute AS target_attribute
                      ON target_attribute.attrelid = target.oid
                     AND target_attribute.attnum = foreign_key.confkey[1]
                    WHERE foreign_key.contype = 'f'
                      AND source_namespace.nspname = current_schema()
                      AND target_attribute.attname = 'id'
                    """
                ).fetchall()
            )
        return tuple(
            contract for contract in FOREIGN_KEY_CONTRACT if contract not in actual
        )
    finally:
        connection.close()
