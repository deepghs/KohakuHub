#!/usr/bin/env python3
"""Read-only schema compatibility check for API and worker startup."""

from __future__ import annotations

import sys
from pathlib import Path

import psycopg

SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR.parent / "src"))

from kohakuhub.config import cfg
from kohakuhub.migrations.schema import (
    is_exact_current_schema,
    operation_schema_object_diff,
)


def verify() -> None:
    if cfg.app.db_backend != "postgres":
        raise RuntimeError("worker schema verification requires PostgreSQL")
    with psycopg.connect(cfg.app.database_url) as connection:
        exact, diff = is_exact_current_schema(connection)
        if not exact:
            raise RuntimeError(f"KHub schema mismatch: {diff}")
        object_diff = operation_schema_object_diff(connection)
        if any(object_diff.values()):
            raise RuntimeError(f"operation schema object mismatch: {object_diff}")
        required = (
            "khub_schema_migrations",
            "procrastinate_jobs",
            "procrastinate_workers",
        )
        rows = connection.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = ANY(%s)
            """,
            (list(required),),
        ).fetchall()
        actual = {row[0] for row in rows}
        missing = sorted(set(required) - actual)
        if missing:
            raise RuntimeError(f"missing durable worker tables: {missing}")


if __name__ == "__main__":
    verify()
