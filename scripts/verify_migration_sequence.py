#!/usr/bin/env python3
"""Statically verify the numbered database migration stream.

This is a deployment/release check, not an application unit test.  It does
not import migration modules or connect to a database, so adding migration
018 or any later migration only extends the checked stream.
"""

from __future__ import annotations

import ast
import re
import sys
from dataclasses import dataclass
from pathlib import Path


MIGRATIONS_DIR = Path(__file__).with_name("db_migrations")
CURRENT_MIGRATION = 17
MIGRATION_FILENAME = re.compile(r"^(?P<number>[0-9]+)_.+\.py$")


@dataclass(frozen=True)
class MigrationFile:
    number: int
    path: Path


def discover_migrations(directory: Path = MIGRATIONS_DIR) -> list[MigrationFile]:
    """Return numbered migration files in numeric order."""

    migrations = []
    for path in directory.glob("*.py"):
        match = MIGRATION_FILENAME.fullmatch(path.name)
        if match is None:
            continue
        migrations.append(MigrationFile(int(match.group("number")), path))
    return sorted(migrations, key=lambda item: (item.number, item.path.name))


def _target_names(node: ast.Assign | ast.AnnAssign) -> list[str]:
    targets = node.targets if isinstance(node, ast.Assign) else [node.target]
    return [target.id for target in targets if isinstance(target, ast.Name)]


def declared_migration_number(path: Path) -> int | None:
    """Read a migration's literal ``MIGRATION_NUMBER`` without importing it."""

    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in tree.body:
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        if "MIGRATION_NUMBER" not in _target_names(node):
            continue
        try:
            value = node.value
            declared = ast.literal_eval(value)
        except (ValueError, TypeError):
            return None
        return declared if type(declared) is int else None
    return None


def validation_errors(directory: Path = MIGRATIONS_DIR) -> list[str]:
    """Return release-check failures for the numbered migration stream."""

    migrations = discover_migrations(directory)
    errors: list[str] = []
    if not migrations:
        return [f"no numbered migrations found in {directory}"]

    by_number: dict[int, list[MigrationFile]] = {}
    for migration in migrations:
        by_number.setdefault(migration.number, []).append(migration)

    for number, duplicates in sorted(by_number.items()):
        if len(duplicates) > 1:
            names = ", ".join(item.path.name for item in duplicates)
            errors.append(f"duplicate migration number {number}: {names}")

    numbers = sorted(by_number)
    highest = numbers[-1]
    if numbers[0] < 1:
        errors.append(f"migration numbers must start at 1, found {numbers[0]}")
    missing = sorted(set(range(1, highest + 1)) - set(numbers))
    if missing:
        errors.append(
            "missing migration numbers before the current highest migration: "
            + ", ".join(str(number) for number in missing)
        )
    if highest < CURRENT_MIGRATION:
        errors.append(
            f"migration stream ends at {highest}, expected at least {CURRENT_MIGRATION}"
        )

    for migration in migrations:
        try:
            declared = declared_migration_number(migration.path)
        except (OSError, SyntaxError, UnicodeError) as exc:
            errors.append(f"could not parse {migration.path.name}: {exc}")
            continue
        if declared is None:
            errors.append(f"{migration.path.name} has no literal MIGRATION_NUMBER")
        elif declared != migration.number:
            errors.append(
                f"{migration.path.name} declares MIGRATION_NUMBER={declared}, "
                f"expected {migration.number}"
            )

    return errors


def main() -> int:
    errors = validation_errors()
    if errors:
        print("Migration sequence check failed:")
        for error in errors:
            print(f"- {error}")
        return 1

    migrations = discover_migrations()
    print(
        "Migration sequence OK: "
        f"{len(migrations)} numbered migrations through {migrations[-1].number:03d}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
