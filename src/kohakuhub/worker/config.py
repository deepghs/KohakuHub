"""Configuration and queue policy for the KHub worker service."""

from __future__ import annotations

import os
from dataclasses import dataclass


CONTROL_QUEUE = "control-v1"
SYNC_QUEUE = "sync-v1"
BULK_QUEUE = "bulk-v1"
CLEANUP_QUEUE = "cleanup-v1"


@dataclass(frozen=True)
class WorkerLane:
    """A supervised worker lane with an explicit queue boundary."""

    name: str
    queues: tuple[str, ...]
    concurrency: int


CONTROL_LANE = WorkerLane("control", (CONTROL_QUEUE,), 1)
WORK_LANE = WorkerLane(
    "work", (SYNC_QUEUE, BULK_QUEUE, CLEANUP_QUEUE), 3
)


@dataclass(frozen=True)
class WorkerSettings:
    """Validated settings shared by both supervised worker lanes."""

    database_url: str
    pool_min_size: int = 1
    pool_max_size: int = 8
    graceful_shutdown_seconds: float = 30.0
    polling_interval_seconds: float = 5.0
    heartbeat_interval_seconds: float = 10.0
    stalled_worker_timeout_seconds: float = 30.0

    @classmethod
    def from_env(cls) -> "WorkerSettings":
        backend = os.getenv("KOHAKU_HUB_DB_BACKEND", "sqlite").lower()
        if backend != "postgres":
            raise ValueError(
                "khub-worker requires KOHAKU_HUB_DB_BACKEND=postgres; "
                "SQLite cannot provide durable distributed worker semantics"
            )

        database_url = os.getenv("KOHAKU_HUB_DATABASE_URL", "").strip()
        if not database_url.startswith(("postgresql://", "postgres://")):
            raise ValueError(
                "khub-worker requires a PostgreSQL KOHAKU_HUB_DATABASE_URL"
            )

        settings = cls(
            database_url=database_url,
            pool_min_size=_positive_int("KOHAKU_HUB_WORKER_POOL_MIN", 1),
            pool_max_size=_positive_int("KOHAKU_HUB_WORKER_POOL_MAX", 8),
            graceful_shutdown_seconds=_positive_float(
                "KOHAKU_HUB_WORKER_SHUTDOWN_SECONDS", 30.0
            ),
            polling_interval_seconds=_positive_float(
                "KOHAKU_HUB_WORKER_POLL_SECONDS", 5.0
            ),
            heartbeat_interval_seconds=_positive_float(
                "KOHAKU_HUB_WORKER_HEARTBEAT_SECONDS", 10.0
            ),
            stalled_worker_timeout_seconds=_positive_float(
                "KOHAKU_HUB_WORKER_STALLED_SECONDS", 30.0
            ),
        )
        if settings.pool_max_size < settings.pool_min_size:
            raise ValueError("worker pool max size must be >= min size")
        return settings


def _positive_int(name: str, default: int) -> int:
    value = int(os.getenv(name, str(default)))
    if value < 1:
        raise ValueError(f"{name} must be positive")
    return value


def _positive_float(name: str, default: float) -> float:
    value = float(os.getenv(name, str(default)))
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value
