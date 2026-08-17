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
    control_pool_max_size: int = 2
    work_pool_max_size: int = 6
    graceful_shutdown_seconds: float = 30.0
    polling_interval_seconds: float = 5.0
    heartbeat_interval_seconds: float = 10.0
    stalled_worker_timeout_seconds: float = 30.0
    operation_retention_hours: int = 168
    operation_retention_batch_size: int = 100
    api_processes: int = 4
    api_pool_max_size: int = 4
    api_fence_max_connections: int = 4
    worker_fence_max_connections: int = 4
    listener_connections: int = 2
    migration_admin_connections: int = 2
    metrics_host: str = "0.0.0.0"
    metrics_port: int = 0

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

        pool_min_size = _positive_int("KOHAKU_HUB_WORKER_POOL_MIN", 1)
        pool_max_size = _positive_int("KOHAKU_HUB_WORKER_POOL_MAX", 8)
        control_pool_max_size = _positive_int(
            "KOHAKU_HUB_WORKER_CONTROL_POOL_MAX",
            max(1, pool_max_size // 4),
        )
        work_pool_max_size = _positive_int(
            "KOHAKU_HUB_WORKER_WORK_POOL_MAX",
            max(1, pool_max_size - control_pool_max_size),
        )
        settings = cls(
            database_url=database_url,
            pool_min_size=pool_min_size,
            pool_max_size=pool_max_size,
            control_pool_max_size=control_pool_max_size,
            work_pool_max_size=work_pool_max_size,
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
            operation_retention_hours=_positive_int(
                "KOHAKU_HUB_OPERATION_RETENTION_HOURS", 168
            ),
            operation_retention_batch_size=_positive_int(
                "KOHAKU_HUB_OPERATION_RETENTION_BATCH", 100
            ),
            api_processes=_positive_int("KOHAKU_HUB_WORKERS", 4),
            api_pool_max_size=_positive_int("KOHAKU_HUB_OPERATION_POOL_MAX", 4),
            api_fence_max_connections=_positive_int(
                "KOHAKU_HUB_API_FENCE_MAX_CONNECTIONS",
                int(os.getenv("KOHAKU_HUB_FENCE_MAX_CONNECTIONS", "4")),
            ),
            worker_fence_max_connections=_positive_int(
                "KOHAKU_HUB_WORKER_FENCE_MAX_CONNECTIONS",
                int(os.getenv("KOHAKU_HUB_FENCE_MAX_CONNECTIONS", "4")),
            ),
            listener_connections=_positive_int(
                "KOHAKU_HUB_WORKER_LISTENER_CONNECTIONS", 2
            ),
            migration_admin_connections=_positive_int(
                "KOHAKU_HUB_MIGRATION_ADMIN_CONNECTIONS", 2
            ),
            metrics_host=os.getenv("KOHAKU_HUB_WORKER_METRICS_HOST", "0.0.0.0"),
            metrics_port=_nonnegative_int("KOHAKU_HUB_WORKER_METRICS_PORT", 0),
        )
        if settings.pool_max_size < settings.pool_min_size:
            raise ValueError("worker pool max size must be >= min size")
        if settings.control_pool_max_size < settings.pool_min_size:
            raise ValueError("control worker pool max size must be >= min size")
        if settings.work_pool_max_size < settings.pool_min_size:
            raise ValueError("work worker pool max size must be >= min size")
        if settings.api_pool_max_size < 1:
            raise ValueError("API operation pool max size must be positive")
        if settings.control_pool_max_size + settings.work_pool_max_size > settings.pool_max_size:
            raise ValueError(
                "control and work worker pool max sizes exceed aggregate pool max size"
            )
        return settings

    @property
    def configured_connection_budget(self) -> int:
        """Upper bound for the whole deployment sharing this PostgreSQL."""

        return (
            self.api_processes * (self.api_pool_max_size + self.api_fence_max_connections)
            + self.control_pool_max_size
            + self.work_pool_max_size
            + self.worker_fence_max_connections
            + self.listener_connections
            + self.migration_admin_connections
        )


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


def _nonnegative_int(name: str, default: int) -> int:
    value = int(os.getenv(name, str(default)))
    if value < 0 or value > 65535:
        raise ValueError(f"{name} must be between 0 and 65535")
    return value
