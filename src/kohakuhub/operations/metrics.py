"""Low-cardinality metrics for the durable operation runtime."""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram


# Keep KHub metrics in an application-owned registry.  The API test harness
# reloads backend modules in one interpreter, and registering collectors in
# prometheus_client's process-global registry would make the second import
# fail with DuplicateTimeseries.  The worker exposes this registry explicitly.
METRICS_REGISTRY = CollectorRegistry(auto_describe=True)


OPERATION_DELIVERIES = Counter(
    "khub_operation_deliveries_total",
    "Operation step deliveries observed by the worker",
    ("kind", "state"),
    registry=METRICS_REGISTRY,
)
OPERATION_FAILURES = Counter(
    "khub_operation_failures_total",
    "Operation step failures classified by the domain executor",
    ("kind", "code"),
    registry=METRICS_REGISTRY,
)
OPERATION_STALLED = Counter(
    "khub_operation_stalled_total",
    "Operation steps recovered from stalled delivery",
    ("kind",),
    registry=METRICS_REGISTRY,
)
OPERATION_JOBS_REPAIRED = Counter(
    "khub_operation_jobs_repaired_total",
    "Missing operation jobs recreated by reconciliation",
    ("kind",),
    registry=METRICS_REGISTRY,
)
OPERATION_UNCERTAIN = Gauge(
    "khub_operation_uncertain",
    "Current number of operations waiting for external observation",
    registry=METRICS_REGISTRY,
)
RECONCILIATION_LAG_SECONDS = Gauge(
    "khub_reconciliation_lag_seconds",
    "Age of the oldest recoverable operation intent",
    registry=METRICS_REGISTRY,
)
RECONCILIATION_RUNS = Counter(
    "khub_reconciliation_runs_total",
    "Reconciliation scans completed",
    ("result",),
    registry=METRICS_REGISTRY,
)
RECONCILIATION_DURATION = Histogram(
    "khub_reconciliation_duration_seconds",
    "Duration of one reconciliation scan",
    registry=METRICS_REGISTRY,
)
QUEUE_DEPTH = Gauge(
    "khub_queue_depth",
    "Durable Procrastinate jobs by queue and delivery status",
    ("queue", "status"),
    registry=METRICS_REGISTRY,
)
QUEUE_OLDEST_AGE_SECONDS = Gauge(
    "khub_queue_oldest_age_seconds",
    "Age of the oldest ready or running job in a queue",
    ("queue",),
    registry=METRICS_REGISTRY,
)
OPERATION_BACKLOG = Gauge(
    "khub_operation_backlog",
    "Non-terminal operations by state",
    ("state",),
    registry=METRICS_REGISTRY,
)
RECONCILIATION_BACKLOG = Gauge(
    "khub_reconciliation_backlog",
    "Operations currently requiring external observation",
    registry=METRICS_REGISTRY,
)
OPERATION_RETENTION_PRUNED = Counter(
    "khub_operation_retention_pruned_total",
    "Terminal operation and durable job records removed by retention",
    registry=METRICS_REGISTRY,
)
OPERATION_RETENTION_BACKLOG = Gauge(
    "khub_operation_retention_backlog",
    "Terminal operation records eligible for retention",
    registry=METRICS_REGISTRY,
)
WORKER_EVENT_LOOP_LAG_SECONDS = Gauge(
    "khub_worker_event_loop_lag_seconds",
    "Observed worker event-loop scheduling lag",
    registry=METRICS_REGISTRY,
)
WORKER_RSS_BYTES = Gauge(
    "khub_worker_rss_bytes",
    "Resident set size of the worker process",
    registry=METRICS_REGISTRY,
)
DB_POOL_SIZE = Gauge(
    "khub_worker_db_pool_size",
    "Current psycopg pool size by worker lane",
    ("lane",),
    registry=METRICS_REGISTRY,
)
DB_POOL_WAITING = Gauge(
    "khub_worker_db_pool_waiting",
    "Requests waiting for a psycopg connection by worker lane",
    ("lane",),
    registry=METRICS_REGISTRY,
)
