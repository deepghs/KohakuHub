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
