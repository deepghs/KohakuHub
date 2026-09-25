# Background Tasks

KohakuHub runs durable background work through a small queue stored in the
application database (`background_task` table) and a separate worker process,
`khub-worker` (`python -m kohakuhub.worker`). There is no broker. Valkey is not
involved, and losing it never loses a task.

- Code: `src/kohakuhub/tasks.py` (queue) and `src/kohakuhub/worker.py` (runtime)
- Admin: **Background Tasks** page in the admin panel, backed by `/admin/api/tasks`
- Design and rationale: issue #104

## Writing a task

Register an async handler with a stable kind name, then enqueue it:

```python
from kohakuhub.db import db
from kohakuhub.tasks import PermanentTaskError, enqueue, task


@task("repo.recalculate_storage", max_attempts=5, timeout=600)
async def recalculate_storage(payload: dict) -> None:
    repo = Repository.get_or_none(Repository.id == payload["repo_id"])
    if repo is None:
        raise PermanentTaskError("repository no longer exists")  # no retries
    await update_repository_storage(repo)


with db.atomic():
    commit_row = Commit.create(...)
    # Inserted in the same transaction: rolled back together with the commit.
    enqueue(
        "repo.recalculate_storage",
        {"repo_id": repo.id},
        dedupe_key=f"repo-storage:{repo.id}",
    )
```

The worker only claims kinds it has registered, so import every module that
defines handlers in `kohakuhub/worker.py`. The API process registers the kinds
it enqueues by importing the same module.

Rules:

- **Handlers must be idempotent.** Delivery is at-least-once. If a worker dies
  mid-task, the task runs again once its lease expires.
- **Payloads carry IDs and small parameters only.** Never put credentials,
  presigned URLs, or file content in a payload. Handlers re-read current state
  from the database.
- Raise `PermanentTaskError` for failures that retrying cannot fix. Any other
  exception is retried with exponential backoff until `max_attempts`.
- Handlers are async functions and run on the worker's event loop. Offload
  heavy blocking work (hashing, large boto3 calls) the same way API handlers
  do.
- **Never `await` inside `db.atomic()`.** Handlers share the loop thread's
  database connection with the worker's own bookkeeping, just as hub-api
  request handlers share it with each other.
- Keep handler modules free of `kohakuhub.api` router imports: some routers
  touch the schema at import time, and the worker must never create tables
  before hub-api has run migrations.

## Semantics

| Feature | How it works |
| --- | --- |
| Claim | `SELECT ... FOR UPDATE SKIP LOCKED` on PostgreSQL. On SQLite, a compare-and-set update. Only kinds registered in the worker process are claimed. |
| Lease | The claim sets `locked_until = now + lease_seconds`. The worker renews it every `lease_seconds / 3`. An expired lease makes the task claimable again. |
| Stale workers | Renewals and outcome writes only apply while the worker still owns the lease. A slow worker that lost its lease cannot overwrite the new owner's state. |
| Retry | `run_after = now + 5s * 2^(attempt-1)` (capped at 1 h, plus up to 25% jitter). |
| Timeout | Per-kind `timeout` seconds; a timeout counts as a retryable failure. |
| Dedupe | `dedupe_key` is unique while the task is pending. Claiming the task releases the key, so work arriving while it runs can enqueue a fresh task. |
| Delay | `enqueue(..., run_after=datetime)`. Timestamps are naive UTC (`kohakuhub.db.utcnow()`). |
| Periodic | `@task(..., every=timedelta(...))`. Claiming an occurrence inserts the next one in the same transaction, so exactly one is pending at any time. Workers re-create a missing occurrence (for example, one discarded from the admin panel) within a minute. |
| Retention | The built-in periodic `tasks.cleanup` task deletes finished rows after the configured retention. |
| Shutdown | SIGTERM stops claiming and drains running tasks for `shutdown_grace_seconds`. It then cancels the rest and hands them back to the queue without spending an attempt. The compose service sets `stop_grace_period: 45s` so Docker does not kill the worker mid-drain. |

## Admin panel

**Background Tasks** in the admin panel has two tabs. The status cards and the health chip above them stay visible on both.

- **Overview** is a dashboard for a selectable window (15m, 1h, 6h, 24h, 7d), served by `GET /admin/api/tasks/stats?window=`:
  - a health verdict with the reasons behind it;
  - KPI tiles: finished and throughput, success rate, due backlog and oldest wait, retrying, running and stuck, p50/p95 duration;
  - an activity chart of succeeded and failed tasks per bucket;
  - health by kind, with a timeline strip per kind;
  - errors grouped by exception class, counting final failures and pending retries separately.

  Clicking a kind, an error's kind chip, a status card or a KPI tile opens **Tasks** already filtered.
- **Tasks** is the filterable task list with details, retry and discard.

The health verdict uses these rules:

| Signal | Degraded | Unhealthy |
| --- | --- | --- |
| Oldest due task waiting | ≥ 60 s | ≥ 5 min (the queue is not draining) |
| Failure rate in the window (≥ 5 finished) | ≥ 10 % | ≥ 50 % |
| Failures with fewer than 5 finished | any | — |
| Tasks waiting to retry | any | — |
| Running tasks with an expired lease | — | any (no worker reclaimed them) |

A queued task whose kind no running worker registers never drains, so the backlog rule flags it too.

## Running the worker

Local development (start `make backend` first; it runs migrations and writes
the LakeFS credentials the worker reuses):

```bash
make worker
```

Docker Compose runs it as the `khub-worker` service from the same image as
`hub-api`. See `docker-compose.example.yml`. Tune it with the
`KOHAKU_HUB_WORKER_*` variables in the
[configuration reference](../reference/config.md#background-task-worker-settings).
If no worker is running, tasks accumulate as `queued`; the API does not depend
on them.
