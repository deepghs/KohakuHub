"""End-to-end check of the real ``python -m kohakuhub.worker`` entrypoint."""

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from kohakuhub import tasks
from kohakuhub.db import BackgroundTask

SRC_DIR = Path(__file__).resolve().parents[2] / "src"


def _wait_for(predicate, timeout=30.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.2)
    return False


def test_worker_process_consumes_tasks_and_exits_on_sigterm(prepared_backend_test_state):
    BackgroundTask.delete().execute()
    env = os.environ | {
        "PYTHONPATH": os.pathsep.join(filter(None, [str(SRC_DIR), os.environ.get("PYTHONPATH")])),
        "KOHAKU_HUB_WORKER_POLL_INTERVAL_SECONDS": "0.2",
        "KOHAKU_HUB_LOG_LEVEL": "INFO",
    }
    process = subprocess.Popen(
        [sys.executable, "-m", "kohakuhub.worker"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        # On startup the worker schedules and runs the built-in cleanup task.
        assert _wait_for(
            lambda: BackgroundTask.select()
            .where(
                (BackgroundTask.kind == tasks.CLEANUP_KIND)
                & (BackgroundTask.status == tasks.SUCCEEDED)
            )
            .exists()
        ), "worker did not run the cleanup task"
        process.send_signal(signal.SIGTERM)
        output, _ = process.communicate(timeout=30)
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate()

    assert process.returncode == 0, output
    assert "stopped" in output
    # The next periodic occurrence is left pending for the next worker.
    pending = BackgroundTask.get(BackgroundTask.status == tasks.QUEUED)
    assert pending.dedupe_key == tasks.periodic_key(tasks.CLEANUP_KIND)
    BackgroundTask.delete().execute()
