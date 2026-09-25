"""Tests for the container entrypoint's worker mode (docker/startup.py)."""

import importlib.util
import sys
from pathlib import Path

import pytest

STARTUP_PATH = Path(__file__).resolve().parents[2] / "docker" / "startup.py"


@pytest.fixture
def startup(monkeypatch, tmp_path):
    spec = importlib.util.spec_from_file_location("khub_startup", STARTUP_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "CRED_FILE", tmp_path / "credentials.env")
    for name in ("KOHAKU_HUB_LAKEFS_ACCESS_KEY", "KOHAKU_HUB_LAKEFS_SECRET_KEY"):
        # setenv first so monkeypatch restores the original state even though
        # load_credentials() writes os.environ directly.
        monkeypatch.setenv(name, "placeholder")
        monkeypatch.delenv(name)
    execs = []
    monkeypatch.setattr(module.os, "execvp", lambda file, args: execs.append(args))
    module.execs = execs
    return module


def test_run_worker_waits_for_credentials_then_execs_worker(startup, monkeypatch):
    def hub_api_writes_credentials(_seconds):
        startup.CRED_FILE.write_text(
            "KOHAKU_HUB_LAKEFS_ACCESS_KEY=ak\nKOHAKU_HUB_LAKEFS_SECRET_KEY=sk\n"
        )

    monkeypatch.setattr(startup.time, "sleep", hub_api_writes_credentials)

    startup.run_worker()

    assert startup.os.environ["KOHAKU_HUB_LAKEFS_ACCESS_KEY"] == "ak"
    assert startup.execs == [[sys.executable, "-m", "kohakuhub.worker"]]


def test_run_worker_uses_existing_credentials_env(startup, monkeypatch):
    monkeypatch.setenv("KOHAKU_HUB_LAKEFS_ACCESS_KEY", "from-env")

    startup.run_worker()

    assert not startup.CRED_FILE.exists()
    assert startup.execs == [[sys.executable, "-m", "kohakuhub.worker"]]


def test_main_dispatches_worker_mode(startup, monkeypatch):
    monkeypatch.setenv("KOHAKU_HUB_LAKEFS_ACCESS_KEY", "from-env")
    monkeypatch.setattr(startup.sys, "argv", ["startup.py", "worker"])
    monkeypatch.setattr(startup, "wait_for_lakefs", lambda: pytest.fail("API bootstrap ran"))

    startup.main()

    assert startup.execs == [[sys.executable, "-m", "kohakuhub.worker"]]
