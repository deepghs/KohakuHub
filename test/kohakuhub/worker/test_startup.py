from __future__ import annotations

from pathlib import Path

import pytest

import docker.worker_startup as startup


def test_load_credentials_does_not_override_environment(monkeypatch, tmp_path: Path):
    credentials = tmp_path / "credentials.env"
    credentials.write_text(
        "KOHAKU_HUB_LAKEFS_ACCESS_KEY=file-key\n"
        "KOHAKU_HUB_LAKEFS_SECRET_KEY=file-secret\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(startup, "CREDENTIALS_FILE", credentials)
    monkeypatch.setenv("KOHAKU_HUB_LAKEFS_ACCESS_KEY", "injected-key")
    monkeypatch.delenv("KOHAKU_HUB_LAKEFS_SECRET_KEY", raising=False)

    startup.load_credentials()

    assert startup.os.environ["KOHAKU_HUB_LAKEFS_ACCESS_KEY"] == "injected-key"
    assert startup.os.environ["KOHAKU_HUB_LAKEFS_SECRET_KEY"] == "file-secret"


def test_main_executes_worker_after_credentials_are_loaded(monkeypatch, tmp_path: Path):
    credentials = tmp_path / "credentials.env"
    credentials.write_text(
        "KOHAKU_HUB_LAKEFS_ACCESS_KEY=file-key\n"
        "KOHAKU_HUB_LAKEFS_SECRET_KEY=file-secret\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(startup, "CREDENTIALS_FILE", credentials)
    monkeypatch.delenv("KOHAKU_HUB_LAKEFS_ACCESS_KEY", raising=False)
    monkeypatch.delenv("KOHAKU_HUB_LAKEFS_SECRET_KEY", raising=False)
    calls = []
    monkeypatch.setattr(startup.os, "execv", lambda *args: calls.append(args))

    startup.main()

    assert calls == [(startup.sys.executable, [startup.sys.executable, "-m", "kohakuhub.worker"])]


def test_main_fails_without_lakefs_credentials(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(startup, "CREDENTIALS_FILE", tmp_path / "missing.env")
    monkeypatch.delenv("KOHAKU_HUB_LAKEFS_ACCESS_KEY", raising=False)
    monkeypatch.delenv("KOHAKU_HUB_LAKEFS_SECRET_KEY", raising=False)
    monkeypatch.setattr(startup.time, "sleep", lambda _seconds: None)

    with pytest.raises(SystemExit, match="requires LakeFS credentials"):
        startup.main()
