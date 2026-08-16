#!/usr/bin/env python3
"""Load deployment-generated LakeFS credentials and start khub-worker."""

from __future__ import annotations

import os
from pathlib import Path
import sys
import time


CREDENTIALS_FILE = Path("/hub-api-creds/credentials.env")


def load_credentials() -> None:
    """Load credentials written by the API startup container.

    Explicit environment variables win so operators can inject a secret from
    an external secret manager instead of sharing the generated credentials
    volume.
    """

    if not CREDENTIALS_FILE.exists():
        return
    for line in CREDENTIALS_FILE.read_text(encoding="utf-8").splitlines():
        key, separator, value = line.partition("=")
        if separator and key and value:
            os.environ.setdefault(key.strip(), value.strip())


def main() -> None:
    for _ in range(60):
        load_credentials()
        if all(
            os.getenv(name)
            for name in (
                "KOHAKU_HUB_LAKEFS_ACCESS_KEY",
                "KOHAKU_HUB_LAKEFS_SECRET_KEY",
            )
        ):
            break
        time.sleep(1)
    required = (
        "KOHAKU_HUB_LAKEFS_ACCESS_KEY",
        "KOHAKU_HUB_LAKEFS_SECRET_KEY",
    )
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise SystemExit(
            "khub-worker requires LakeFS credentials; missing " + ", ".join(missing)
        )
    os.execv(sys.executable, [sys.executable, "-m", "kohakuhub.worker"])


if __name__ == "__main__":
    main()
