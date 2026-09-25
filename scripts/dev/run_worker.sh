#!/usr/bin/env bash
set -euo pipefail

# Run the background task worker against the local dev stack.
# Start the backend first (make backend): it runs migrations and writes the
# LakeFS credentials this worker reuses.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${ROOT_DIR}/.env.dev"
LAKEFS_CREDENTIALS_FILE="${ROOT_DIR}/hub-meta/dev/lakefs/credentials.env"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Missing ${ENV_FILE}"
  echo "Create it first: cp .env.dev.example .env.dev"
  exit 1
fi

if [[ -n "${VIRTUAL_ENV:-}" ]]; then
  PYTHON_BIN="${PYTHON_BIN:-python}"
elif [[ -x "${ROOT_DIR}/venv/bin/python" ]]; then
  PYTHON_BIN="${PYTHON_BIN:-${ROOT_DIR}/venv/bin/python}"
else
  PYTHON_BIN="${PYTHON_BIN:-python3}"
fi

set -a
# shellcheck disable=SC1090
source "${ENV_FILE}"
if [[ -f "${LAKEFS_CREDENTIALS_FILE}" ]]; then
  # shellcheck disable=SC1090
  source "${LAKEFS_CREDENTIALS_FILE}"
fi
set +a

export PYTHONPATH="${ROOT_DIR}/src${PYTHONPATH:+:${PYTHONPATH}}"

exec "${PYTHON_BIN}" -m kohakuhub.worker
