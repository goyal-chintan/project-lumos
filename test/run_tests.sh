#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY="${REPO_ROOT}/.venv/bin/python"

if [[ ! -x "$PY" ]]; then
  echo "ERROR: expected .venv python at: ${PY}" >&2
  exit 1
fi

cd "${REPO_ROOT}"

exec "${PY}" -m pytest -c "${REPO_ROOT}/test/pytest.ini" test "$@"


