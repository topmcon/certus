#!/usr/bin/env bash
set -euo pipefail
export PYTHONUNBUFFERED=1
uvicorn certus.api.cp45:app --host 0.0.0.0 --port 8001 --reload
