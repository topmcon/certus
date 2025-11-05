#!/usr/bin/env bash
set -euo pipefail

# CI/local wrapper for the action-friendly API check script.
# This previously regenerated the GitHub workflow file which caused
# confusing behavior in CI. Instead, invoke the checker directly.

python -u scripts/action_check_apis.py

