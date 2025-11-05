#!/usr/bin/env python3
"""Run the full ingest -> dedupe -> merge -> report pipeline.

This script orchestrates existing scripts in sequence.
"""
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def run(cmd):
    print(f"Running: {' '.join(cmd)}")
    res = subprocess.run(cmd, cwd=str(ROOT))
    if res.returncode != 0:
        print(f"Command failed: {' '.join(cmd)}")
        sys.exit(res.returncode)


def main():
    # 1) Fetch raw
    run([sys.executable, "scripts/fetch_all_raw.py",])
    # 2) Dedupe (replace)
    run([sys.executable, "scripts/dedupe_api_catalog.py", "--replace"])
    # 3) Merge to canonical
    run([sys.executable, "scripts/merge_to_canonical.py"])
    # 4) Generate report if available
    if (ROOT / "scripts" / "generate_report.py").exists():
        run([sys.executable, "scripts/generate_report.py"])
    else:
        print("No generate_report.py found, skipping report step")


if __name__ == "__main__":
    main()
