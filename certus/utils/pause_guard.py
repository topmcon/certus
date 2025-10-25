# certus/utils/pause_guard.py
import os
import sys

def guard_pause():
    """Exit immediately if Certus is globally paused."""
    if os.getenv("CERTUS_PAUSED", "").lower() in ("true", "1", "yes"):
        print("[Certus] ⚠️  System is paused — skipping execution.")
        sys.stdout.flush()
        sys.exit(0)
