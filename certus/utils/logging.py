# certus/utils/logging.py
from __future__ import annotations
import logging
from pathlib import Path
from typing import Optional

def get_logger(name: str = "certus", log_dir: Optional[str] = None) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # already configured

    logger.setLevel(logging.INFO)

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Console
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # File (optional)
    if log_dir:
        Path(log_dir).mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(str(Path(log_dir) / "pipeline.log"))
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    return logger
