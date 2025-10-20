# certus/utils/logging.py

import logging
import sys

def _build_logger(name: str, level: int) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        # Already configured in this process
        return logger
    logger.setLevel(level)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s %(name)s — %(message)s"))
    logger.addHandler(handler)
    return logger

def get_logger(name: str = "certus", level: int = logging.INFO) -> logging.Logger:
    """Legacy/public API some files already use."""
    return _build_logger(name, level)

def setup_logger(name: str = "certus", level: int = logging.INFO) -> logging.Logger:
    """Alias kept for compatibility with older imports."""
    return _build_logger(name, level)
