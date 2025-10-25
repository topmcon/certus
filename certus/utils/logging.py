from __future__ import annotations
from loguru import logger
import sys, os

os.makedirs("logs", exist_ok=True)
logger.remove()  # drop default
logger.add(sys.stderr, level="INFO", enqueue=True, backtrace=False, diagnose=False)
logger.add("logs/p45.log", level="INFO", enqueue=True, rotation="10 MB", retention="7 days")
