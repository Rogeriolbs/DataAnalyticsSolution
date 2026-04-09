"""Structured pipeline logger."""
import logging
import uuid
from datetime import datetime, timezone


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
        )
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


def new_run_id() -> str:
    return str(uuid.uuid4())


def utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)
