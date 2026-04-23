from __future__ import annotations

import json
import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Any


def configure_rotating_logger(name: str, path: Path) -> logging.Logger:
    path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if logger.handlers:
        return logger

    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    file_handler = TimedRotatingFileHandler(path, when="midnight", backupCount=30, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    return logger


def log_json(logger: logging.Logger, event: str, **payload: Any) -> None:
    body = {"event": event, **payload}
    logger.info(json.dumps(body, sort_keys=True, default=str))
