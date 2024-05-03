from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from logging import Logger
    
DEFAULT_LOG_LEVEL = os.environ.get("LOG_LEVEL") or logging.INFO


def setup_logger(name: str = None) -> Logger:
    """Setup and return a logger with a given name."

    Args:
        name (str, optional): Name of the python file. Defaults to None.

    Returns:
        Logger: A logger object
    """

    logger = logging.getLogger(name)
    logger.setLevel(DEFAULT_LOG_LEVEL)

    # Ensure we have at least one handler.
    # In real-world scenarios, you might want to add more handlers (e.g., file handlers).
    if not logger.handlers:
        ch = logging.StreamHandler()
        ch.setLevel(DEFAULT_LOG_LEVEL)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    return logger
