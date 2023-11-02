import logging
import os

DEFAULT_LOG_LEVEL = os.environ.get("LOG_LEVEL") or logging.INFO

def setup_logger(name=None):
    """Setup and return a logger with the given name."""
    logger = logging.getLogger(name)
    logger.setLevel(DEFAULT_LOG_LEVEL)

    # Ensure we have at least one handler. In real-world scenarios, you might want to add more handlers (e.g., file handlers).
    if not logger.handlers:
        ch = logging.StreamHandler()
        ch.setLevel(DEFAULT_LOG_LEVEL)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    return logger