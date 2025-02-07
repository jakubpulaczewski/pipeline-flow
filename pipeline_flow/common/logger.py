# Standard Imports
import logging
import logging.config
import os
from pathlib import Path

# Third-party imports


# Project Imports


def setup_logger() -> None:
    log_level = os.getenv("LOG_LEVEL", "info").lower()

    config_files = {
        "info": "logging/info_logging.conf",
        "debug": "logging/debug_logging.conf",
    }

    config_file = config_files.get(log_level)

    if not config_file:
        msg = "Invalid logging level: %s. Expected 'info' or 'debug'.", log_level
        raise ValueError(msg)

    if not Path(config_file).is_file():
        msg = "Logging configuration file not found: %s", config_file
        raise FileNotFoundError(msg)

    # Load logging configuration from the selected config file
    logging.config.fileConfig(config_file, disable_existing_loggers=False)
