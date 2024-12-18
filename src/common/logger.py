# Standard Imports
import logging
import logging.config
import os

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
        raise ValueError(f"Invalid logging level: {log_level}. Expected 'info' or 'debug'.")

    if not os.path.isfile(config_file):
        raise FileNotFoundError(f"Logging configuration file not found: {config_file}")

    # Load logging configuration from the selected config file
    logging.config.fileConfig(config_file, disable_existing_loggers=False)