"""
This module provides a factory class for registering, adding and removing plugins.
"""

from typing import Type

from common.config import ETL_CLASS
from common.utils.logger import setup_logger

logger = setup_logger(__name__)


class PluginFactory:
    """A Plugin Factory that dynamically registers, removes and fetches plugins.

    Registry example: {'extract': {'s3': S3Plugin}}

    """

    _registry: dict[str, dict[str, ETL_CLASS]] = {}

    @classmethod
    def register(cls, etl_stage, plugin, plugin_class) -> bool:
        """Regisers a plugin for a given ETL type and plugin."""
        # FIX - In the future should get the instance of the protocol
        # implemented classs and self derive it.
        if etl_stage not in cls._registry:
            cls._registry[etl_stage] = {}

        if plugin in cls._registry[etl_stage]:
            logger.warning(
                "Plugin for `%s` stage already exists in PluginFactory class.",
                etl_stage,
            )
            return False

        cls._registry[etl_stage][plugin] = plugin_class
        logger.debug(
            "Plugin `%s` for stage `%s` registered successfully.", plugin, etl_stage
        )
        return True

    @classmethod
    def remove(cls, etl_stage, plugin) -> bool:
        """Remove a plugin for a given ETL type and plugin."""
        if etl_stage in cls._registry and plugin in cls._registry[etl_stage]:
            del cls._registry[etl_stage][plugin]
            logger.debug(
                "Plugin '%s' for stage '%s' has been removed.", plugin, etl_stage
            )

            # Remove the ETL type dict if empty after removing the plugin.
            if not cls._registry[etl_stage]:
                logger.debug("Stage '%s' has been removed.", etl_stage)
                del cls._registry[etl_stage]
            return True
        return False

    @classmethod
    def get(cls, etl_stage, plugin) -> Type[ETL_CLASS] | None:
        """Retrieve a plugin for a given ETL type and plugin."""
        etl_class = cls._registry.get(etl_stage, {}).get(plugin, None)
        logger.debug("Plugin class '%s' has been successfully retrieved.", etl_class)
        return etl_class
