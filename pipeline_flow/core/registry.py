# Standard Imports
from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING, ClassVar

# Third Party Imports
# Project Imports
from pipeline_flow.common.utils import SingletonMeta

if TYPE_CHECKING:
    from pipeline_flow.common.type_def import PluginName, PluginPayload
    from pipeline_flow.plugins import IPlugin


class PluginRegistry(metaclass=SingletonMeta):
    """Plugin registry class for storing and retrieving plugins."""

    _registry: ClassVar[dict[PluginName, IPlugin]] = {}

    @classmethod
    def register(cls: PluginRegistry, plugin_name: PluginName, plugin_callable: IPlugin) -> None:
        """Registers a plugin in the registry.

        If the plugin is already registered, a ValueError is raised.
        """
        logging.debug("Registering plugin `%s`.", plugin_name)
        # Check if the plugin has been registered.
        if plugin_name in cls._registry:
            error_msg = f"Plugin for `{plugin_name}` already exists in PluginRegistry class."
            raise ValueError(error_msg)

        cls._registry[plugin_name] = plugin_callable
        logging.debug("Plugin `%s` have been successfully registered. ", plugin_name)

    @classmethod
    def get(cls: PluginRegistry, plugin_name: PluginName) -> IPlugin:
        """Retrieve a plugin from the registry."""
        logging.debug("Retrieving plugin class for `%s`.", plugin_name)
        plugin_factory = cls._registry.get(plugin_name, None)

        if not plugin_factory:
            msg = f"Plugin class was not found for following plugin `{plugin_name}`."
            raise ValueError(msg)

        logging.debug("Plugin class '%s' has been successfully retrieved.", plugin_factory)
        return plugin_factory

    @classmethod
    def instantiate_plugin(cls: PluginRegistry, plugin_data: PluginPayload) -> IPlugin:
        """Resolve and return a single plugin instance."""
        plugin_name = plugin_data.pop("plugin", None)
        if not plugin_name:
            raise ValueError("The attribute 'plugin' is empty.")

        plugin_factory: IPlugin = cls.get(plugin_name)

        plugin_id = plugin_data.pop("id", None) or f"{plugin_name}_{uuid.uuid4().hex[:16]}"
        plugin_params = plugin_data.get("args", {})

        return plugin_factory(plugin_id=plugin_id, **plugin_params)


# Registering plugins after PluginRegistry class definition. This is done to avoid circular imports as
# the plugins are registed when they are imported from IPlugin interface.
from pipeline_flow.plugins import *  # noqa: E402, F403
