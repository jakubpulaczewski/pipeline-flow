# Standard Imports
from __future__ import annotations

import importlib
from typing import Type, TYPE_CHECKING

# Third Party Imports

# Project Imports
from common.type_def import PLUGIN_BASE_CALLABLE
from common.utils.logger import setup_logger

import core.models.phases as phase
#from core.models.phases import PipelinePhase


logger = setup_logger(__name__)

PLUGIN_NAME = str


class PluginFactory:
    """A Plugin Factory that dynamically registers, removes and fetches plugins.

    Registry example: {'extract': {'s3': S3Plugin}}
    """

    _registry: dict[phase.PipelinePhase, dict[PLUGIN_NAME, PLUGIN_BASE_CALLABLE]] = {}

    @staticmethod
    def _validate_plugin_registration(
        pipeline_phase: phase.PipelinePhase, plugin_class: PLUGIN_BASE_CALLABLE
    ):
        expected_inteface = phase.PipelinePhase.get_plugin_interface_for_phase(pipeline_phase)

        if not expected_inteface:
            raise ValueError("No interface defined for phase %s", pipeline_phase)

        if not issubclass(plugin_class, expected_inteface):
            raise TypeError(
                f"Plugin class '{plugin_class.__name__}' must be a subclass of '{expected_inteface.__name__}' "
            )

    @classmethod
    def register(
        cls,
        pipeline_phase: phase.PipelinePhase,
        plugin: str,
        plugin_class: PLUGIN_BASE_CALLABLE,
    ) -> bool:
        """Regisers a plugin for a given Pipeline type and plugin."""
        # Validates the plugin implements the correct interface for the given phrase.
        cls._validate_plugin_registration(pipeline_phase, plugin_class)

        # Initialise the Pipeline phase in the registry.
        if pipeline_phase not in cls._registry:
            cls._registry[pipeline_phase] = {}

        # Check if the plugin has been registered.
        if plugin in cls._registry[pipeline_phase]:
            logger.warning(
                "Plugin for `%s` phase already exists in PluginFactory class.",
                pipeline_phase,
            )
            return False

        cls._registry[pipeline_phase][plugin] = plugin_class
        logger.debug(
            "Plugin `%s` for phase `%s` registered successfully.",
            plugin,
            pipeline_phase,
        )
        return True

    @classmethod
    def remove(cls, pipeline_phase: phase.PipelinePhase, plugin: str) -> bool:
        """Remove a plugin for a given Pipeline type and plugin."""
        if pipeline_phase in cls._registry and plugin in cls._registry[pipeline_phase]:
            del cls._registry[pipeline_phase][plugin]
            logger.debug(
                "Plugin '%s' for phase '%s' has been removed.", plugin, pipeline_phase
            )

            # Remove the ETL type dict if empty after removing the plugin.
            if not cls._registry[pipeline_phase]:
                logger.debug("Stage '%s' has been removed.", pipeline_phase)
                del cls._registry[pipeline_phase]
            return True
        return False

    @classmethod
    def get(
        cls, pipeline_phase: phase.PipelinePhase, plugin: str
    ) -> PLUGIN_BASE_CALLABLE:
        """Retrieve a plugin for a given ETL type and plugin."""
        etl_class = cls._registry.get(pipeline_phase, {}).get(plugin, None)

        if not etl_class:
            raise ValueError("Plugin class '%s' was not found", etl_class)
        logger.debug("Plugin class '%s' has been successfully retrieved.", etl_class)
        return etl_class


class PluginConfig:
    """A config plugin class that contains default Plugins"""

    _DEFAULT_EXTRACT_PLUGINS = {"pandas": ["s3"]}
    _DEFAULT_LOAD_PLUGINS = {"pandas": ["s3"]}

    # Mapping Pipeline phases to their respective default plugins


    @staticmethod
    def plugin_phase_mapper(pipeline_phase: str) -> dict[str, list[str]] | None:
        
        _STAGE_PLUGIN_MAP = {
            phase.PipelinePhase.EXTRACT_PHASE: PluginConfig._DEFAULT_EXTRACT_PLUGINS,
            phase.PipelinePhase.LOAD_PHASE: PluginConfig._DEFAULT_LOAD_PLUGINS,
        }
        return _STAGE_PLUGIN_MAP.get(pipeline_phase.lower())


class PluginLoader:
    ...
#     """A plugin loader class that initialiazes plugins at run time.

#     An example of plugins using yaml syntax:

#     plugins:
#         extract: [s3]
#         load: [s3, jdbc]
#     """

#     def __init__(self, plugins: dict[str, list[str]], engine: str) -> None:
#         self._validate_plugins(plugins)
#         self.plugins = plugins
#         self.engine = engine

#     @staticmethod
#     def _validate_plugins(plugins: dict[str, list[str]]) -> None:
#         if not isinstance(plugins, dict):
#             logger.error(
#                 "Invalid type for 'plugins': expected 'dict', got '%s'",
#                 type(plugins).__name__,
#             )
#             raise TypeError(
#                 f"Expected plugins to be a dict, got {type(plugins).__name__}"
#             )

#         allowed_phases = {phase.PipelinePhase.EXTRACT_PHASE, phase.PipelinePhase.LOAD_PHASE}

#         if not all(key in allowed_phases for key in plugins.keys()):
#             err_message = "Plugins must only contain 'EXTRACT' and 'LOAD' keys."
#             logger.error(err_message)
#             raise ValueError(err_message)

#     def _initialize_plugin(self, phase: str, plugin_list: list[str]) -> None:
#         """Initializes plugin modules for a given ETL phase."""
#         logger.info(
#             "Initializing plugin list %s for ETL phase %s with engine %s",
#             plugin_list,
#             phase,
#             self.engine,
#         )

#         default_plugins = PluginConfig.plugin_phase_mapper(phase)[self.engine]
#         combined_plugins = set(default_plugins + plugin_list)

#         if not combined_plugins:
#             raise ValueError("No plugins to load for phase {}".format(phase))

#         for plugin in combined_plugins:
#             try:
#                 module = importlib.import_module(
#                     f"plugins.{phase}.{self.engine}.{plugin}"
#                 )
#                 module.initialize()
#             except ModuleNotFoundError:
#                 logger.error(
#                     "Plugin module not found: plugins.%s.%s.%s",
#                     phase,
#                     self.engine,
#                     plugin,
#                 )
#                 raise

#         logger.info("All plugins have been initialized.")

#     def loader(self) -> None:
#         """Load the plugins defined in the plugins list."""
#         # For each phase in plugins, initialize the plugins for that phase.
#         for phase, plugin_list in self.plugins.items():
#             self._initialize_plugin(phase.lower(), plugin_list)


class PluginModuleInterface:
    """A plugin has a single function called initialize."""

    @staticmethod
    def initialize() -> None:
        """initialize the plugin module."""
        raise NotImplementedError(
            "The method has not been implemented. You must implement it in your python module."
        )
