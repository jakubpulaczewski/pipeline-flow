# Standard Imports
from __future__ import annotations

import os
import sys
import logging
import importlib

import importlib.util

# Third Party Imports

# Project Imports


from common.utils import SingletonMeta
from core.models.phases import (
    PipelinePhase, 
    PLUGIN_PHASE_INTERFACE_MAP, 
    PluginCallable
)


logger = logging.getLogger(__name__)

PLUGIN_NAME = str

# Decorator for plugin registration
def plugin(plugin_phase: str | PipelinePhase, plugin_name: str):

    if (type(plugin_phase) == str):
        plugin_phase = PipelinePhase(plugin_phase)

    def decorator(plugin_class):
        PluginFactory.register(plugin_phase, plugin_name, plugin_class)
        return plugin_class
    return decorator

class PluginLoader:

    @staticmethod
    def load_plugin_from_file(plugin_file: str) -> None:
        # Get the module name from the file and remove .py extension
        if plugin_file.startswith('/'):
            fq_module_name = plugin_file.replace(os.sep, '.')[1:-3]  
        else:
            fq_module_name =  plugin_file.replace(os.sep, '.')[:-3]  
        
        # Check if the module is already loaded to avoid re-importing
        if fq_module_name in sys.modules:
            logger.debug(f"Module {fq_module_name} has been re-loaded.")
            return
        
        try:
            logger.debug(f"Loading module {fq_module_name}")
            spec = importlib.util.spec_from_file_location(fq_module_name, plugin_file)
            plugin_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(plugin_module)
            logger.info(f"Loaded plugin from {plugin_file} as {fq_module_name}")
        except ImportError as e:
            logger.error(f"Error importing plugin from `{fq_module_name}` module. ")


    def load_core_engine_transformations(self, engine: str) -> None:
        core_engine_file = f"src/plugins/transform/{engine}.py"
        self.load_plugin_from_file(core_engine_file)


    def load_custom_plugins(self, custom_files: set[str]) -> None:
        logger.info(f"Planning to load all custom files: {custom_files}")
        for custom_file in custom_files:
            self.load_plugin_from_file(custom_file)

        logger.info("Loaded all custom plugins.")
            
        
    def load_community_plugins(self):
        raise NotImplementedError("Will be implemented in the future.")  
    
    def load_plugins():
        ...
            
            
class PluginFactory(metaclass=SingletonMeta):
    """A Plugin Factory that dynamically registers, removes and fetches plugins.

    Registry example: {EXTRACT_PHASE: {'s3': S3Plugin}}
    """

    _registry: dict[PipelinePhase, dict[PLUGIN_NAME, PluginCallable]] = {}

    @staticmethod
    def _validate_plugin_interface(
        pipeline_phase: PipelinePhase, plugin_callable: PluginCallable
    ):
        expected_inteface = PLUGIN_PHASE_INTERFACE_MAP.get(pipeline_phase)
                                                        
        if pipeline_phase == PipelinePhase.TRANSFORM_PHASE:           
             # Plugin can be either callable or a subclass of the expected interface
            if callable(plugin_callable):
               return 
            

        if not issubclass(plugin_callable, expected_inteface):
            raise TypeError(
                f"Plugin class '{plugin_callable.__name__}' must be a subclass of '{expected_inteface.__name__}' "
            )

    @classmethod
    def register(
        cls,
        pipeline_phase: PipelinePhase,
        plugin: str,
        plugin_callable: PluginCallable,
    ) -> bool:
        """Regisers a plugin for a given Pipeline type and plugin."""
        # Validates the plugin implements the correct interface for the given phrase.
        #cls._validate_plugin_interface(pipeline_phase, plugin_callable)

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

        cls._registry[pipeline_phase][plugin] = plugin_callable
        logger.debug(
            "Plugin `%s` for phase `%s` registered successfully.",
            plugin,
            pipeline_phase,
        )
        return True

    @classmethod
    def remove(cls, pipeline_phase: PipelinePhase, plugin: str) -> bool:
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
        cls, pipeline_phase: PipelinePhase, plugin: str
    ) -> PluginCallable:
        """Retrieve a plugin for a given ETL type and plugin."""
        etl_class = cls._registry.get(pipeline_phase, {}).get(plugin, None)

        if not etl_class:
            raise ValueError("Plugin class was not found for following plugin `{}`.".format(plugin))
        logger.debug("Plugin class '%s' has been successfully retrieved.", etl_class)
        return etl_class
