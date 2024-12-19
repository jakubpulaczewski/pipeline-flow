# Standard Imports
from __future__ import annotations

import os
import sys
import logging
import importlib
import importlib.util

# Third Party Imports

# Project Imports
from core.parser import PluginParser

logger = logging.getLogger(__name__)


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
        if not custom_files:
            return
        logger.info(f"Planning to load all custom files: {custom_files}")
        for custom_file in custom_files:
            self.load_plugin_from_file(custom_file)

        logger.info("Loaded all custom plugins.")
            
        
    def load_community_plugins(self, community_modules: set[str]):
        if not community_modules:
            return
        logger.info(f"Planning to load all community modules: {community_modules.join(', ')}")
        for module in community_modules:
            self.load_plugin_from_file(module)
        logger.info("Loaded all community plugins/")

    
    def load_plugins(self, engine: str, plugins_payload: dict) -> None:
        """ Invoke all methods to load plugins. """
        logger.info("Starting to load plugins...")

        plugin_parser = PluginParser(plugins_payload)

        # Load core engine transformations
        self.load_core_engine_transformations(engine)

        # Load custom plugins
        custom_files = plugin_parser.fetch_custom_plugin_files()
        self.load_custom_plugins(custom_files)

        # Load community plugins
        community_modules = plugin_parser.fetch_community_plugin_modules()
        self.load_community_plugins(community_modules)

        logger.info("All plugins loaded successfully.")
            
            