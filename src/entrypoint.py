# Standard Imports
import logging
import os

# Third-party imports

# # Project Imports
from core.parser import YamlParser, PipelineParser, PluginParser
from core.orchestrator import PipelineOrchestrator
from plugins.registry import PluginLoader

from common.logger import setup_logger

def start(yaml_text: str = None, file_path: str = None):
    # Set up the logger configuration
    setup_logger()
    
    # Parse YAML and initialize YAML config (engine, concurrency)
    yaml_parser = YamlParser(yaml_text, file_path)
    yaml_config = yaml_parser.initialize_yaml_config()


    # Parse plugins and load custom plugins if available
    plugin_parser = PluginParser(yaml_parser)
    custom_plugins = plugin_parser.get_custom_plugin_files()

    loader = PluginLoader()
    if custom_plugins:
        loader.load_custom_plugins(custom_plugins)

    if yaml_config.engine:
        loader.load_core_engine_transformations(yaml_config.engine)

    # Parse pipelines and execute them using the orchestrator
    pipelines = PipelineParser(yaml_parser)
    orchestrator = PipelineOrchestrator(yaml_config)
    orchestrator.execute_pipelines(pipelines)


