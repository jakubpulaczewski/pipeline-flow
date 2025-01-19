# Standard Imports
from common.logger import setup_logger
from core.loaders import PluginLoader
from core.orchestrator import PipelineOrchestrator

# # Project Imports
from core.parser import PipelineParser, YamlParser

# Third-party imports


def start(yaml_text: str | None = None, file_path: str | None = None) -> bool:
    # Set up the logger configuration
    setup_logger()

    # Parse YAML and initialize YAML config (engine, concurrency)
    yaml_parser = YamlParser(yaml_text, file_path)
    yaml_config = yaml_parser.initialize_yaml_config()
    plugins_payload = yaml_parser.get_plugins_dict()

    # Parse plugins directly within the load_plugins function
    plugin_loader = PluginLoader()
    plugin_loader.load_plugins(yaml_config.engine, plugins_payload)

    # Parse pipelines and execute them using the orchestrator
    pipeline_parser = PipelineParser()
    pipelines = pipeline_parser.parse_pipelines(yaml_parser.get_pipelines_dict())

    orchestrator = PipelineOrchestrator(yaml_config)
    orchestrator.execute_pipelines(pipelines)
    return True
