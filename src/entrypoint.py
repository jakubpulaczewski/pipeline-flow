# Standard Imports
from common.logger import setup_logger
from core.loaders import load_plugins
from core.orchestrator import PipelineOrchestrator

# # Project Imports
from core.parsers import YamlParser, parse_pipelines

# Third-party imports


async def start(yaml_text: str | None = None, file_path: str | None = None) -> bool:
    # Set up the logger configuration
    setup_logger()

    # Parse YAML and initialize YAML config (engine, concurrency)
    yaml_parser = YamlParser(yaml_text, file_path)
    yaml_config = yaml_parser.initialize_yaml_config()
    plugins_payload = yaml_parser.get_plugins_dict()

    # Parse plugins directly within the load_plugins function
    load_plugins(yaml_config.engine, plugins_payload)

    # Parse pipelines and execute them using the orchestrator
    pipelines = parse_pipelines(yaml_parser.get_pipelines_dict())

    orchestrator = PipelineOrchestrator(yaml_config)
    await orchestrator.execute_pipelines(pipelines)
    return True
