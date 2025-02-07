# Standard Imports
import logging

from common.logger import setup_logger
from core.loaders import load_plugins
from core.orchestrator import PipelineOrchestrator

# # Project Imports
from core.parsers import YamlParser, parse_pipelines


async def start(yaml_text: str | None = None, file_path: str | None = None) -> bool:
    # Set up the logger configuration
    setup_logger()

    # Parse YAML and initialize YAML config (engine, concurrency)
    if yaml_text is None and file_path is None:
        raise ValueError("Either yaml_text or file_path must be provided.")

    yaml_parser = (
        await YamlParser.from_file(file_path) if file_path else YamlParser.from_text(yaml_text) if yaml_text else None
    )
    if not yaml_parser:
        raise ValueError("YamlParser could not be initialized.")

    yaml_config = yaml_parser.initialize_yaml_config()
    plugins_payload = yaml_parser.get_plugins_dict()

    # Parse plugins directly within the load_plugins function
    load_plugins(yaml_config.engine, plugins_payload)

    # Parse pipelines and execute them using the orchestrator
    pipelines = parse_pipelines(yaml_parser.get_pipelines_dict())

    try:
        orchestrator = PipelineOrchestrator(yaml_config)
        await orchestrator.execute_pipelines(pipelines)

    except Exception as e:
        logging.error("The following error occurred: %s", e)
        logging.error("The original cause is: %s", e.__cause__)
        raise
    else:
        return True
