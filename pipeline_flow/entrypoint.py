# Standard Imports
import logging

# # Project Imports
from pipeline_flow.common.type_def import StreamType
from pipeline_flow.common.utils import setup_logger
from pipeline_flow.core.orchestrator import PipelineOrchestrator
from pipeline_flow.core.parsers import YamlParser, parse_pipelines
from pipeline_flow.core.plugin_loader import load_plugins


async def start_orchestration(stream: StreamType) -> None:
    """Main entry point for orchestrating the pipeline flow.

    This function parses the YAML configuration, loads the plugins,
    and executes the pipelines using the PipelineOrchestrator.

    The Stream must be one of the following types:
       - `str` object: A string containing the YAML configuration.
       - 'obj' object: An object containing the YAML configuration.
       - `file` object: A file object containing the YAML configuration.

    Args:
        stream (StreamType): A stream containing the YAML configuration.
    """
    # Set up the logger configuration
    if not logging.getLogger().hasHandlers() > 0:
        setup_logger()

    # Parse YAML
    yaml_parser = YamlParser(stream)

    # Parse plugins directly within the load_plugins function
    load_plugins(yaml_parser.plugins)

    # Parse pipelines and execute them using the orchestrator
    pipelines = parse_pipelines(yaml_parser.pipelines)

    try:
        yaml_config = yaml_parser.initialize_yaml_config()
        orchestrator = PipelineOrchestrator(yaml_config)
        await orchestrator.execute_pipelines(pipelines)

    except Exception as e:
        logging.error("The following error occurred: %s", e)
        logging.error("The original cause is: %s", e.__cause__)
        raise
