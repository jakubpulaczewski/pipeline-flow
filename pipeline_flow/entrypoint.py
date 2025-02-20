# Standard Imports
import logging

# # Project Imports
from pipeline_flow.common.type_def import StreamType
from pipeline_flow.common.utils import setup_logger
from pipeline_flow.core.orchestrator import PipelineOrchestrator
from pipeline_flow.core.parsers import YamlParser, parse_pipelines
from pipeline_flow.core.plugin_loader import load_plugins


async def start_orchestration(yaml_text: StreamType | None, local_file_path: str | None = None) -> bool:
    # Set up the logger configuration
    setup_logger()

    # Parse YAML
    yaml_parser = YamlParser.from_input(yaml_text, local_file_path)
    yaml_config = yaml_parser.initialize_yaml_config()
    plugins_payload = yaml_parser.get_plugins_dict()

    # Parse plugins directly within the load_plugins function
    load_plugins(plugins_payload)

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
