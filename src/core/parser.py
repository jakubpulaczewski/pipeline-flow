# Standard Imports

# Third-party imports
import yaml

# Project Imports
from common.config import ETLConfig
from core.models import Pipeline
from core.plugins import PluginFactory


def deserialize_yaml(yaml_str: str) -> dict:
    """Deserialiazes a yaml string into python objects i.e. dicts, strings, int"""
    return yaml.safe_load(yaml_str)


def parse_plugins_for_etl_stage(stage_data: dict, stage: str) -> list[ETLConfig.ETL_CALLABLE]:
    """Parse individual ETL pipeline plugins by fetching their respective plugin class."""
    plugins = []
    for args in stage_data.get(stage, []):
        service = args.pop("type")
        plugin = PluginFactory.get(stage, service)
        plugins.append(plugin)
    return plugins

def create_pipeline_from_data(pipeline_name: str, pipeline_data: dict) -> Pipeline:
    """Parse a single pipeline's data and return a pipeline instance."""
    parsed_data = pipeline_data.copy()
    for stage in ETLConfig.ETL_STAGES:
        parsed_data[stage] = parse_plugins_for_etl_stage(pipeline_data, stage)

    return Pipeline(name=pipeline_name, **parsed_data)


def create_pipelines_from_dict(pipelines: dict) -> list[Pipeline]:
    """Parse all pipelines and return a list of Pipeline instances."""
    return [
        create_pipeline_from_data(pipeline_name, pipeline_data) 
        for pipeline_name, pipeline_data in pipelines.items()
    ]