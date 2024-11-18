# Standard Imports
from __future__ import annotations
from typing import TYPE_CHECKING

# Third-party imports
import yaml

# Project Imports
if TYPE_CHECKING:
    from common.type_def import PLUGIN_BASE_CALLABLE

from core.models.pipeline import Pipeline
from core.models.phases import PipelinePhase, PHASE_TYPE
from core.plugins import PluginFactory




def parse_yaml_str(yaml_text: str) -> dict:
    """ Parses a YAML text into a Python native data structures."""
    if not yaml_text:
        raise ValueError("Provided YAML is empty.")
    try:
        return yaml.safe_load(yaml_text)
    except yaml.YAMLError as e:
        raise yaml.YAMLError(f"Invalid YAML file: {e}")


def parse_yaml_file(file_path: str) -> dict:
    """ Parses a YAML file into a Python native data structures."""
    try:
        with open(file_path, "r") as file:
            return parse_yaml_str(file)

    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {file_path}")



def parse_plugins_by_phase(phase_pipeline: PipelinePhase, steps: list[dict]) -> list[PLUGIN_BASE_CALLABLE]:
    plugin_callables = []

    for step in steps:
        plugin_name = step.pop("plugin", None)

        if not plugin_name:
            raise ValueError("The attribute 'plugin` is empty.")
        
        plugin = PluginFactory.get(phase_pipeline, plugin_name)(**step)
        plugin_callables.append(plugin)
    
    return plugin_callables
        


def create_phase(phase_name: str, phase_data: dict) -> PHASE_TYPE:
    phase_pipeline = PipelinePhase(phase_name)
    phase_class = PipelinePhase.get_phase_class(phase_pipeline)

    phase_data["steps"] = parse_plugins_by_phase(phase_pipeline, phase_data.get("steps", []))

    return phase_class(**phase_data)



def create_pipeline(pipeline_name: str, pipeline_data: dict) -> Pipeline:
    """Parse a single pipeline's data and return a pipeline instance."""
    if not pipeline_data:
        raise ValueError("Pipeline attributes are empty")

    phases = {}
    
    for phase_name, phase_details in pipeline_data.get("phases").items():
        phases[phase_name] = create_phase(phase_name, phase_details)
        
    pipeline_data["phases"] = phases
    return Pipeline(name=pipeline_name, **pipeline_data)


def parse_all_pipelines(yaml_data: dict[str, dict]) -> list[Pipeline]:
    if "pipelines" not in yaml_data:
        raise ValueError("Pipeline attributes are empty")
    return [
        create_pipeline(pipeline_name, pipeline_data)
        for pipeline_name, pipeline_data in yaml_data.get("pipelines", []).items()
    ]


def start_parser(yaml_text: str = None, file_path: str = None):
    if file_path:
        parsed_yaml = parse_yaml_file(file_path)
    elif yaml_text:
        parsed_yaml = parse_yaml_str(yaml_text)
    else:
        raise ValueError("Either `yaml_text` or `file_path` must be provided")

    return parse_all_pipelines(parsed_yaml)
