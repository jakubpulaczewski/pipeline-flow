# Standard Imports
from __future__ import annotations
from typing import TYPE_CHECKING

# Third-party imports
import yaml

# Project Imports
if TYPE_CHECKING:
    from common.type_def import PLUGIN_BASE_CALLABLE

from core.models.pipeline import Pipeline, PipelineType, MANDATORY_PHASES_BY_PIPELINE_TYPE
from core.models.phases import PipelinePhase
from core.plugins import PluginFactory


def deserialize_yaml(yaml_str: str) -> dict:
    """Deserialiazes a yaml string into python objects i.e. dicts, strings, int"""
    if not yaml_str:
        raise ValueError("A YAML string you provided is empty.")
    return yaml.safe_load(yaml_str)


def parse_phase_steps_plugins(phase: PipelinePhase, phase_args: dict) -> list[PLUGIN_BASE_CALLABLE]:
    plugins = []
    
    for step in phase_args["steps"]:
        plugin_name = step.get("plugin")
        plugin = PluginFactory.get(phase, plugin_name)(**step)
        plugins.append(plugin)

    return plugins


def create_pipeline_from_data(pipeline_name: str, pipeline_data: dict) -> Pipeline:
    """Parse a single pipeline's data and return a pipeline instance."""
    if not pipeline_data:
        raise ValueError("Pipeline attributes are empty")

    # Fetch the pipeline type (e.g., ETL, ELT, etc.)    
    for phase_name, phase_details in pipeline_data.get("phases")[0].items():

        phase_pipeline = PipelinePhase(phase_name)
        phase_class = PipelinePhase.get_phase_class(phase_pipeline)

        plugins = parse_phase_steps_plugins(phase_pipeline, phase_details)

        pipeline_data["phases"][0][phase_name] = phase_class(steps=plugins)
        


    return Pipeline(name=pipeline_name, **pipeline_data)


def create_pipelines_from_dict(pipelines: dict[str, dict]) -> list[Pipeline]:
    """Parse all pipelines and return a list of Pipeline instances."""
    return [
        create_pipeline_from_data(pipeline_name, pipeline_data)
        for pipeline_name, pipeline_data in pipelines.items()
    ]
