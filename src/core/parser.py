# Standard Imports
from __future__ import annotations
from typing import  Self
from enum import Enum

import os
import logging

# Third-party imports
import yaml

# Project Imports

from core.models.pipeline import Pipeline
from core.models.phases import (
    PhaseInstance,
    PluginCallable,
    PipelinePhase,
    PHASE_CLASS_MAP
)

from plugins.registry import PluginRegistry
from common.utils import SingletonMeta


logger = logging.getLogger(__name__)

DEFAULT_CONCURRENCY = 2
DEFAULT_ENGINE = 'native'

class YamlConfig(metaclass=SingletonMeta):
    engine: str
    concurrency: int

    def __init__(self, engine: str = DEFAULT_ENGINE, concurrency: int = DEFAULT_CONCURRENCY):
        self.engine = engine
        self.concurrency = concurrency


class YamlAttribute(Enum):
    PIPELINES = "pipelines"
    PLUGINS = "plugins"
    ENGINE = "engine"
    CONCURRENCY = "concurrency"


class YamlParser:
    """ Loads a YAML and initialiases YAML Config."""

    def __init__(self, yaml_text: str = None, file_path: str = None):
        if not (yaml_text or file_path):
            raise ValueError("Either `yaml_text` or `file_path` must be provided")

        self.parsed_yaml = self.parse_yaml(yaml_text, file_path)

    
    @staticmethod
    def parse_yaml_text(yaml_text: str) -> dict:
        if not yaml_text:
            raise ValueError("Provided YAML is empty.")
        try:
            return yaml.safe_load(yaml_text)
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f"Invalid YAML file: {e}")

    @staticmethod
    def parse_yaml_file(file_path: str) -> dict:
        try:
            with open(file_path, "r") as file:
                return YamlParser.parse_yaml_text(file)

        except FileNotFoundError:
            raise FileNotFoundError(f"File not found: {file_path}")

    @staticmethod
    def parse_yaml(yaml_text: str = None, file_path: str = None):
        if file_path:
            return YamlParser.parse_yaml_file(file_path)
        elif yaml_text:
            return YamlParser.parse_yaml_text(yaml_text)


    def initialize_yaml_config(self):
        # Create the map of attributes with their values
        attrs_map = {
            YamlAttribute.ENGINE.value: self.get_engine(),
            YamlAttribute.CONCURRENCY.value: self.get_concurrency()
        }

        # Filter out the None values
        attrs = {key: value for key, value in attrs_map.items() if value is not None}

        return YamlConfig(**attrs)

    def get_pipelines_dict(self: Self) -> dict:
        """Return the 'pipelines' section from the parsed YAML."""
        return self.parsed_yaml.get(YamlAttribute.PIPELINES.value, {})

    def get_plugins_dict(self: Self) -> str | None:
        return self.parsed_yaml.get(YamlAttribute.PLUGINS.value, {})

    def get_engine(self: Self) -> str | None:
        return self.parsed_yaml.get(YamlAttribute.ENGINE.value)
        
    def get_concurrency(self: Self) -> int:
        return self.parsed_yaml.get(YamlAttribute.CONCURRENCY.value)


class PluginParser:
    def __init__(self, plugins_payload: dict) -> None:
        self.plugins_payload = plugins_payload

    @staticmethod
    def get_all_files(paths: list[str]) -> set[str]:
        files = set()
        for path in paths:
            if os.path.isdir(path):
                for filename in os.listdir(path):
                    if filename.endswith(".py"):
                        files.add(os.path.join(path, filename))
            elif path.endswith(".py"):
                files.add(path)
        
        return files


    def fetch_custom_plugin_files(self) -> set[str | None]:
        custom_payload = self.plugins_payload.get("custom", {})
        if not custom_payload:
            logger.debug("No custom plugins found in the YAML.")
            return set()
        
        # Gather files from dirs and individual files
        files_from_dir = self.get_all_files(custom_payload.get("dirs", []))
        files = self.get_all_files(custom_payload.get("files", []))

        # Combine both sets of files
        return files_from_dir.union(files)

    def fetch_community_plugin_modules(self) -> set[str | None]:        
        comm_payload = self.plugins_payload.get("community", {})
        if not comm_payload:
            logger.debug("No community plugins found in the YAML.")
            return set()
    
        base_module = "community.plugins."

        comm_plugins = { base_module + plugin for plugin in comm_payload }
        return comm_plugins
    
    
class PipelineParser:


    def parse_pipelines(self: Self, pipelines_data) -> list[Pipeline]:
        pipelines_data = pipelines_data

        if not pipelines_data:
            raise ValueError("No Pipelines detected.")
        
        return [
            self.create_pipeline(pipeline_name, pipeline_data)
            for pipeline_name, pipeline_data in pipelines_data.items()
        ]


    def create_pipeline(self: Self, pipeline_name: str, pipeline_data: dict) -> Pipeline:
        """Parse a single pipeline's data and return a pipeline instance."""
        if not pipeline_data:
            raise ValueError("Pipeline attributes are empty")

        phases = {}
        
        for phase_name, phase_details in pipeline_data.get("phases").items():
            phases[phase_name] = self.create_phase(phase_name, phase_details)
            
        pipeline_data["phases"] = phases
        return Pipeline(name=pipeline_name, **pipeline_data)
    

    def create_phase(self, phase_name: str, phase_data: dict) -> PhaseInstance:
        phase_pipeline = PipelinePhase(phase_name)
        phase_class = PHASE_CLASS_MAP.get(phase_pipeline)

        
        # Parse plugins for different attributes
        # TODO: Potentially need to be changed as root_validators.
        # If there are mroe than two extracts, merge should be inputted otherwise fail.
        # Write tests for it. #TODO: Both unit and integrations!!
        if "steps" in phase_data:
            phase_data["steps"] = self.parse_plugins_by_phase(phase_pipeline, phase_data["steps"])

        if "pre" in phase_data:
            phase_data["pre"] = self.parse_plugins_by_phase(phase_pipeline, phase_data["pre"])
        
        if "post" in phase_data:
            phase_data["post"] =  self.parse_plugins_by_phase(phase_pipeline, phase_data["post"])
        
        if "merge" in phase_data:
            phase_data["merge"] = self.parse_plugin_by_phase(phase_pipeline, phase_data["merge"])

        return phase_class(**phase_data)

    
    def parse_plugins_by_phase(self, phase_pipeline: PipelinePhase, plugins: list[dict]) -> list[PluginCallable]:
        return [
            self.parse_plugin_by_phase(phase_pipeline, data) for data in plugins
        ]
  
        
    @staticmethod
    def parse_plugin_by_phase(phase_pipeline: PipelinePhase, plugin_data: dict) -> PluginCallable:
        """Parse and return a single plugin."""
        plugin_name = plugin_data.pop("plugin", None)
        if not plugin_name:
            raise ValueError("The attribute 'plugin' is empty.")
        
        # Create the plugin instance from the registry based on phase and plugin name
        plugin = PluginRegistry.get(phase_pipeline, plugin_name)(**plugin_data)
        return plugin

