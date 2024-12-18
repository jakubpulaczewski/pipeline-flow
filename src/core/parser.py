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

from plugins.registry import PluginFactory
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

    def __init__(self, yaml_text: str = None, file_path: str = None):
        """Initialize and parse the YAML text or file into a dictionary."""
        self.parsed_yaml = self.parse_yaml(yaml_text, file_path)

    @staticmethod
    def parse_yaml_text(yaml_text: str) -> dict:
        if not yaml_text:
            raise ValueError("Provided YAML is empty.")
        try:
            return yaml.safe_load(yaml_text)
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f"Invalid YAML file: {e}")

    def parse_yaml_file(self, file_path: str) -> dict:
        try:
            with open(file_path, "r") as file:
                return self.parse_yaml_text(file)

        except FileNotFoundError:
            raise FileNotFoundError(f"File not found: {file_path}")

    def parse_yaml(self, yaml_text: str = None, file_path: str = None):
        if file_path:
            return self.parse_yaml_file(file_path)
        elif yaml_text:
            return self.parse_yaml_text(yaml_text)

        raise ValueError("Either `yaml_text` or `file_path` must be provided")

    def initialize_yaml_config(self):
        # Create the map of attributes with their values
        attrs_map = {
            YamlAttribute.ENGINE.value: self.get_engine(),
            YamlAttribute.CONCURRENCY.value: self.get_concurrency()
        }

        # Filter out the None values
        attrs = {key: value for key, value in attrs_map.items() if value is not None}

        return YamlConfig(**attrs)

    def get_pipelines_data(self: Self) -> dict:
        """Return the 'pipelines' section from the parsed YAML."""
        return self.parsed_yaml.get(YamlAttribute.PIPELINES.value, {})

    def get_plugins(self: Self) -> str | None:
        return self.parsed_yaml.get(YamlAttribute.PLUGINS.value, {})

    def get_engine(self: Self) -> str | None:
        return self.parsed_yaml.get(YamlAttribute.ENGINE.value)
        
    def get_concurrency(self: Self) -> int:
        return self.parsed_yaml.get(YamlAttribute.CONCURRENCY.value)


class PluginParser:
    def __init__(self, yaml_parser: YamlParser) -> None:
        self.yaml_parser = yaml_parser
        self.plugins_data = yaml_parser.get_plugins()


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


    def get_custom_plugin_files(self) -> set[str]:
        plugin_data = self.plugins_data.get("custom", {})
        if not plugin_data:
            logger.debug("No custom plugins found in YAML.")
            return None
        
        # Gather files from dirs and individual files
        files_from_dir = self.get_all_files(plugin_data.get("dirs", []))
        files = self.get_all_files(plugin_data.get("files", []))

        # Combine both sets of files
        return files_from_dir.union(files)

    def get_community_plugin_files(self) -> set[str]:
        raise NotImplementedError("This will be implemented.")
            

class PipelineParser:

    def __init__(self, yaml_parser: YamlParser):
        self.pipelines_data = yaml_parser.get_pipelines_data()

        if not self.pipelines_data:
            raise ValueError("Pipeline attributes are empty")

    def parse_pipelines(self: Self) -> list[Pipeline]:

        return [
            self.create_pipeline(pipeline_name, pipeline_data)
            for pipeline_name, pipeline_data in self.pipelines_data.items()
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
        phase_class = self.get_phase_class(phase_pipeline)

        phase_data["steps"] = self.parse_plugins_by_phase(phase_pipeline, phase_data.get("steps", []))

        return phase_class(**phase_data)

    @staticmethod
    def get_phase_class(phase_name: PipelinePhase) -> PhaseInstance:
        phase_class = PHASE_CLASS_MAP.get(phase_name)

        if not phase_class:
            raise ValueError(f"Unknown phase: {phase_name}")
        return phase_class


    @staticmethod
    def parse_plugins_by_phase(phase_pipeline: PipelinePhase, steps: list[dict]) -> list[PluginCallable]:
        plugin_callables = []

        for step in steps:
            plugin_name = step.pop("plugin", None)

            if not plugin_name:
                raise ValueError("The attribute 'plugin` is empty.")
            
            plugin = PluginFactory.get(phase_pipeline, plugin_name)(**step)
            plugin_callables.append(plugin)
        
        return plugin_callables
            




