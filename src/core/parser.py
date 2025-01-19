# Standard Imports
from __future__ import annotations

import logging
import os
from enum import Enum
from pathlib import Path
from typing import Any, Self, TextIO

# Third-party imports
import yaml

from common.utils import SingletonMeta
from core.models.phases import PHASE_CLASS_MAP, PhaseInstance, PipelinePhase
from core.models.pipeline import Pipeline

# Project Imports

type JSON_DATA = dict
logger = logging.getLogger(__name__)

DEFAULT_CONCURRENCY = 2
DEFAULT_ENGINE = "native"


class YamlConfig(metaclass=SingletonMeta):
    engine: str
    concurrency: int

    def __init__(self, engine: str = DEFAULT_ENGINE, concurrency: int = DEFAULT_CONCURRENCY) -> None:
        self.engine = engine
        self.concurrency = concurrency


class YamlAttribute(Enum):
    PIPELINES = "pipelines"
    PLUGINS = "plugins"
    ENGINE = "engine"
    CONCURRENCY = "concurrency"


class YamlParser:
    """Loads a YAML and initialiases YAML Config."""

    def __init__(self, yaml_text: str | None = None, file_path: str | None = None) -> None:
        if not (yaml_text or file_path):
            raise ValueError("Either `yaml_text` or `file_path` must be provided")

        self.parsed_yaml = self.parse_yaml(yaml_text, file_path)

    @staticmethod
    def parse_yaml_text(yaml_text: str | TextIO) -> JSON_DATA:
        if not yaml_text:
            raise ValueError("Provided YAML is empty.")
        try:
            return yaml.safe_load(yaml_text)
        except yaml.YAMLError as e:
            raise yaml.YAMLError("Invalid YAML file.") from e

    @staticmethod
    def parse_yaml_file(file_path: str, encoding: str = "utf-8") -> JSON_DATA:
        try:
            with open(file_path, encoding=encoding) as file:  # noqa: PTH123
                return YamlParser.parse_yaml_text(file)

        except FileNotFoundError as error:
            error_msg = f"File not found: {file_path}"
            raise FileNotFoundError(error_msg) from error

    @staticmethod
    def parse_yaml(yaml_text: str | None = None, file_path: str | None = None) -> JSON_DATA:
        if yaml_text and file_path:
            raise ValueError("You cannot provide `yaml_text` and `file_path` both at the same time.")

        if file_path:
            return YamlParser.parse_yaml_file(file_path)
        if yaml_text:
            return YamlParser.parse_yaml_text(yaml_text)
        raise ValueError("You must provide either `yaml_text` or `file_path`.")

    def initialize_yaml_config(self: Self) -> YamlConfig:
        # Create the map of attributes with their values
        attrs_map = {
            YamlAttribute.ENGINE.value: self.parsed_yaml.get(YamlAttribute.ENGINE.value, DEFAULT_ENGINE),
            YamlAttribute.CONCURRENCY.value: self.parsed_yaml.get(YamlAttribute.CONCURRENCY.value, DEFAULT_CONCURRENCY),
        }

        # Filter out the None values
        attrs = {key: value for key, value in attrs_map.items() if value is not None}

        return YamlConfig(**attrs)

    def get_pipelines_dict(self: Self) -> dict:
        """Return the 'pipelines' section from the parsed YAML."""
        return self.parsed_yaml.get(YamlAttribute.PIPELINES.value, {})

    def get_plugins_dict(self: Self) -> str | None:
        return self.parsed_yaml.get(YamlAttribute.PLUGINS.value, {})


class PluginParser:
    def __init__(self: Self, plugins_payload: dict) -> None:
        self.plugins_payload = plugins_payload

    @staticmethod
    def get_all_files(paths: list[str]) -> set[str]:
        files = set()
        for path in paths:
            if os.path.isdir(path):  # noqa: PTH112
                for filename in os.listdir(path):
                    if filename.endswith(".py"):
                        full_path = Path(path) / filename
                        files.add(str(full_path))
            elif path.endswith(".py"):
                files.add(path)

        return files

    def fetch_custom_plugin_files(self: Self) -> set[str]:
        custom_payload = self.plugins_payload.get("custom", {})
        if not custom_payload:
            logger.debug("No custom plugins found in the YAML.")
            return set()

        # Gather files from dirs and individual files
        files_from_dir = self.get_all_files(custom_payload.get("dirs", []))
        files = self.get_all_files(custom_payload.get("files", []))

        # Combine both sets of files
        return files_from_dir.union(files)

    def fetch_community_plugin_modules(self: Self) -> set[str]:
        comm_payload = self.plugins_payload.get("community", {})
        if not comm_payload:
            logger.debug("No community plugins found in the YAML.")
            return set()

        base_module = "community.plugins."

        return {base_module + plugin for plugin in comm_payload}


class PipelineParser:
    def parse_pipelines(self: Self, pipelines_data: dict[str, dict[str, Any]]) -> list[Pipeline]:
        if not pipelines_data:
            raise ValueError("No Pipelines detected.")

        return [
            self.create_pipeline(pipeline_name, pipeline_data)
            for pipeline_name, pipeline_data in pipelines_data.items()
        ]

    def create_pipeline(self: Self, pipeline_name: str, pipeline_data: dict[str, Any]) -> Pipeline:
        """Parse a single pipeline's data and return a pipeline instance."""
        if not pipeline_data:
            raise ValueError("Pipeline attributes are empty")

        phases = {}

        if "phases" not in pipeline_data:
            raise ValueError("The argument `phases` in pipelines must be specified.")
        for phase_name, phase_details in pipeline_data["phases"].items():
            phases[phase_name] = self.create_phase(phase_name, phase_details)

        pipeline_data["phases"] = phases
        return Pipeline(name=pipeline_name, **pipeline_data)

    def create_phase(self: Self, phase_name: str, phase_data: dict[str, Any]) -> PhaseInstance:
        phase_pipeline: PipelinePhase = PipelinePhase(phase_name)
        phase_class = PHASE_CLASS_MAP[phase_pipeline]

        return phase_class(**phase_data)
