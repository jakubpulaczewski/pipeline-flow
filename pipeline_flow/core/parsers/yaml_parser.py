from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, Self

import aiofiles
import yaml
import yamlcore
from pydantic.dataclasses import dataclass

if TYPE_CHECKING:
    from pipeline_flow.common.type_def import PluginRegistryJSON

from pipeline_flow.common.utils import SingletonMeta

type JSON_DATA = dict

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


@dataclass
class YamlParser:
    content: dict

    @classmethod
    def from_text(cls, yaml_text: str) -> YamlParser:
        return cls(yaml.load(yaml_text, Loader=yamlcore.CoreLoader))  # noqa: S506 - Extension of PyYAML YAML 1.2 Compliant

    @classmethod
    async def from_file(cls, file_path: str, encoding: str = "utf-8") -> YamlParser:
        """Create YamlDocument from file."""
        try:
            async with aiofiles.open(file_path, encoding=encoding) as file:
                content = await file.read()
                return cls.from_text(content)

        except FileNotFoundError as error:
            error_msg = f"File not found: {file_path}"
            raise FileNotFoundError(error_msg) from error

    def get_pipelines_dict(self: Self) -> JSON_DATA:
        """Return the 'pipelines' section from the parsed YAML."""
        return self.content.get(YamlAttribute.PIPELINES.value, {})

    def get_plugins_dict(self: Self) -> PluginRegistryJSON | None:
        return self.content.get(YamlAttribute.PLUGINS.value, None)

    def initialize_yaml_config(self: Self) -> YamlConfig:
        # Create the map of attributes with their values
        attrs_map = {
            YamlAttribute.ENGINE.value: self.content.get(YamlAttribute.ENGINE.value, DEFAULT_ENGINE),
            YamlAttribute.CONCURRENCY.value: self.content.get(YamlAttribute.CONCURRENCY.value, DEFAULT_CONCURRENCY),
        }

        # Filter out the None values
        attrs = {key: value for key, value in attrs_map.items() if value is not None}

        return YamlConfig(**attrs)
