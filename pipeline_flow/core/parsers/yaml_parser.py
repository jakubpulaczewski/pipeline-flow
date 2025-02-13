from __future__ import annotations

import os
import re
from enum import StrEnum
from typing import TYPE_CHECKING, Match, Self

import aiofiles
import yaml
import yamlcore
from pydantic.dataclasses import dataclass

if TYPE_CHECKING:
    from yaml.nodes import Node

    from pipeline_flow.common.type_def import PluginRegistryJSON

from pipeline_flow.common.utils import SingletonMeta

type JSON_DATA = dict

DEFAULT_CONCURRENCY = 2
DEFAULT_ENGINE = "native"


class YamlAttribute(StrEnum):
    PIPELINES = "pipelines"
    PLUGINS = "plugins"
    ENGINE = "engine"
    CONCURRENCY = "concurrency"


@dataclass(frozen=True)
class YamlConfig(metaclass=SingletonMeta):
    engine: str = DEFAULT_ENGINE
    concurrency: int = DEFAULT_CONCURRENCY


# Pattern for environment variables, e.g. ${{ env.HOME }}
ENV_VAR_YAML_TAG = "!env_var"
ENV_VAR_PATTERN = re.compile(r"\${{\s*env\.([^}]+?)\s*}}")


class ExtendedCoreLoader(yamlcore.CCoreLoader):  # CCoreLoader
    """Extends yamlcore.CoreLoader, an extension of YAML 1.2 Compliant.

    Fixes issues wihth parsing bool_values like `on` or `off`.
    """

    ENV_PREFIX = "env."

    def parse_env_var_name(self: Self, match: Match[str]) -> str:
        """Extracts the environment variable name from a regex match.

        The expected format is: ${{ env.ENV_VAR_NAME }}.

        Args:
            match (Match[str]): A regex match containing the env variable reference.

        Returns:
            str: Extracted environment variable name.
        """
        # Parsed String: 8 because `${{ .env` format and -3 because ``}}.`
        return match.group()[8:-3]

    def env_var_parser(self: Self, node: Node) -> str:
        """Parses a YAML node for an env var reference and replaces it with the value.

        Parses a YAML node for an environment variable reference and replaces it
        with the value of the environment variable concatenated with the remainder
        of the original node value.

        Args:
            node (Node): A YAML node containing the `!env_var` reference added by implicit resolver.

        Raises:
            ValueError: If the environment variable is not set.

        Returns:
            str: A String with the environment variable's value.
        """
        value = node.value

        match = ENV_VAR_PATTERN.match(value)

        env_var = self.parse_env_var_name(match)
        env_var_value = os.environ.get(env_var, None)
        if not env_var_value:
            error_msg = f"Environment variable `{env_var}` is not set."
            raise ValueError(error_msg)
        return env_var_value + value[match.end() :]


ExtendedCoreLoader.add_implicit_resolver(ENV_VAR_YAML_TAG, ENV_VAR_PATTERN, None)
ExtendedCoreLoader.add_constructor(ENV_VAR_YAML_TAG, ExtendedCoreLoader.env_var_parser)


@dataclass(frozen=True)
class YamlParser:
    content: dict

    @classmethod
    def from_text(cls, yaml_text: str) -> YamlParser:
        return cls(yaml.load(yaml_text, Loader=ExtendedCoreLoader))  # noqa: S506 - Extension of PyYAML YAML 1.2 Compliant

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
