# Standard Imports
from __future__ import annotations

from typing import Any, Awaitable, Callable, TypeVar

# Third-party imports

# Project Imports

# Data returns types from each ETL phase
ExtractedData = TypeVar("ExtractedData", bound=Any)
TransformedData = TypeVar("TransformedData", bound=Any)
LoadedData = TypeVar("LoadedData", bound=Any)
TransformLoadedData = TypeVar("TransformLoadedData", bound=Any)

type Data = ExtractedData | TransformedData | LoadedData | TransformLoadedData
type PluginName = str


PluginOutput = TypeVar("PluginOutput")  # Represents the return type of the plugin function

type Plugin[**PluginArgs] = (
    Callable[PluginArgs, Callable[..., PluginOutput]] | Callable[PluginArgs, Callable[..., Awaitable[PluginOutput]]]
)
