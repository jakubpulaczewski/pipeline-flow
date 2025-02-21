# Standard Imports
from __future__ import annotations

from io import TextIOWrapper
from typing import Annotated, Any, TypedDict

# Third Party Imports
from pydantic.dataclasses import dataclass

type ExtractedData = Any
type ExtractMergedData = Any
type TransformedData = Any
type UnifiedExtractData = ExtractedData | ExtractMergedData
type ETLData = UnifiedExtractData | TransformedData


type PluginName = str
type StreamType = str | bytes | TextIOWrapper


class PluginPayload(TypedDict):
    plugin_id: str
    plugin: PluginName
    args: dict[str, Any]


class CustomPluginRegistryJSON(TypedDict):
    dirs: Annotated[list[str], "List of directories to dynamically import for custom plugins"]
    files: Annotated[list[str], "List of files to dynamically import for custom plugins"]


class PluginRegistryJSON(TypedDict):
    custom: CustomPluginRegistryJSON
    community: Annotated[list[str], "List of community plugins to import"]


@dataclass
class ExtractStageResult:
    id: str
    success: bool
    data: ExtractedData
    error: str | None = None


@dataclass
class TransformStageResult:
    id: str
    success: bool
    data: TransformedData
    error: str | None = None


@dataclass
class LoadStageResult:
    id: str
    success: bool
    error: str | None = None


@dataclass
class TransformLoadStageResult:
    id: str
    success: bool
    error: str | None = None
