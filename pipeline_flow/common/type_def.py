from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Annotated, Any, TypedDict

from pydantic.dataclasses import dataclass

type ExtractedData = Any
type TransformedData = Any

type ETLData = ExtractedData | TransformedData
type PluginName = str

type SyncPlugin = Callable[..., ETLData]
type AsyncPlugin = Callable[..., Awaitable[ETLData]]
type WrappedPlugin[**PluginArgs] = Callable[PluginArgs, SyncPlugin] | Callable[PluginArgs, AsyncPlugin]


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
