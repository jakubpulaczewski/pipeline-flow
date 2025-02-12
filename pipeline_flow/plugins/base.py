# Standard Imports
from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Self

# Third Party Imports
# Local Imports
from pipeline_flow.core.registry import PluginRegistry

if TYPE_CHECKING:
    from pipeline_flow.common.type_def import ExtractedData, ExtractMergedData, TransformedData, UnifiedExtractData


class AsyncAdapterMixin:
    """Mixin class to run sychronous code in a new event loop thread such that it can be awaited and does not block the main event loop."""

    async def async_wrap[**P](self: Self, func: Callable[..., Any], *args: P.args, **kwargs: P.kwargs) -> Any:
        """Wrap synchronous code in an async function."""
        return await asyncio.to_thread(func, *args, **kwargs)


class IPlugin:
    """Abstract base class for all plugins."""

    def __init_subclass__(
        cls,
        *,
        plugin_name: str | None = None,
        interface: bool = False,
    ) -> None:
        super().__init_subclass__()
        # If the class is an interface, do not register it.
        if interface:
            return

        if not plugin_name:
            raise ValueError("Plugin name must be provided for concrete classes.")

        PluginRegistry.register(plugin_name, cls)

    def __init__(self: Self, plugin_id: str) -> None:
        self.id = plugin_id


class IExtractPlugin(ABC, IPlugin, interface=True):
    """Abstract base class for extract plugins."""

    @abstractmethod
    async def __call__(self: Self) -> ExtractedData:
        """Asynchronously extract data."""
        raise NotImplementedError("Extract plugins must implement __call__()")


class IMergeExtractPlugin(ABC, IPlugin, interface=True):
    """Abstract base class for merge-extract plugins."""

    @abstractmethod
    def __call__(self: Self, extracted_data: dict[str, ExtractedData]) -> ExtractMergedData:
        """Merge multiple extracted data sources into a single merged format."""
        raise NotImplementedError("Merge-extract plugins must implement __call__()")


class ITransformPlugin(ABC, IPlugin, interface=True):
    """Abstract base class for transform plugins."""

    @abstractmethod
    def __call__(self: Self, data: UnifiedExtractData) -> TransformedData:
        """Transform the input data."""
        raise NotImplementedError("Transform plugins must implement __call__()")


class ILoadPlugin(ABC, IPlugin, interface=True):
    """Abstract base class for load plugins."""

    @abstractmethod
    async def __call__(self: Self, data: UnifiedExtractData | TransformedData) -> None:
        """Asynchronously load data."""
        raise NotImplementedError("Load plugins must implement __call__()")


class ITransformLoadPlugin(ABC, IPlugin, interface=True):
    """Abstract base class for transform-load plugins."""

    @abstractmethod
    def __call__(self: Self) -> None:
        """Sychronously transform data at the destination."""
        raise NotImplementedError("Transform-load plugins must implement __call__()")


class IAuthPlugin(ABC, IPlugin, interface=True):
    """Abstract base class for authentication plugins."""

    @abstractmethod
    def __call__(self: Self, secret_name: str) -> str:
        """Retrieve a secret from a secret store."""
        raise NotImplementedError("Auth plugins must implement __call__()")


class IPreProcessPlugin(ABC, IPlugin, interface=True):
    """Abstract base class for pre-processing plugins."""

    @abstractmethod
    async def __call__(self: Self) -> None:
        """Pre-process data before main plugin execution."""
        raise NotImplementedError("Pre-process plugins must implement __call__()")


class IPostProcessPlugin(ABC, IPlugin, interface=True):
    """Abstract base class for post-processing plugins."""

    @abstractmethod
    async def __call__(self: Self) -> None:
        """Post-process data after main plugin execution."""
        raise NotImplementedError("Post-process plugins must implement __call__()")
