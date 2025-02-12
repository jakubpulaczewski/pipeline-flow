# Standard Imports
from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, ParamSpec, Self

# Third Party Imports
# Local Imports
from pipeline_flow.core.registry import PluginRegistry

if TYPE_CHECKING:
    from collections.abc import Callable

    from pipeline_flow.common.type_def import ExtractedData, ExtractMergedData, TransformedData, UnifiedExtractData

_P = ParamSpec("_P")

_PluginReturn = Any


class AsyncAdapterMixin:
    """Mixin class to run sychronous code in a new event loop thread.

    This mixin allows running synchronous code in a separate thread using
    asyncio.to_thread, so it can be awaited without blocking the main event loop.
    It is useful for wrapping blocking I/O operations (e.g. using `pandas`) in an
    async function.
    """

    async def async_wrap(
        self: Self, func: Callable[..., _PluginReturn], *args: _P.args, **kwargs: _P.kwargs
    ) -> _PluginReturn:
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

        # Register the plugin with the plugin registry.
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
