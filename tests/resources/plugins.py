# Standard Imports
import asyncio
import time
from typing import Self

# Third-party Imports
# Project Imports
from pipeline_flow.plugins import (
    IExtractPlugin,
    ILoadPlugin,
    IMergeExtractPlugin,
    IPlugin,
    IPreProcessPlugin,
    ITransformLoadPlugin,
    ITransformPlugin,
)


class SimpleDummyPlugin(IPlugin, plugin_name="simple_dummy_plugin"):
    def __call__(self: Self) -> str:
        return "simple_dummy_plugin"


class SimpleExtractorPlugin(IExtractPlugin, plugin_name="simple_extractor_plugin"):
    def __init__(self: Self, plugin_id: str, delay: float = 0) -> None:
        super().__init__(plugin_id)
        self.delay = delay

    async def __call__(self) -> str:
        await asyncio.sleep(self.delay)
        return "extracted_data"


class SimpleMergePlugin(IMergeExtractPlugin, plugin_name="simple_merge_plugin"):
    def __call__(self: Self, extracted_data: dict) -> str:  # noqa: ARG002
        return "merged_data"


class SimpleTransformPlugin(ITransformPlugin, plugin_name="simple_transform_plugin"):
    def __init__(self: Self, plugin_id: str, delay: float = 0) -> None:
        super().__init__(plugin_id)
        self.delay = delay

    def __call__(self: Self, data: str) -> str:  # noqa: ARG002
        time.sleep(self.delay)
        return f"transformed_{data}"


class SimpleLoaderPlugin(ILoadPlugin, plugin_name="simple_loader_plugin"):
    def __init__(self: Self, plugin_id: str, delay: float = 0) -> None:
        super().__init__(plugin_id)
        self.delay = delay

    async def __call__(self: Self, data: str) -> None:  # noqa: ARG002
        await asyncio.sleep(self.delay)  # Simulating a slow loader


class SimpleTransformLoadPlugin(ITransformLoadPlugin, plugin_name="simple_transform_load_plugin"):
    def __init__(self: Self, plugin_id: str, query: str, delay: float = 0) -> None:
        super().__init__(plugin_id)
        self.delay = delay
        self.query = query

    def __call__(self: Self) -> str:
        time.sleep(self.delay)  # Stimulating a transformation at load phase where data is transformed on


class SimpleAsyncPrePlugin(IPreProcessPlugin, plugin_name="simple_async_pre_plugin"):
    def __init__(self: Self, plugin_id: str, delay: float = 0) -> None:
        super().__init__(plugin_id)
        self.delay = delay

    async def __call__(self: Self) -> str:
        await asyncio.sleep(self.delay)  # Stimulate some delay.
        return "Async result"
