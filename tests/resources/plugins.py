# Standard Imports
import asyncio
import time
from typing import Self

# Third-party Imports
# Project Imports
from pipeline_flow.core.registry import PluginRegistry
from pipeline_flow.plugins import (
    IExtractPlugin,
    ILoadPlugin,
    IMergeExtractPlugin,
    IPlugin,
    IPreProcessPlugin,
    ISecretManager,
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

    def __call__(self: Self, data: str) -> str:
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


class SimpleSecretPlugin(ISecretManager, plugin_name="simple_secret_plugin"):
    def __init__(self: Self, plugin_id: str, secret_name: str, region: str) -> None:
        super().__init__(plugin_id)
        self.secret_name = secret_name
        self.region = region

    def __call__(self: Self) -> str:
        return "super_secret_value"


class NestedSecretPlugin(ISecretManager, plugin_name="nested_secret_plugin"):
    def __init__(self: Self, plugin_id: str, secret_name: str) -> None:
        super().__init__(plugin_id)
        self.secret_name = secret_name

    def __call__(self: Self) -> str:
        return {"user": "secret_user", "password": "secret_password"}


class NestedPlugin(IPlugin, plugin_name="extended_plugin"):
    def __init__(self, plugin_id: str, arg1: str, arg2: str) -> None:
        super().__init__(plugin_id)
        self.arg1 = arg1
        self.arg2 = arg2

    def __call__(self) -> str:
        return "extended_plugin"


class SimplePluginWithNestedPlugin(IPlugin, plugin_name="simple_plugin_with_nested_plugin"):
    def __init__(self, plugin_id: str, pagination: IPlugin | None = None) -> None:
        super().__init__(plugin_id)

        pagination_payload = pagination or {
            "id": f"{plugin_id}_default_pagination",
            "plugin": "extended_plugin",
            "args": {
                "arg1": "default_value1",
                "arg2": "default_value2",
            },
        }

        self.pagination = PluginRegistry.instantiate_plugin(pagination_payload)

    def __call__(self) -> str:
        return "simple_dummy_plugin"
