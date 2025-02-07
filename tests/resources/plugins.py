# Standard Imports
import asyncio
import time
from functools import wraps

# Third-party Imports
# Project Imports
from pipeline_flow.common.type_def import AsyncPlugin, SyncPlugin


def simple_dummy_plugin() -> SyncPlugin:
    @wraps(simple_dummy_plugin)
    def inner() -> str:
        return "simple_dummy_plugin"

    return inner


def simple_extractor_plugin(delay: float = 0) -> AsyncPlugin:
    @wraps(simple_extractor_plugin)
    async def inner() -> str:
        await asyncio.sleep(delay)
        return "extracted_data"

    return inner


def simple_merge_plugin() -> SyncPlugin:
    @wraps(simple_merge_plugin)
    def inner(extracted_data: str) -> str:  # noqa: ARG001
        return "merged_data"

    return inner


def simple_transform_plugin(delay: float = 0) -> SyncPlugin:
    @wraps(simple_transform_plugin)
    def inner(data: str) -> str:
        time.sleep(delay)
        return f"transformed_{data}"

    return inner


def simple_loader_plugin(delay: float = 0) -> AsyncPlugin:
    @wraps(simple_loader_plugin)
    async def inner(data: str) -> None:  # noqa: ARG001
        await asyncio.sleep(delay)  # Simulating a slow loader
        return  # noqa: PLR1711

    return inner


def simple_transform_load_plugin(query: str, delay: float = 0) -> SyncPlugin:  # noqa: ARG001
    @wraps(simple_transform_load_plugin)
    def inner() -> None:
        time.sleep(delay)  # Stimulating a transformation at load phase where data is transformed on
        return  # noqa: PLR1711

    return inner


def async_pre(delay: float = 0) -> AsyncPlugin:
    async def inner() -> str:
        await asyncio.sleep(delay)
        return "Async result"

    return inner
