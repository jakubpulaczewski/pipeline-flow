# Standard Imports
import asyncio
import time
from functools import wraps

# Third-party Imports
# Project Imports
from common.type_def import AsyncPlugin, SyncPlugin


# TODO: Verify the claim of using ExtractedData and so on....
def mock_merger() -> SyncPlugin:
    @wraps(mock_merger)
    def inner(extracted_data: str) -> str:
        return "merged_data"

    return inner


def mock_extractor(id: str) -> AsyncPlugin:
    @wraps(mock_extractor)
    async def inner() -> str:
        return "extracted_data"

    return inner


def mock_async_extractor(id: str, delay: float = 0) -> AsyncPlugin:
    @wraps(mock_async_extractor)
    async def inner() -> str:
        await asyncio.sleep(delay)
        return "async_extracted_data"

    return inner


def mock_transformer(id: str) -> SyncPlugin:
    @wraps(mock_transformer)
    def inner(data: str) -> str:
        return "transformed_etl_data"

    return inner


def mock_sync_transformer(id: str, delay: float) -> SyncPlugin:
    @wraps(mock_sync_transformer)
    def inner(data: str) -> str:
        time.sleep(delay)
        return "TF" + data

    return inner


def mock_loader(id: str) -> AsyncPlugin:
    @wraps(mock_loader)
    async def inner(data: str) -> None:
        return

    return inner


def mock_async_loader(id: str, delay: float) -> AsyncPlugin:
    @wraps(mock_async_loader)
    async def inner(data: str) -> None:
        await asyncio.sleep(delay)

    return inner


def mock_load_transformer(id: str, query: str) -> SyncPlugin:
    @wraps(mock_load_transformer)
    def inner() -> None:
        return

    return inner


def mock_sync_load_transformer(id: str, delay: float) -> SyncPlugin:
    @wraps(mock_sync_load_transformer)
    def inner() -> None:
        time.sleep(delay)

    return inner
