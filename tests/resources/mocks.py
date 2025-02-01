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


def mock_extractor() -> AsyncPlugin:
    @wraps(mock_extractor)
    async def inner() -> str:
        return "extracted_data"

    return inner


def mock_async_extractor(delay: float = 0) -> AsyncPlugin:
    @wraps(mock_async_extractor)
    async def inner() -> str:
        await asyncio.sleep(delay)
        return "async_extracted_data"

    return inner


def mock_transformer() -> SyncPlugin:
    @wraps(mock_transformer)
    def inner(data: str) -> str:
        return "transformed_etl_data"

    return inner


def mock_sync_transformer(delay: float) -> SyncPlugin:
    @wraps(mock_sync_transformer)
    def inner(data: str) -> str:
        time.sleep(delay)
        return "TF" + data

    return inner


def mock_loader() -> AsyncPlugin:
    @wraps(mock_loader)
    async def inner(data: str) -> None:
        return

    return inner


def mock_async_loader(delay: float) -> AsyncPlugin:
    @wraps(mock_async_loader)
    async def inner(data: str) -> None:
        await asyncio.sleep(delay)

    return inner


def mock_load_transformer(query: str) -> SyncPlugin:
    @wraps(mock_load_transformer)
    def inner() -> None:
        return

    return inner


def mock_sync_load_transformer(delay: float) -> SyncPlugin:
    @wraps(mock_sync_load_transformer)
    def inner() -> None:
        time.sleep(delay)

    return inner
