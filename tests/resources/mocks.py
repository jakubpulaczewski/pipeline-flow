# Standard Imports
import asyncio
import time

from functools import wraps

# Third-party Imports
import pytest

from common.type_def import ExtractedData, TransformedData

# Project Imports


# TODO: Verify the claim of using ExtractedData and so on....
def mock_merger():
    @wraps(mock_merger)
    def inner(extracted_data: dict[str, ExtractedData]):
        return "merged_data"
    return inner


def mock_extractor(id: str):
    @wraps(mock_extractor)
    async def inner() -> ExtractedData:
        return "extracted_data"
    return inner

def mock_async_extractor(id: str, delay: float=0.2):
    @wraps(mock_async_extractor)
    async def inner() -> ExtractedData:
        await asyncio.sleep(delay)
        return "async_extracted_data"
    return inner

def mock_transformer(id: str):
    @wraps(mock_transformer)
    def inner(data: ExtractedData) -> TransformedData:
        return "transformed_etl_data"
    return inner

def mock_sync_transformer(id:str, delay: float) -> TransformedData:
    @wraps(mock_sync_transformer)
    def inner(data: ExtractedData):
        time.sleep(delay)
        return "TF" + data
    return inner


def mock_loader(id: str):
    @wraps(mock_loader)
    async def inner(data: ExtractedData | TransformedData) -> None:
        return None
    return inner

def mock_async_loader(id: str, delay: float):
    @wraps(mock_async_loader)
    async def inner(data: ExtractedData | TransformedData) -> None:
        await asyncio.sleep(delay)
        return None
    return inner


def mock_load_transformer(id: str, query: str) -> None:
    @wraps(mock_load_transformer)
    def inner() -> None:
        return None

    return inner

def mock_sync_load_transformer(id: str, delay: float=0):
    @wraps(mock_sync_load_transformer)
    def inner() -> None:
        time.sleep(delay)
        return None
    return inner
