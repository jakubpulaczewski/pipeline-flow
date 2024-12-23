# Standard Imports
import asyncio
import time

from functools import wraps

# Third-party Imports
import pytest

from common.type_def import ExtractedData, TransformedData

# Project Imports
from core.models.pipeline import Pipeline
from core.models.phases import(
    IExtractor,
    ILoader,
    ILoadTransform, 
    ITransform,
    iMerger
)


class MockStrategy:
 
    def __init__(self, execution_time: float):
        self.execution_time = execution_time
    
    async def execute(self, pipeline: Pipeline) -> bool:
        await asyncio.sleep(self.execution_time)
        return True

class MockMerger(iMerger):

    def merge_data(self, extracted_data: dict[str, ExtractedData]):
        return "merged_data"

class MockExtractor(IExtractor):

    async def extract_data(self) -> ExtractedData:
        return "extracted_data"

class MockAwaitExtractor(IExtractor):
    delay: float

    async def extract_data(self) -> ExtractedData:
        await asyncio.sleep(self.delay)
        return "extracted_data"

class MockTransform(ITransform):

    def transform_data(self, data: ExtractedData) -> TransformedData:
        return "transformed_etl_data"


class MockAwaitTransformer(ITransform):
    delay: float

    def transform_data(self, data: ExtractedData) -> TransformedData:
        time.sleep(self.delay)
        return "TF" + data
    
class MockTransformAddSuffix(ITransform):
    def transform_data(self, data: ExtractedData) -> TransformedData:
        return f"{data}_suffix"

class MockTransformToUpper(ITransform):
    def transform_data(self, data: ExtractedData) -> TransformedData:
        return data.upper()


def upper_case_and_suffix_transform(id: str):
    @wraps(upper_case_and_suffix_transform)
    def inner(data: ExtractedData) -> str:
        return data.upper() + "transformed_suffix"
    return inner

class MockLoadTransform(ILoadTransform):
    def transform_data(self) -> None:
        return None

class MockAwaitLoadTransformer(ILoadTransform):
    delay: float

    def transform_data(self) -> None:
        time.sleep(self.delay)
        return None

class MockLoad(ILoader):

    async def load_data(self, data: ExtractedData | TransformedData) -> None:
        return None

class MockAwaitLoader(ILoader):
    delay: float = 0.3

    async def load_data(self, data: ExtractedData | TransformedData) -> None:
        await asyncio.sleep(self.delay)
        return None
