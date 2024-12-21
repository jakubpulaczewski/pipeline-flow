# Standard Imports
from functools import wraps


# Third-party Imports
import pytest

from common.type_def import ExtractedData, TransformedData

# Project Imports
from core.models.phases import(
    IExtractor,
    ILoader,
    ILoadTransform, 
    ITransform,
    iMerger
)

class MockMerger(iMerger):

    def merge_data(self, extracted_data: dict[str, ExtractedData]):
        return "merged_data"

class MockExtractor(IExtractor):

    async def extract_data(self) -> ExtractedData:
        return "extracted_data"


class MockTransform(ITransform):

    def transform_data(self, data: ExtractedData) -> TransformedData:
        return "transformed_etl_data"
    

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


class MockLoad(ILoader):

    async def load_data(self, data: ExtractedData | TransformedData) -> None:
        return None
