# Standard Imports


# Third-party Imports
import pytest

from common.type_def import ExtractedData, TransformedData

# Project Imports
from core.models.extract import IExtractor
from core.models.load import ILoader
from core.models.transform import ILoadTransform, ITransform


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


class MockLoadTransform(ILoadTransform):

    def transform_data(self) -> None:
        return None


class MockLoad(ILoader):

    async def load_data(self, data: ExtractedData | TransformedData) -> None:
        return None
