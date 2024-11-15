# Standard Imports


# Third-party Imports
import pytest

from common.type_def import ExtractedData, TransformedData

# Project Imports
from core.models.extract import IExtractor, extract_decorator
from core.models.load import ILoader, load_decorator
from core.models.transform import ITransformerELT, ITransformerETL, transform_decorator


class MockExtractor(IExtractor):

    @extract_decorator
    async def extract_data(self) -> ExtractedData:
        return "extracted_data"


class MockTransformETL(ITransformerETL):

    @transform_decorator
    def transform_data(self, data: ExtractedData) -> TransformedData:
        return "transformed_etl_data"


class MockTransformELT(ITransformerELT):

    @transform_decorator
    def transform_data(self) -> None:
        return


class MockLoad(ILoader):

    @load_decorator
    async def load_data(self, data: ExtractedData | TransformedData) -> None:
        return
