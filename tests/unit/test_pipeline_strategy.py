# Standard Imports

# Third-party Imports
import pytest

# Project Imports
from core.models.phases import (
    ExtractPhase,
    LoadPhase,
    TransformLoadPhase,
    TransformPhase,
)
from core.models.pipeline import Pipeline, PipelineType
from core.pipeline_strategy import (
    ETLStrategy,
    ELTStrategy,
    ETLTStrategy,
    PipelineStrategy,
    PipelineStrategyFactory
)

from tests.common.mocks import (
    MockExtractor, 
    MockLoad, 
    MockTransform, 
    MockLoadTransform
)
from core.models.extract import ExtractResult
from core.models.transform import  TransformResult
from core.models.load import  LoadResult


@pytest.mark.parametrize("pipeline_type, expected_strategy", [
    (PipelineType('ETL'), ETLStrategy),
    (PipelineType('ELT'), ELTStrategy),
    (PipelineType('ETLT'), ETLTStrategy)
])
def test_factory_pipeline(pipeline_type, expected_strategy) -> None:
    strategy = PipelineStrategyFactory.get_pipeline_strategy(pipeline_type)

    assert strategy == expected_strategy


# @pytest.mark.asyncio
# async def test_run_extractor(extractor_mock) -> None:
#     extract = ExtractPhase(steps=[extractor_mock])
#     result = await PipelineStrategy.run_extractor(extract)

#     assert result == {
#         'mock_extractor': ExtractResult(
#             name='extractor_id', 
#             success=True, 
#             result='extracted_data',
#             error=None
#         )
#     }
