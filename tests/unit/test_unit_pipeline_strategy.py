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
from core.models.pipeline import PipelineType
from core.pipeline_strategy import (
    ETLStrategy,
    ELTStrategy,
    ETLTStrategy,
    PipelineStrategy,
    PipelineStrategyFactory
)

from tests.resources.mocks import (
    MockTransformAddSuffix,
    MockTransformToUpper
)



@pytest.mark.parametrize("pipeline_type, expected_strategy", [
    (PipelineType('ETL'), ETLStrategy),
    (PipelineType('ELT'), ELTStrategy),
    (PipelineType('ETLT'), ETLTStrategy)
])
def test_factory_pipeline(pipeline_type, expected_strategy) -> None:
    strategy = PipelineStrategyFactory.get_pipeline_strategy(pipeline_type)

    assert isinstance(strategy, expected_strategy) 


@pytest.mark.asyncio
async def test_run_extractor(extractor_mock) -> None:
    extract = ExtractPhase.model_construct(steps=[extractor_mock])
    result = await PipelineStrategy.run_extractor(extract)

    assert result == 'extracted_data'



@pytest.mark.asyncio
async def test_run_extractor_multiple(extractor_mock, second_extractor_mock, merger_mock) -> None:
    extracts = ExtractPhase.model_construct(
        steps=[extractor_mock, second_extractor_mock],
        merge=merger_mock
    )

    result = await PipelineStrategy.run_extractor(extracts)

    assert result == 'merged_data'


def test_run_transformer(mock_transformer) -> None:
    transformations = TransformPhase.model_construct(steps=[mock_transformer])

    result = PipelineStrategy.run_transformer("DATA", transformations)

    assert result == "transformed_etl_data"


def test_run_transformer_multiple() -> None:
    data = "initial_data"
    transformations = TransformPhase.model_construct(
        steps=[
            MockTransformAddSuffix(id="add_suffix"),
            MockTransformToUpper(id="to_upper"),
        ]
    )

    result = PipelineStrategy.run_transformer(data, transformations)

    assert result == 'INITIAL_DATA_SUFFIX'


@pytest.mark.asyncio
async def test_run_loader(mock_loader) -> None:
    data = "INITIAL_DATA_SUFFIX"
    destinations = LoadPhase.model_construct(
        steps=[
            mock_loader
        ]
    )
    result = await PipelineStrategy.run_loader(data, destinations)

    assert result == [{'id': 'loader_id', 'success': True}]

@pytest.mark.asyncio
async def test_run_loader_multiple(mock_loader, second_mock_loader) -> None:
    data = "INITIAL_DATA_SUFFIX"
    destinations = LoadPhase.model_construct(
        steps=[
            mock_loader,
            second_mock_loader
        ]
    )
    result = await PipelineStrategy.run_loader(data, destinations)
    assert result == [
        {'id': 'loader_id', 'success': True},
        {'id': 'loader_id_2', 'success': True}
    ]


def test_run_transformer_after_load(mock_load_transformer) -> None:
    transformations = TransformLoadPhase.model_construct(steps= [
        mock_load_transformer
    ])
    result = PipelineStrategy.run_transformer_after_load(transformations)
    
    assert result == [
        {'id': 'mock_transform_load_id', 'success': True},
    ]


def test_run_transformer_after_load_multiple(mock_load_transformer, second_mock_load_transformer) -> None:
    transformations = TransformLoadPhase.model_construct(steps= [
        mock_load_transformer,
        second_mock_load_transformer
    ])
    result = PipelineStrategy.run_transformer_after_load(transformations)
    
    assert result == [
        {'id': 'mock_transform_load_id', 'success': True},
        {'id': 'mock_transform_load_id_2', 'success': True}
    ]


