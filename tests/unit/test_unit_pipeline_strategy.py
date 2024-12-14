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
from core.models.phase_wrappers import (
    ExtractResult,
    TransformResult,
    LoadResult
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
    extract = ExtractPhase(steps=[extractor_mock])
    result = await PipelineStrategy.run_extractor(extract)

    assert result == {
        'extractor_id': ExtractResult(
            name='extractor_id', 
            success=True, 
            result='extracted_data',
            error=None
        )
    }

@pytest.mark.asyncio
async def test_run_extractor_multiple(extractor_mock, second_extractor_mock) -> None:
    extracts = ExtractPhase(
        steps=[extractor_mock, second_extractor_mock]
    )

    result = await PipelineStrategy.run_extractor(extracts)

    assert result == {
        'extractor_id': ExtractResult(
            name='extractor_id', 
            success=True, 
            result='extracted_data', 
            error=None
        ),
        'extractor_id_2': ExtractResult(
            name='extractor_id_2', 
            success=True, 
            result='extracted_data', 
            error=None
        )
    }


def test_run_transformer(mock_transformer) -> None:
    transformations = TransformPhase(steps=[mock_transformer])

    result = PipelineStrategy.run_transformer("DATA", transformations)

    assert result == TransformResult(
        name="transformer_id",
        success=True,
        result="transformed_etl_data",
        error=None,
    )

def test_run_transformer_multiple() -> None:
    data = "initial_data"
    transformations = TransformPhase(
        steps=[
            MockTransformAddSuffix(id="add_suffix"),
            MockTransformToUpper(id="to_upper"),
        ]
    )

    result = PipelineStrategy.run_transformer(data, transformations)

    assert result == TransformResult(
        name='to_upper', 
        success=True, 
        result='INITIAL_DATA_SUFFIX', 
        error=None
    )


@pytest.mark.asyncio
async def test_run_loader(mock_loader) -> None:
    data = "INITIAL_DATA_SUFFIX"
    destinations = LoadPhase(
        steps=[
            mock_loader
        ]
    )
    result = await PipelineStrategy.run_loader(data, destinations)

    assert result == [
        LoadResult(name='loader_id', success=True, error=None)
    ]

@pytest.mark.asyncio
async def test_run_loader_multiple(mock_loader, second_mock_loader) -> None:
    data = "INITIAL_DATA_SUFFIX"
    destinations = LoadPhase(
        steps=[
            mock_loader,
            second_mock_loader
        ]
    )
    result = await PipelineStrategy.run_loader(data, destinations)
    assert result == [
        LoadResult(name='loader_id', success=True, error=None),
        LoadResult(name='loader_id_2', success=True, error=None)
    ]


def test_run_transformer_after_load(mock_load_transformer) -> None:
    transformations = TransformLoadPhase(steps= [
        mock_load_transformer
    ])
    result = PipelineStrategy.run_transformer_after_load(transformations)
    
    assert result == [
        TransformResult(name='mock_transform_load_id', success=True, result=None, error=None)
    ]


def test_run_transformer_after_load_multiple(mock_load_transformer, second_mock_load_transformer) -> None:
    transformations = TransformLoadPhase(steps= [
        mock_load_transformer,
        second_mock_load_transformer
    ])
    result = PipelineStrategy.run_transformer_after_load(transformations)
    
    assert result == [
        TransformResult(name='mock_transform_load_id', success=True, result=None, error=None),
        TransformResult(name='mock_transform_load_id_2', success=True, result=None, error=None)
    ]


