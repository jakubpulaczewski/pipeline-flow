# Standard Imports
import asyncio
import time

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
from core.models.phases import PipelinePhase
from core.pipeline_strategy import (
    ETLStrategy,
    ELTStrategy,
    ETLTStrategy,
    PipelineStrategy,
    PipelineStrategyFactory
)

from tests.resources.mocks import (
    MockTransformAddSuffix,
    MockTransformToUpper,
    MockAwaitExtractor,
    MockAwaitLoader,
    MockAwaitTransformer,
    MockAwaitLoadTransformer
)

async def async_pre_1():
    await asyncio.sleep(0.2)
    return "Async 1 result"

async def async_pre_2():
    await asyncio.sleep(0.4)
    return "Async 2 result"

def sync_pre_1():
    time.sleep(0.5)
    return "Sync 1 result"

def sync_pre_2():
    time.sleep(0.3)
    return "Sync 2 result"



@pytest.mark.parametrize("pipeline_type, expected_strategy", [
    (PipelineType('ETL'), ETLStrategy),
    (PipelineType('ELT'), ELTStrategy),
    (PipelineType('ETLT'), ETLTStrategy)
])
def test_factory_pipeline(pipeline_type, expected_strategy) -> None:
    strategy = PipelineStrategyFactory.get_pipeline_strategy(pipeline_type)

    assert isinstance(strategy, expected_strategy) 


@pytest.mark.asyncio
async def test_run_extractor_without_delay(extractor_mock) -> None:
    extract = ExtractPhase.model_construct(steps=[extractor_mock])
    result = await PipelineStrategy.run_extractor(extract)

    assert result == 'extracted_data'



@pytest.mark.asyncio
async def test_run_extractor_multiple_without_delay(extractor_mock, second_extractor_mock, merger_mock) -> None:
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
async def test_run_loader_without_delay(mock_loader) -> None:
    data = "INITIAL_DATA_SUFFIX"
    destinations = LoadPhase.model_construct(
        steps=[
            mock_loader
        ]
    )
    result = await PipelineStrategy.run_loader(data, destinations)

    assert result == [{'id': 'loader_id', 'success': True}]

@pytest.mark.asyncio
async def test_run_loader_multiple_without_delay(mock_loader, second_mock_loader) -> None:
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


class TestUnitPipelineStrategyConcurrency:

    @staticmethod
    @pytest.mark.asyncio
    async def test_execute_processing_steps() -> None:
        start = asyncio.get_running_loop().time()
        result = await PipelineStrategy._execute_processing_steps(
            PipelinePhase.EXTRACT_PHASE,
            [async_pre_1, async_pre_2, sync_pre_1, sync_pre_2]
        )
        total = asyncio.get_running_loop().time() - start

        # Concurrency validation
        assert 0.6 > total > 0.5, "Delay Should be 0.5 seconds"
        assert result == ['Async 1 result', 'Async 2 result', 'Sync 1 result', 'Sync 2 result']


    @staticmethod
    @pytest.mark.asyncio
    async def test_run_extractor_with_pre_processing() -> None:
        # Set the side_effect function
        
        start = asyncio.get_running_loop().time()
        extract = ExtractPhase.model_construct(
            steps=[MockAwaitExtractor(id='await_extractor_id', delay=0.2)],
            pre=[async_pre_1, async_pre_2, sync_pre_1, sync_pre_2]
        )

        result = await PipelineStrategy.run_extractor(extract)
        total = asyncio.get_running_loop().time() - start
        # Concurrency validation
        assert 0.8 > total >= 0.7, "Delay Should be (0.2) Extract + (0.5) Pre processing "
        assert result == 'extracted_data'


    @staticmethod
    @pytest.mark.asyncio
    async def test_run_extractor_multiple_with_delay(merger_mock) -> None:
        extract = ExtractPhase.model_construct(
            steps=[
                MockAwaitExtractor(id='await_extractor_id', delay=0.2),
                MockAwaitExtractor(id='await_extractor_id_2', delay=0.3)
            ],
            merge=merger_mock
        )
        start = asyncio.get_running_loop().time()
        extracted_data = await PipelineStrategy.run_extractor(extract)
        total = asyncio.get_running_loop().time() - start

        assert 0.4 > total >= 0.3, "Delay Should be 0.3 second for asychronously executing two extracts"
        assert extracted_data == 'merged_data'

    @staticmethod
    @pytest.mark.asyncio
    async def test_run_loader_with_pre_processing() -> None:
        start = asyncio.get_running_loop().time()
        destinations = LoadPhase.model_construct(
            steps=[MockAwaitLoader(id='await_loader_id', delay=0.2)],
            pre=[async_pre_1, async_pre_2, sync_pre_1, sync_pre_2]
        )

        result = await PipelineStrategy.run_loader("DATA", destinations)
        total = asyncio.get_running_loop().time() - start

        # Concurrency validation
        assert 0.8 > total >= 0.7, "Delay Should be (0.2) Load + (0.5) Pre processing "
        assert result == [{'id': 'await_loader_id', 'success': True}]


    @staticmethod
    @pytest.mark.asyncio
    async def test_run_loader_multiple_with_delay() -> None:
        destinations = LoadPhase.model_construct(
            steps=[
                MockAwaitLoader(id='await_loader_id', delay=0.4),
                MockAwaitLoader(id='await_loader_id_2', delay=0.2)
            ]
        )
        start = asyncio.get_running_loop().time()
        extracted_data = await PipelineStrategy.run_loader("DATA", destinations)
        total = asyncio.get_running_loop().time() - start

        assert 0.5 > total >= 0.4, "Delay Should be 0.4 second for asychronously executing two loads"

        assert extracted_data == [
            {'id': 'await_loader_id', 'success': True}, 
            {'id': 'await_loader_id_2', 'success': True}
        ]

    @staticmethod
    def test_run_transformer_multiple_with_delay() -> None:
        tf = TransformPhase.model_construct(
            steps=[
                MockAwaitTransformer(id='await_transformer_id', delay=0.2),
                MockAwaitTransformer(id='await_transformer_id_2', delay=0.3)
            ]
        )

        start = time.time()
        PipelineStrategy.run_transformer("DATA", tf)
        total = time.time() - start

        assert 0.6 > total >= 0.5, 'Delay Should be 0.5 seconds for sychronous transformations.'



    @staticmethod
    def test_run_transformer_after_load_multiple_with_delay() -> None:
        tf = TransformLoadPhase.model_construct(
            steps=[
                MockAwaitLoadTransformer(id='await_transform_load_id', delay=0.1),
                MockAwaitLoadTransformer(id='await_transform_load_id2', delay=0.2)
            ]
        )

        start = time.time()
        PipelineStrategy.run_transformer_after_load(tf)
        total = time.time() - start

        assert 0.4 > total >= 0.3, 'Delay Should be 0.3 seconds for sychronous transformations.'
