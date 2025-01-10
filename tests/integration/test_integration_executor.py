# Standard Imports
import asyncio

# Third-party Imports
import pytest

# Project Imports
import tests.resources.mocks as mocks

from core.models.phases import (
    ExtractPhase,
    LoadPhase,
    TransformLoadPhase,
    TransformPhase,
)
from core.models.pipeline import Pipeline
from core.plugins import PluginWrapper
from core.executor import (
    ETLStrategy, 
    ELTStrategy, 
    ETLTStrategy
)


@pytest.mark.asyncio
async def test_etl_strategy(etl_pipeline_factory) -> None:
    etl_pipeline = etl_pipeline_factory(name="Job1")

    result = await ETLStrategy().execute(etl_pipeline)

    assert result == True


@pytest.mark.asyncio
async def test_elt_strategy(elt_pipeline_factory) -> None:
    elt_pipeline = elt_pipeline_factory(name="Job1")
    
    result = await ELTStrategy().execute(elt_pipeline)

    assert result == True


@pytest.mark.asyncio
async def test_etlt_strategy(etlt_pipeline_factory) -> None:
    etlt_pipeline = etlt_pipeline_factory(name="Job1")

    result = await ETLTStrategy().execute(etlt_pipeline)
    assert result == True


@pytest.mark.asyncio
async def test_etl_strategy_with_delay():
    pipeline = Pipeline(
        name="Job1",
        type="ETL",
        phases={
            'extract': ExtractPhase.model_construct(
                steps=[PluginWrapper(id="async_extractor_id", func=mocks.mock_async_extractor(id='async_extractor_id', delay=0.2))],
            ), 
            'transform': TransformPhase.model_construct(
                steps=[
                    PluginWrapper(id='async_transformer_id', func=mocks.mock_sync_transformer(id='async_transformer_id', delay=0.1)),
                ]
            ), 
            'load': LoadPhase.model_construct(
                steps=[
                    PluginWrapper(id='async_loader_id', func=mocks.mock_async_loader(id='async_loader_id', delay=0.2)),
                    PluginWrapper(id='async_loader_id', func=mocks.mock_async_loader(id='async_loader_id', delay=0.1)),
                    PluginWrapper(id='async_loader_id', func=mocks.mock_async_loader(id='async_loader_id', delay=0.3))
                ]
            )
        }
    )

    start = asyncio.get_running_loop().time()
    result = await ETLStrategy().execute(pipeline)
    total = asyncio.get_running_loop().time() - start

    assert result == True
    assert 0.7 > total >= 0.6, "Delay Should be Extract (0.2) + Transform (0.1) + Load (0.3) "


@pytest.mark.asyncio
async def test_elt_strategy_with_delay():
    pipeline = Pipeline(
        name="Job1",
        type="ELT",
        phases={
            'extract': ExtractPhase.model_construct(
                steps=[PluginWrapper(id="async_extractor_id", func=mocks.mock_async_extractor(id='async_extractor_id', delay=0.2))],
            ), 
            'load': LoadPhase.model_construct(
                steps=[
                    PluginWrapper(id='async_loader_id', func=mocks.mock_async_loader(id='async_loader_id', delay=0.2)),
                    PluginWrapper(id='async_loader_id', func=mocks.mock_async_loader(id='async_loader_id', delay=0.1)),
                ]
            ),
            'transform_at_load': TransformLoadPhase.model_construct(
                steps=[
                    PluginWrapper(id='async_transform_loader_id', func=mocks.mock_sync_load_transformer(id='async_transform_loader_id', delay=0.1)),
                    PluginWrapper(id='async_transform_loader_id_2', func=mocks.mock_sync_load_transformer(id='async_transform_loader_id_2', delay=0.1))
                ]
            )
        }
    )

    start = asyncio.get_running_loop().time()
    result = await ELTStrategy().execute(pipeline)
    total = asyncio.get_running_loop().time() - start

    assert result == True
    assert 0.7 > total >= 0.6, "Delay Should be Extract (0.2) + Load (0.2) + Transform at load (0.2) "


@pytest.mark.asyncio
async def test_etlt_strategy_with_delay():
    pipeline = Pipeline(
        name="Job1",
        type="ETLT",
        phases={
            'extract': ExtractPhase.model_construct(
                steps=[PluginWrapper(id="async_extractor_id", func=mocks.mock_async_extractor(id='async_extractor_id', delay=0.2))],
            ),
            'transform': TransformPhase.model_construct(
                steps=[PluginWrapper(id='async_transformer_id', func=mocks.mock_sync_transformer(id='async_transformer_id', delay=0.1))],
            ), 
            'load': LoadPhase.model_construct(
                steps=[
                    PluginWrapper(id='async_loader_id', func=mocks.mock_async_loader(id='async_loader_id', delay=0.2)),
                    PluginWrapper(id='async_loader_id', func=mocks.mock_async_loader(id='async_loader_id', delay=0.1)),
                ]
            ),
            'transform_at_load': TransformLoadPhase.model_construct(
                steps=[
                    PluginWrapper(id='async_transform_loader_id', func=mocks.mock_sync_load_transformer(id='async_transform_loader_id', delay=0.1)),
                    PluginWrapper(id='async_transform_loader_id_2', func=mocks.mock_sync_load_transformer(id='async_transform_loader_id_2', delay=0.1))
                ]
            )
        }
    )

    start = asyncio.get_running_loop().time()
    result = await ETLTStrategy().execute(pipeline)
    total = asyncio.get_running_loop().time() - start

    assert result == True
    assert 0.8 > total >= 0.7, "Delay Should be Extract (0.2) + Transform (0.1) + Load (0.2) + Transform at load (0.2) "
