# Standard Imports
import asyncio
from collections.abc import Callable

# Third-party Imports
import pytest

# Project Imports
from pipeline_flow.core.executor import ELTStrategy, ETLStrategy, ETLTStrategy
from pipeline_flow.core.models.phases import (
    ExtractPhase,
    LoadPhase,
    TransformLoadPhase,
    TransformPhase,
)
from pipeline_flow.core.models.pipeline import Pipeline
from tests.resources.plugins import (
    SimpleExtractorPlugin,
    SimpleLoaderPlugin,
    SimpleTransformLoadPlugin,
    SimpleTransformPlugin,
)


@pytest.mark.asyncio
async def test_etl_strategy(etl_pipeline_factory: Callable[..., Pipeline]) -> None:
    etl_pipeline = etl_pipeline_factory(name="Job1")

    result = await ETLStrategy().execute(etl_pipeline)

    assert result is True


@pytest.mark.asyncio
async def test_elt_strategy(elt_pipeline_factory: Callable[..., Pipeline]) -> None:
    elt_pipeline = elt_pipeline_factory(name="Job1")

    result = await ELTStrategy().execute(elt_pipeline)

    assert result is True


@pytest.mark.asyncio
async def test_etlt_strategy(etlt_pipeline_factory: Callable[..., Pipeline]) -> None:
    etlt_pipeline = etlt_pipeline_factory(name="Job1")

    result = await ETLTStrategy().execute(etlt_pipeline)
    assert result is True


@pytest.mark.asyncio
async def test_etl_strategy_with_delay() -> None:
    pipeline = Pipeline(
        name="Job1",
        type="ETL",  # type: ignore[reportArgumentType]
        phases={  # type: ignore[reportArgumentType]
            "extract": ExtractPhase.model_construct(
                steps=[
                    SimpleExtractorPlugin(plugin_id="async_extractor_id", delay=0.2),
                ],
            ),
            "transform": TransformPhase.model_construct(
                steps=[
                    SimpleTransformPlugin(plugin_id="async_transformer_id", delay=0.1),
                ]
            ),
            "load": LoadPhase.model_construct(
                steps=[
                    SimpleLoaderPlugin(plugin_id="async_loader_id1", delay=0.2),
                    SimpleLoaderPlugin(plugin_id="async_loader_id2", delay=0.1),
                    SimpleLoaderPlugin(plugin_id="async_loader_id3", delay=0.3),
                ]
            ),
        },
    )

    start = asyncio.get_running_loop().time()
    result = await ETLStrategy().execute(pipeline)
    total = asyncio.get_running_loop().time() - start

    assert result is True
    assert 0.7 > total >= 0.6, "Delay Should be Extract (0.2) + Transform (0.1) + Load (0.3) "


@pytest.mark.asyncio
async def test_elt_strategy_with_delay() -> None:
    pipeline = Pipeline(
        name="Job1",
        type="ELT",  # type: ignore[reportArgumentType]
        phases={  # type: ignore[reportArgumentType]
            "extract": ExtractPhase.model_construct(
                steps=[
                    SimpleExtractorPlugin(plugin_id="async_extractor_id", delay=0.2),
                ],
            ),
            "load": LoadPhase.model_construct(
                steps=[
                    SimpleLoaderPlugin(plugin_id="async_loader_id1", delay=0.2),
                    SimpleLoaderPlugin(plugin_id="async_loader_id2", delay=0.1),
                ]
            ),
            "transform_at_load": TransformLoadPhase.model_construct(
                steps=[
                    SimpleTransformLoadPlugin(plugin_id="async_transform_loader_id", query="SELECT 1", delay=0.1),
                    SimpleTransformLoadPlugin(plugin_id="async_transform_loader_id_2", query="SELECT 2", delay=0.1),
                ]
            ),
        },
    )

    start = asyncio.get_running_loop().time()
    result = await ELTStrategy().execute(pipeline)
    total = asyncio.get_running_loop().time() - start

    assert result is True
    assert 0.7 > total >= 0.6, "Delay Should be Extract (0.2) + Load (0.2) + Transform at load (0.2) "


@pytest.mark.asyncio
async def test_etlt_strategy_with_delay() -> None:
    pipeline = Pipeline(
        name="Job1",
        type="ETLT",  # type: ignore[reportArgumentType]
        phases={  # type: ignore[reportArgumentType]
            "extract": ExtractPhase.model_construct(
                steps=[
                    SimpleExtractorPlugin(plugin_id="async_extractor_id", delay=0.2),
                ],
            ),
            "transform": TransformPhase.model_construct(
                steps=[
                    SimpleTransformPlugin(plugin_id="async_transformer_id", delay=0.1),
                ],
            ),
            "load": LoadPhase.model_construct(
                steps=[
                    SimpleLoaderPlugin(plugin_id="async_loader_id1", delay=0.2),
                    SimpleLoaderPlugin(plugin_id="async_loader_id2", delay=0.1),
                ]
            ),
            "transform_at_load": TransformLoadPhase.model_construct(
                steps=[
                    SimpleTransformLoadPlugin(plugin_id="async_transform_loader_id", query="SELECT 1", delay=0.1),
                    SimpleTransformLoadPlugin(plugin_id="async_transform_loader_id_2", query="SELECT 1", delay=0.1),
                ]
            ),
        },
    )

    start = asyncio.get_running_loop().time()
    result = await ETLTStrategy().execute(pipeline)
    total = asyncio.get_running_loop().time() - start

    assert result is True
    assert 0.8 > total >= 0.7, "Delay Should be Extract (0.2) + Transform (0.1) + Load (0.2) + Transform at load (0.2) "
