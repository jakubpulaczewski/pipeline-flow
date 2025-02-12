# Standard Imports
from collections.abc import Callable
from unittest.mock import AsyncMock, Mock

# Third-party Imports
import pytest
from pytest_mock import MockerFixture

# Project Imports
from pipeline_flow.core import executor
from pipeline_flow.core.models import Pipeline
from pipeline_flow.core.models.phases import (
    ExtractPhase,
    LoadPhase,
    TransformLoadPhase,
    TransformPhase,
)
from tests.resources.plugins import (
    SimpleExtractorPlugin,
    SimpleLoaderPlugin,
    SimpleMergePlugin,
    SimpleTransformLoadPlugin,
    SimpleTransformPlugin,
)


@pytest.mark.asyncio
async def test_run_extractor_without_delay() -> None:
    extract = ExtractPhase.model_construct(steps=[SimpleExtractorPlugin(plugin_id="extractor_id")])
    result = await executor.run_extractor(extract)

    assert result == "extracted_data"


@pytest.mark.asyncio
async def test_run_extractor_multiple_without_delay() -> None:
    extracts = ExtractPhase.model_construct(
        steps=[
            SimpleExtractorPlugin(plugin_id="extractor_id"),
            SimpleExtractorPlugin(plugin_id="extractor_id_2"),
        ],
        merge=SimpleMergePlugin(plugin_id="merge_mock_id"),
    )

    result = await executor.run_extractor(extracts)
    assert result == "merged_data"


def test_run_transformer_with_zero_transformation() -> None:
    transformations = TransformPhase.model_construct(steps=[])

    result = executor.run_transformer("DATA", transformations)

    assert result == "DATA"


def test_run_transformer_with_one_transformation() -> None:
    transformations = TransformPhase.model_construct(steps=[SimpleTransformPlugin(plugin_id="transformer_id")])

    result = executor.run_transformer("data", transformations)

    assert result == "transformed_data"


def test_run_multiple_transformer() -> None:
    transformations = TransformPhase.model_construct(
        steps=[
            Mock(id="transformer_id", spec=SimpleTransformPlugin, side_effect=lambda data: f"transformed_{data}"),
            Mock(id="transformer_upper", spec=SimpleTransformPlugin, side_effect=lambda data: data.upper()),
        ]
    )

    result = executor.run_transformer("data", transformations)

    assert result == "TRANSFORMED_DATA"


@pytest.mark.asyncio
async def test_run_loader_without_delay(mocker: MockerFixture) -> None:
    loader_plugin = SimpleLoaderPlugin(plugin_id="loader_id")
    destinations = LoadPhase.model_construct(steps=[loader_plugin])

    spy = mocker.spy(SimpleLoaderPlugin, "__call__")

    await executor.run_loader("TRANSFORMED_DATA", destinations)

    spy.assert_called_once_with(loader_plugin, data="TRANSFORMED_DATA")  # loader_plugin operates as "self" in the call


@pytest.mark.asyncio
async def test_run_loader_multiple_without_delay(mocker: MockerFixture) -> None:
    loader_plugin1 = SimpleLoaderPlugin(plugin_id="loader_id")
    loader_plugin2 = SimpleLoaderPlugin(plugin_id="loader_id_2")

    destinations = LoadPhase.model_construct(
        steps=[
            loader_plugin1,
            loader_plugin2,
        ]
    )
    spy = mocker.spy(SimpleLoaderPlugin, "__call__")

    await executor.run_loader("TRANSFORMED_ETL_DATA", destinations)

    assert spy.call_count == 2, "Both loaders should be called"


def test_run_transformer_after_load(mocker: MockerFixture) -> None:
    tf_load_plugin = SimpleTransformLoadPlugin(plugin_id="transform_loader_id", query="SELECT 1")

    spy = mocker.spy(SimpleTransformLoadPlugin, "__call__")

    transformations = TransformLoadPhase.model_construct(
        steps=[
            tf_load_plugin,
        ]
    )
    executor.run_transformer_after_load(transformations)

    spy.assert_called_once_with(tf_load_plugin)  # tf_load_plugin operates as "self" in the call


def test_run_transformer_after_load_multiple(mocker: MockerFixture) -> None:
    tf_load_plugin1 = SimpleTransformLoadPlugin(plugin_id="transform_loader_id", query="SELECT 1")
    tf_load_plugin2 = SimpleTransformLoadPlugin(plugin_id="transform_loader_id_2", query="SELECT 1")

    spy = mocker.spy(SimpleTransformLoadPlugin, "__call__")

    transformations = TransformLoadPhase.model_construct(
        steps=[
            tf_load_plugin1,
            tf_load_plugin2,
        ]
    )
    executor.run_transformer_after_load(transformations)

    assert spy.call_count == 2


@pytest.mark.asyncio
async def test_execution_etl_pipeline(mocker: MockerFixture, etl_pipeline_factory: Callable[..., Pipeline]) -> None:
    extract_mock = mocker.patch.object(executor, "run_extractor", new_callable=AsyncMock, return_value="extracted_data")
    tf_mock = mocker.patch.object(executor, "run_transformer", new_callable=Mock, return_value="transformed_data")
    load_mock = mocker.patch.object(executor, "run_loader", new_callable=AsyncMock)

    etl_pipeline = etl_pipeline_factory(name="Job1")

    result = await executor.ETLStrategy().execute(etl_pipeline)

    extract_mock.assert_called_once_with(etl_pipeline.extract)
    tf_mock.assert_called_once_with("extracted_data", etl_pipeline.transform)
    load_mock.assert_called_once_with("transformed_data", etl_pipeline.load)

    assert result is True


@pytest.mark.asyncio
async def test_execution_elt_pipeline(mocker: MockerFixture, elt_pipeline_factory: Callable[..., Pipeline]) -> None:
    extract_mock = mocker.patch.object(executor, "run_extractor", new_callable=AsyncMock, return_value="extracted_data")
    load_mock = mocker.patch.object(executor, "run_loader", new_callable=AsyncMock)
    tf_load_mock = mocker.patch.object(executor, "run_transformer_after_load", new_callable=Mock)

    elt_pipeline = elt_pipeline_factory(name="Job1")

    result = await executor.ELTStrategy().execute(elt_pipeline)

    extract_mock.assert_called_once_with(elt_pipeline.extract)
    load_mock.assert_called_once_with("extracted_data", elt_pipeline.load)
    tf_load_mock.assert_called_once_with(elt_pipeline.load_transform)

    assert result is True


@pytest.mark.asyncio
async def test_execution_etlt_strategy(mocker: MockerFixture, etlt_pipeline_factory: Callable[..., Pipeline]) -> None:
    extract_mock = mocker.patch.object(executor, "run_extractor", new_callable=AsyncMock, return_value="extracted_data")
    tf_mock = mocker.patch.object(executor, "run_transformer", new_callable=Mock, return_value="transformed_data")
    load_mock = mocker.patch.object(executor, "run_loader", new_callable=AsyncMock)
    tf_load_mock = mocker.patch.object(executor, "run_transformer_after_load", new_callable=Mock)

    etlt_pipeline = etlt_pipeline_factory(name="Job1")
    result = await executor.ETLTStrategy().execute(etlt_pipeline)

    extract_mock.assert_called_once_with(etlt_pipeline.extract)
    tf_mock.assert_called_once_with("extracted_data", etlt_pipeline.transform)
    load_mock.assert_called_once_with("transformed_data", etlt_pipeline.load)
    tf_load_mock.assert_called_once_with(etlt_pipeline.load_transform)

    assert result is True
