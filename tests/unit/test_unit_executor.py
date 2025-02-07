# Standard Imports
from collections.abc import Callable
from unittest.mock import AsyncMock, Mock

# Third-party Imports
import pytest

# Project Imports
from core import executor
from core.models import Pipeline
from core.models.phases import (
    ExtractPhase,
    LoadPhase,
    TransformLoadPhase,
    TransformPhase,
)
from core.plugins import PluginWrapper
from pytest_mock import MockerFixture

from tests.resources.plugins import (
    simple_extractor_plugin,
    simple_loader_plugin,
    simple_merge_plugin,
    simple_transform_load_plugin,
    simple_transform_plugin,
)


@pytest.mark.asyncio
async def test_run_extractor_without_delay() -> None:
    extract = ExtractPhase.model_construct(steps=[PluginWrapper(id="extractor_id", func=simple_extractor_plugin())])
    result = await executor.run_extractor(extract)

    assert result == "extracted_data"


@pytest.mark.asyncio
async def test_run_extractor_multiple_without_delay() -> None:
    extracts = ExtractPhase.model_construct(
        steps=[
            PluginWrapper(id="extractor_id", func=simple_extractor_plugin()),
            PluginWrapper(
                id="extractor_id_2",
                func=simple_extractor_plugin(),
            ),
        ],
        merge=PluginWrapper(id="func_mock", func=simple_merge_plugin()),
    )

    result = await executor.run_extractor(extracts)
    assert result == "merged_data"


def test_run_transformer_with_zero_transformation() -> None:
    transformations = TransformPhase.model_construct(steps=[])

    result = executor.run_transformer("DATA", transformations)

    assert result == "DATA"


def test_run_transformer_with_one_transformation() -> None:
    transformations = TransformPhase.model_construct(
        steps=[PluginWrapper(id="transformer_id", func=simple_transform_plugin())]
    )

    result = executor.run_transformer("data", transformations)

    assert result == "transformed_data"


def test_run_multiple_transformer() -> None:
    mock_transformer = Mock(side_effect=lambda data: f"transformed_{data}")
    upper_transformer = Mock(side_effect=lambda data: data.upper())

    transformations = TransformPhase.model_construct(
        steps=[
            PluginWrapper(id="transformer_id", func=mock_transformer),
            PluginWrapper(id="transformer_upper", func=upper_transformer),
        ]
    )

    result = executor.run_transformer("data", transformations)

    assert result == "TRANSFORMED_DATA"


@pytest.mark.asyncio
async def test_run_loader_without_delay() -> None:
    simple_loader_plugin_mock = AsyncMock(side_effect=simple_loader_plugin())
    destinations = LoadPhase.model_construct(steps=[PluginWrapper(id="loader_id", func=simple_loader_plugin_mock)])

    await executor.run_loader("TRANSFORMED_DATA", destinations)

    simple_loader_plugin_mock.assert_called_once_with(data="TRANSFORMED_DATA")
    assert simple_loader_plugin_mock.call_count == 1, "Loader should be called once"


@pytest.mark.asyncio
async def test_run_loader_multiple_without_delay() -> None:
    simple_loader_plugin_mock = AsyncMock(side_effect=simple_loader_plugin())

    destinations = LoadPhase.model_construct(
        steps=[
            PluginWrapper(id="loader_id", func=simple_loader_plugin_mock),
            PluginWrapper(id="loader_id_2", func=simple_loader_plugin_mock),
        ]
    )
    await executor.run_loader("TRANSFORMED_ETL_DATA", destinations)

    simple_loader_plugin_mock.assert_called_with(data="TRANSFORMED_ETL_DATA")
    assert simple_loader_plugin_mock.call_count == 2, "Both loaders should be called"
    assert simple_loader_plugin_mock.mock_calls[0] == simple_loader_plugin_mock.mock_calls[1], (
        "Both loaders should be called with the same data"
    )


def test_run_transformer_after_load() -> None:
    simple_transform_load_plugin_mock = Mock(side_effect=simple_transform_load_plugin(query="SELECT 1"))

    transformations = TransformLoadPhase.model_construct(
        steps=[
            PluginWrapper(
                id="mock_transform_loader_id",
                func=simple_transform_load_plugin_mock,
            )
        ]
    )
    executor.run_transformer_after_load(transformations)

    simple_transform_load_plugin_mock.assert_called_once()


def test_run_transformer_after_load_multiple() -> None:
    simple_transform_load_plugin_mock = Mock(
        side_effect=[
            simple_transform_load_plugin(query="SELECT 1"),
            simple_transform_load_plugin(query="SELECT 2"),
        ]
    )

    transformations = TransformLoadPhase.model_construct(
        steps=[
            PluginWrapper(
                id="mock_transform_loader_id",
                func=simple_transform_load_plugin_mock,
            ),
            PluginWrapper(
                id="mock_transform_loader_id_2",
                func=simple_transform_load_plugin_mock,
            ),
        ]
    )
    executor.run_transformer_after_load(transformations)

    assert simple_transform_load_plugin_mock.call_count == 2


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
