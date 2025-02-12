# Standard Imports
import asyncio
import time
from unittest.mock import call

# Third-party Imports
import pytest
from pytest_mock import MockerFixture

# Project Imports
from pipeline_flow.core.executor import (
    run_extractor,
    run_loader,
    run_transformer,
    run_transformer_after_load,
    task_group_executor,
)
from pipeline_flow.core.models.phases import (
    ExtractPhase,
    LoadPhase,
    TransformLoadPhase,
    TransformPhase,
)
from tests.resources.plugins import (
    SimpleAsyncPrePlugin,
    SimpleExtractorPlugin,
    SimpleLoaderPlugin,
    SimpleMergePlugin,
    SimpleTransformLoadPlugin,
    SimpleTransformPlugin,
)


@pytest.mark.asyncio
async def test_concurrency_with_pre_processing(mocker: MockerFixture) -> None:
    # Define pre processing mocks with delays
    pre_plugin1 = SimpleAsyncPrePlugin(plugin_id="pre_mock_id", delay=0.1)
    pre_plugin2 = SimpleAsyncPrePlugin(plugin_id="pre_mock_id_2", delay=0.2)

    spy = mocker.spy(SimpleAsyncPrePlugin, "__call__")

    start = asyncio.get_running_loop().time()
    result = await task_group_executor([pre_plugin1, pre_plugin2])
    total = asyncio.get_running_loop().time() - start

    # Behaviour validation
    spy.assert_has_calls(
        [
            call(pre_plugin1),
            call(pre_plugin2),
        ]
    )
    assert spy.call_count == 2

    # Concurrency validation
    assert 0.3 > total > 0.2, "Delay Should be 0.2 seconds"
    assert result == {
        "pre_mock_id": "Async result",
        "pre_mock_id_2": "Async result",
    }


@pytest.mark.asyncio
async def test_concurrency_with_extractor_and_pre_processing(mocker: MockerFixture) -> None:
    # Define the plugin mocks with delays
    extractor_plugin = SimpleExtractorPlugin(plugin_id="extractor_id", delay=0.1)
    pre_plugin1 = SimpleAsyncPrePlugin(plugin_id="pre_mock_id", delay=0.1)
    pre_plugin2 = SimpleAsyncPrePlugin(plugin_id="pre_mock_id_2", delay=0.15)

    extractor_spy = mocker.spy(SimpleExtractorPlugin, "__call__")
    pre_spy = mocker.spy(SimpleAsyncPrePlugin, "__call__")

    extract = ExtractPhase.model_construct(
        steps=[extractor_plugin],
        pre=[
            pre_plugin1,
            pre_plugin2,
        ],
    )

    start = asyncio.get_running_loop().time()
    result = await run_extractor(extract)
    total = asyncio.get_running_loop().time() - start

    # Behaviour validation
    extractor_spy.assert_awaited_once()
    pre_spy.assert_has_calls(
        [
            call(pre_plugin1),
            call(pre_plugin2),
        ]
    )

    # Concurrency validation
    assert 0.3 > total >= 0.25, "Delay Should be (0.1) Extract + (0.15) Pre processing "
    assert result == "extracted_data"


@pytest.mark.asyncio
async def test_concurrency_with_extractor_and_merge(mocker: MockerFixture) -> None:
    # Define plugins
    extractor_plugin = SimpleExtractorPlugin(plugin_id="extractor_id", delay=0.1)
    extractor_plugin2 = SimpleExtractorPlugin(plugin_id="extractor_id2", delay=0.2)
    merge_plugin = SimpleMergePlugin(plugin_id="merge_mock_id")

    # Define spies
    extractor_spy = mocker.spy(SimpleExtractorPlugin, "__call__")
    merge_spy = mocker.spy(SimpleMergePlugin, "__call__")

    # `model_construct` Skips the validators of the ExtractPhase model
    extract = ExtractPhase.model_construct(
        steps=[
            extractor_plugin,
            extractor_plugin2,
        ],
        merge=merge_plugin,
    )
    start = asyncio.get_running_loop().time()
    extracted_data = await run_extractor(extract)
    total = asyncio.get_running_loop().time() - start

    # Behaviour validation
    assert extractor_spy.call_count == 2
    merge_spy.assert_called_with(
        merge_plugin, extracted_data={"extractor_id": "extracted_data", "extractor_id2": "extracted_data"}
    )

    # Concurrency validation
    assert 0.3 > total >= 0.2, "Delay Should be 0.2 second for asychronously executing two extracts"
    assert extracted_data == "merged_data"


@pytest.mark.asyncio
async def test_concurrency_with_multiple_loaders(mocker: MockerFixture) -> None:
    destinations = LoadPhase.model_construct(
        steps=[
            SimpleLoaderPlugin(plugin_id="loader_id", delay=0.1),
            SimpleLoaderPlugin(plugin_id="loader_id_2", delay=0.2),
        ]
    )

    loader_spy = mocker.spy(SimpleLoaderPlugin, "__call__")

    start = asyncio.get_running_loop().time()
    await run_loader("DATA", destinations)
    total = asyncio.get_running_loop().time() - start

    # Behaviour validation
    assert loader_spy.call_count == 2

    # Concurrency validation
    assert 0.3 > total >= 0.2, "Delay Should be 0.2 second for asychronously executing two loads"


@pytest.mark.asyncio
async def test_concurrency_with_loaders_and_pre_processing(mocker: MockerFixture) -> None:
    destinations = LoadPhase.model_construct(
        steps=[
            SimpleLoaderPlugin(plugin_id="loader_id", delay=0.1),
        ],
        pre=[
            SimpleAsyncPrePlugin(plugin_id="pre_id", delay=0.1),
            SimpleAsyncPrePlugin(plugin_id="pre_id_2", delay=0.2),
        ],
    )

    loader_spy = mocker.spy(SimpleLoaderPlugin, "__call__")
    pre_spy = mocker.spy(SimpleAsyncPrePlugin, "__call__")

    start = asyncio.get_running_loop().time()
    await run_loader("DATA", destinations)
    total = asyncio.get_running_loop().time() - start

    # Behaviour validation
    loader_spy.assert_awaited_once()
    assert pre_spy.call_count == 2

    # Concurrency validation
    assert 0.4 > total >= 0.3, "Delay Should be (0.1) Load + (0.2) Pre processing "


def test_concurrency_with_multiple_transformations(mocker: MockerFixture) -> None:
    tf1 = SimpleTransformPlugin(plugin_id="transformer_id", delay=0.1)
    tf2 = SimpleTransformPlugin(plugin_id="transformer_id_2", delay=0.2)

    tf = TransformPhase.model_construct(steps=[tf1, tf2])

    spy = mocker.spy(SimpleTransformPlugin, "__call__")

    start = time.time()
    run_transformer("DATA", tf)
    total = time.time() - start

    # Behaviour validation
    spy.assert_has_calls(
        [
            call(tf1, "DATA"),
            call(tf2, "transformed_DATA"),
        ]
    )

    # Concurrent validation
    assert 0.4 > total >= 0.3, "Delay Should be 0.3 seconds for sychronous transformations."


def test_concurrency_with_multiple_load_transformtions(mocker: MockerFixture) -> None:
    tf = TransformLoadPhase.model_construct(
        steps=[
            SimpleTransformLoadPlugin(plugin_id="transform_loader_id", query="SELECT 1", delay=0.1),
            SimpleTransformLoadPlugin(plugin_id="transform_loader_id_2", query="SELECT 2", delay=0.2),
        ]
    )

    spy = mocker.spy(SimpleTransformLoadPlugin, "__call__")

    start = time.time()
    run_transformer_after_load(tf)
    total = time.time() - start

    # Behaviour validation
    assert spy.call_count == 2

    # Concurrency Validation
    assert 0.4 > total >= 0.3, "Delay Should be 0.3 seconds for sychronous transformations."
