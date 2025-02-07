# Standard Imports
import asyncio
import time
from unittest.mock import AsyncMock, Mock

# Third-party Imports
import pytest

from core.executor import (
    run_extractor,
    run_loader,
    run_transformer,
    run_transformer_after_load,
    task_group_executor,
)
from core.models.phases import (
    ExtractPhase,
    LoadPhase,
    TransformLoadPhase,
    TransformPhase,
)
from core.plugins import PluginWrapper

# Project Imports
from tests.resources.plugins import (
    async_pre,
    simple_extractor_plugin,
    simple_loader_plugin,
    simple_merge_plugin,
    simple_transform_load_plugin,
    simple_transform_plugin,
)


@pytest.mark.asyncio
async def test_concurrency_with_pre_processing() -> None:
    # Define pre processing mocks with delays
    pre_mock1 = AsyncMock(side_effect=async_pre(delay=0.1))
    pre_mock2 = AsyncMock(side_effect=async_pre(delay=0.2))

    plugins = [
        PluginWrapper(id="1", func=pre_mock1),
        PluginWrapper(id="2", func=pre_mock2),
    ]

    start = asyncio.get_running_loop().time()
    result = await task_group_executor(plugins)
    total = asyncio.get_running_loop().time() - start

    # Behaviour validation
    pre_mock1.assert_called_once()
    pre_mock2.assert_called_once()

    # Concurrency validation
    assert 0.3 > total > 0.2, "Delay Should be 0.2 seconds"
    assert result == {
        "1": "Async result",
        "2": "Async result",
    }


@pytest.mark.asyncio
async def test_concurrency_with_extractor_and_pre_processing() -> None:
    # Define the plugin mocks with delays
    extractor_mock = AsyncMock(side_effect=simple_extractor_plugin(delay=0.1))
    pre_mock1 = AsyncMock(side_effect=async_pre(delay=0.1))
    pre_mock2 = AsyncMock(side_effect=async_pre(delay=0.15))

    extract = ExtractPhase.model_construct(
        steps=[
            PluginWrapper(
                id="async_extractor_id",
                func=extractor_mock,
            )
        ],
        pre=[
            PluginWrapper(id="1", func=pre_mock1),
            PluginWrapper(id="2", func=pre_mock2),
        ],
    )

    start = asyncio.get_running_loop().time()
    result = await run_extractor(extract)
    total = asyncio.get_running_loop().time() - start

    # Behaviour validation
    extractor_mock.assert_awaited_once()
    pre_mock1.assert_awaited_once()
    pre_mock2.assert_awaited_once()

    # Concurrency validation
    assert 0.3 > total >= 0.25, "Delay Should be (0.1) Extract + (0.15) Pre processing "
    assert result == "extracted_data"


@pytest.mark.asyncio
async def test_concurrency_with_extractor_and_merge() -> None:
    # Define the plugin mocks with delays
    extract1_mock = AsyncMock(side_effect=simple_extractor_plugin(delay=0.1))
    extract2_mock = AsyncMock(side_effect=simple_extractor_plugin(delay=0.2))
    merge_mock = Mock(side_effect=simple_merge_plugin())

    # Skips the validators of the ExtractPhase model
    extract = ExtractPhase.model_construct(
        steps=[
            PluginWrapper(
                id="async_extractor_id",
                func=extract1_mock,
            ),
            PluginWrapper(
                id="async_extractor_id_2",
                func=extract2_mock,
            ),
        ],
        merge=PluginWrapper(id="func_mock", func=merge_mock),
    )
    start = asyncio.get_running_loop().time()
    extracted_data = await run_extractor(extract)
    total = asyncio.get_running_loop().time() - start

    # Behaviour validation
    extract1_mock.assert_awaited_once()
    extract2_mock.assert_awaited_once()
    merge_mock.assert_called_once_with(
        extracted_data={"async_extractor_id": "extracted_data", "async_extractor_id_2": "extracted_data"}
    )

    # Concurrency validation
    assert 0.3 > total >= 0.2, "Delay Should be 0.2 second for asychronously executing two extracts"
    assert extracted_data == "merged_data"


@pytest.mark.asyncio
async def test_concurrency_with_multiple_loaders() -> None:
    # Define the plugin mocks with delays
    load_plugin1_mock = AsyncMock(side_effect=simple_loader_plugin(delay=0.2))
    load_plugin2_mock = AsyncMock(side_effect=simple_loader_plugin(delay=0.1))

    destinations = LoadPhase.model_construct(
        steps=[
            PluginWrapper(
                id="async_loader_id",
                func=load_plugin1_mock,
            ),
            PluginWrapper(
                id="async_loader_id_2",
                func=load_plugin2_mock,
            ),
        ]
    )

    start = asyncio.get_running_loop().time()
    await run_loader("DATA", destinations)
    total = asyncio.get_running_loop().time() - start

    # Behaviour validation
    load_plugin1_mock.assert_awaited_once_with(data="DATA")
    load_plugin2_mock.assert_awaited_once_with(data="DATA")

    # Concurrency validation
    assert 0.3 > total >= 0.2, "Delay Should be 0.2 second for asychronously executing two loads"


@pytest.mark.asyncio
async def test_concurrency_with_loaders_and_pre_processing() -> None:
    # Define the mocks with delays
    loader_mock = AsyncMock(side_effect=simple_loader_plugin(delay=0.1))
    pre_mock1 = AsyncMock(side_effect=async_pre(delay=0.1))
    pre_mock2 = AsyncMock(side_effect=async_pre(delay=0.2))

    destinations = LoadPhase.model_construct(
        steps=[
            PluginWrapper(
                id="async_loader_id",
                func=loader_mock,
            )
        ],
        pre=[
            PluginWrapper(id="1", func=pre_mock1),
            PluginWrapper(id="2", func=pre_mock2),
        ],
    )
    start = asyncio.get_running_loop().time()
    await run_loader("DATA", destinations)
    total = asyncio.get_running_loop().time() - start

    # Behaviour validation
    pre_mock1.assert_awaited_once()
    pre_mock2.assert_awaited_once()
    loader_mock.assert_awaited_once_with(data="DATA")

    # Concurrency validation
    assert 0.4 > total >= 0.3, "Delay Should be (0.1) Load + (0.2) Pre processing "


def test_concurrency_with_multiple_transformations() -> None:
    # Define the mocks with delays
    transformer_mock1 = Mock(side_effect=simple_transform_plugin(delay=0.1))
    transformer_mock2 = Mock(side_effect=simple_transform_plugin(delay=0.2))

    tf = TransformPhase.model_construct(
        steps=[
            PluginWrapper(
                id="async_transformer_id",
                func=transformer_mock1,
            ),
            PluginWrapper(
                id="async_transformer_id_2",
                func=transformer_mock2,
            ),
        ]
    )

    start = time.time()
    run_transformer("DATA", tf)
    total = time.time() - start

    # Behaviour validation
    transformer_mock1.assert_called_once_with("DATA")
    transformer_mock2.assert_called_once_with("transformed_DATA")

    # Concurrent validation
    assert 0.4 > total >= 0.3, "Delay Should be 0.3 seconds for sychronous transformations."


def test_concurrency_with_multiple_load_transformtions() -> None:
    # Define the mocks with delays
    load_transform_plugin1 = Mock(side_effect=simple_transform_load_plugin(query="SELECT 1", delay=0.1))
    load_transform_plugin2 = Mock(side_effect=simple_transform_load_plugin(query="SELECT 2", delay=0.2))

    tf = TransformLoadPhase.model_construct(
        steps=[
            PluginWrapper(
                id="async_transform_loader_id",
                func=load_transform_plugin1,
            ),
            PluginWrapper(
                id="async_transform_loader_id_2",
                func=load_transform_plugin2,
            ),
        ]
    )

    start = time.time()
    run_transformer_after_load(tf)
    total = time.time() - start

    # Behaviour validation
    load_transform_plugin1.assert_called_once()
    load_transform_plugin2.assert_called_once()

    # Concurrency Validation
    assert 0.4 > total >= 0.3, "Delay Should be 0.3 seconds for sychronous transformations."
