# Standard Imports
import asyncio
import time
from functools import wraps

# Third-party Imports
import pytest

from common.type_def import Plugin
from core.executor import (
    run_extractor,
    run_loader,
    run_transformer,
    run_transformer_after_load,
    task_executor,
)
from core.models.phases import (
    ExtractPhase,
    LoadPhase,
    TransformLoadPhase,
    TransformPhase,
)
from core.plugins import PluginWrapper

# Project Imports
from tests.resources import mocks


def async_pre(output: str, delay: float = 0.2) -> Plugin:
    async def inner() -> str:
        await asyncio.sleep(delay)
        return output

    return inner


def sync_pre(output: str, delay: float = 0.2) -> Plugin:
    def inner() -> str:
        time.sleep(delay)
        return output

    return inner


def upper_transformer(id: str) -> Plugin:  # noqa: A002,ARG001
    @wraps(upper_transformer)
    def inner(data: str) -> str:
        return data.upper()

    return inner


@pytest.mark.asyncio
async def test_run_extractor_without_delay() -> None:
    extract = ExtractPhase.model_construct(
        steps=[PluginWrapper(id="extractor_id", func=mocks.mock_async_extractor(id="extractor_id"))]
    )
    result = await run_extractor(extract)

    assert result == "async_extracted_data"


@pytest.mark.asyncio
async def test_run_extractor_multiple_without_delay() -> None:
    extracts = ExtractPhase.model_construct(
        steps=[
            PluginWrapper(id="extractor_id", func=mocks.mock_async_extractor(id="extractor_id")),
            PluginWrapper(
                id="extractor_id_2",
                func=mocks.mock_async_extractor(id="extractor_id_2"),
            ),
        ],
        merge=PluginWrapper(id="func_mock", func=mocks.mock_merger()),
    )

    result = await run_extractor(extracts)
    assert result == "merged_data"


def test_run_transformer() -> None:
    transformations = TransformPhase.model_construct(
        steps=[PluginWrapper(id="transformer_id", func=mocks.mock_transformer(id="transformer_id"))]
    )

    result = run_transformer("DATA", transformations)

    assert result == "transformed_etl_data"


def test_run_multiple_transformer() -> None:
    data = "initial_data"
    transformations = TransformPhase.model_construct(
        steps=[
            PluginWrapper(id="transformer_id", func=mocks.mock_transformer(id="transformer_id")),
            PluginWrapper(id="transformer_upper", func=upper_transformer(id="transformer_upper")),
        ]
    )

    result = run_transformer(data, transformations)

    assert result == "TRANSFORMED_ETL_DATA"


@pytest.mark.asyncio
async def test_run_loader_without_delay() -> None:
    data = "INITIAL_DATA_SUFFIX"
    destinations = LoadPhase.model_construct(
        steps=[PluginWrapper(id="loader_id", func=mocks.mock_loader(id="loader_id"))]
    )
    result = await run_loader(data, destinations)

    assert result == [{"id": "loader_id", "success": True}]


@pytest.mark.asyncio
async def test_run_loader_multiple_without_delay() -> None:
    data = "TRANSFORMED_ETL_DATA"
    destinations = LoadPhase.model_construct(
        steps=[
            PluginWrapper(id="loader_id", func=mocks.mock_loader(id="loader_id")),
            PluginWrapper(id="loader_id_2", func=mocks.mock_loader(id="loader_id_2")),
        ]
    )
    result = await run_loader(data, destinations)
    assert result == [
        {"id": "loader_id", "success": True},
        {"id": "loader_id_2", "success": True},
    ]


def test_run_transformer_after_load() -> None:
    transformations = TransformLoadPhase.model_construct(
        steps=[
            PluginWrapper(
                id="mock_transform_loader_id",
                func=mocks.mock_load_transformer(id="mock_transform_loader_id", query="SELECT 1"),
            )
        ]
    )
    result = run_transformer_after_load(transformations)

    assert result == [
        {"id": "mock_transform_loader_id", "success": True},
    ]


def test_run_transformer_after_load_multiple() -> None:
    transformations = TransformLoadPhase.model_construct(
        steps=[
            PluginWrapper(
                id="mock_transform_loader_id",
                func=mocks.mock_load_transformer(id="mock_transform_loader_id", query="SELECT 1"),
            ),
            PluginWrapper(
                id="mock_transform_loader_id_2",
                func=mocks.mock_load_transformer(id="mock_transform_loader_id_2", query="SELECT 2"),
            ),
        ]
    )
    result = run_transformer_after_load(transformations)

    assert result == [
        {"id": "mock_transform_loader_id", "success": True},
        {"id": "mock_transform_loader_id_2", "success": True},
    ]


class TestUnitPipelineStrategyConcurrency:
    @staticmethod
    @pytest.mark.asyncio
    async def test_execute_pre_processing() -> None:
        plugins = [
            PluginWrapper(id="1", func=async_pre(output="Async 1 result", delay=0.1)),
            PluginWrapper(id="2", func=async_pre(output="Async 2 result", delay=0.2)),
        ]

        start = asyncio.get_running_loop().time()
        result = await task_executor(plugins)
        total = asyncio.get_running_loop().time() - start

        # Concurrency validation
        assert 0.3 > total > 0.2, "Delay Should be 0.2 seconds"
        assert result == {
            "1": "Async 1 result",
            "2": "Async 2 result",
        }

    @staticmethod
    @pytest.mark.asyncio
    async def test_run_extractor_with_pre_processing() -> None:
        # Set the side_effect function
        extract = ExtractPhase.model_construct(
            steps=[
                PluginWrapper(
                    id="async_extractor_id",
                    func=mocks.mock_async_extractor(id="async_extractor_id", delay=0.1),
                )
            ],
            pre=[
                PluginWrapper(id="1", func=async_pre(output="Async 1 result", delay=0.1)),
                PluginWrapper(id="2", func=async_pre(output="Async 2 result", delay=0.3)),
            ],
        )

        start = asyncio.get_running_loop().time()
        result = await run_extractor(extract)
        total = asyncio.get_running_loop().time() - start

        # Concurrency validation
        assert 0.5 > total >= 0.4, "Delay Should be (0.1) Extract + (0.3) Pre processing "
        assert result == "async_extracted_data"

    @staticmethod
    @pytest.mark.asyncio
    async def test_run_extractor_multiple_with_delay() -> None:
        extract = ExtractPhase.model_construct(
            steps=[
                PluginWrapper(
                    id="async_extractor_id",
                    func=mocks.mock_async_extractor(id="async_extractor_id", delay=0.1),
                ),
                PluginWrapper(
                    id="async_extractor_id_2",
                    func=mocks.mock_async_extractor(id="async_extractor_id_2", delay=0.2),
                ),
            ],
            merge=PluginWrapper(id="func_mock", func=mocks.mock_merger()),
        )
        start = asyncio.get_running_loop().time()
        extracted_data = await run_extractor(extract)
        total = asyncio.get_running_loop().time() - start

        assert 0.3 > total >= 0.2, "Delay Should be 0.2 second for asychronously executing two extracts"
        assert extracted_data == "merged_data"

    @staticmethod
    @pytest.mark.asyncio
    async def test_run_loader_with_pre_processing() -> None:
        destinations = LoadPhase.model_construct(
            steps=[
                PluginWrapper(
                    id="async_loader_id",
                    func=mocks.mock_async_loader(id="async_loader_id", delay=0.1),
                )
            ],
            pre=[
                PluginWrapper(id="1", func=async_pre(output="Async 1 result", delay=0.2)),
                PluginWrapper(id="2", func=async_pre(output="Async 2 result", delay=0.1)),
            ],
        )
        start = asyncio.get_running_loop().time()
        result = await run_loader("DATA", destinations)
        total = asyncio.get_running_loop().time() - start

        # Concurrency validation
        assert 0.4 > total >= 0.3, "Delay Should be (0.1) Load + (0.2) Pre processing "
        assert result == [{"id": "async_loader_id", "success": True}]

    @staticmethod
    @pytest.mark.asyncio
    async def test_run_loader_multiple_with_delay() -> None:
        destinations = LoadPhase.model_construct(
            steps=[
                PluginWrapper(
                    id="async_loader_id",
                    func=mocks.mock_async_loader(id="async_loader_id", delay=0.4),
                ),
                PluginWrapper(
                    id="async_loader_id_2",
                    func=mocks.mock_async_loader(id="async_loader_id_2", delay=0.2),
                ),
            ]
        )

        start = asyncio.get_running_loop().time()
        extracted_data = await run_loader("DATA", destinations)
        total = asyncio.get_running_loop().time() - start

        assert 0.5 > total >= 0.4, "Delay Should be 0.4 second for asychronously executing two loads"

        assert extracted_data == [
            {"id": "async_loader_id", "success": True},
            {"id": "async_loader_id_2", "success": True},
        ]

    @staticmethod
    def test_run_transformer_multiple_with_delay() -> None:
        tf = TransformPhase.model_construct(
            steps=[
                PluginWrapper(
                    id="async_transformer_id",
                    func=mocks.mock_sync_transformer(id="async_transformer_id", delay=0.2),
                ),
                PluginWrapper(
                    id="async_transformer_id_2",
                    func=mocks.mock_sync_transformer(id="async_transformer_id_2", delay=0.1),
                ),
            ]
        )

        start = time.time()
        run_transformer("DATA", tf)
        total = time.time() - start

        assert 0.4 > total >= 0.3, "Delay Should be 0.3 seconds for sychronous transformations."

    @staticmethod
    def test_run_transformer_after_load_multiple_with_delay() -> None:
        tf = TransformLoadPhase.model_construct(
            steps=[
                PluginWrapper(
                    id="async_transform_loader_id",
                    func=mocks.mock_sync_load_transformer(id="async_transform_loader_id", delay=0.1),
                ),
                PluginWrapper(
                    id="async_transform_loader_id_2",
                    func=mocks.mock_sync_load_transformer(id="async_transform_loader_id_2", delay=0.2),
                ),
            ]
        )

        start = time.time()
        run_transformer_after_load(tf)
        total = time.time() - start

        assert 0.4 > total >= 0.3, "Delay Should be 0.3 seconds for sychronous transformations."
