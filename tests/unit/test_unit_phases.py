# Standard Imports
from unittest.mock import AsyncMock, Mock

# Third-party Imports
import pytest
from pydantic import ValidationError
from pytest_mock import MockerFixture

# Project Imports
from core.executor import plugin_async_executor, plugin_sync_executor
from core.models.phases import (
    ExtractPhase,
    LoadPhase,
    Phase,
    PipelinePhase,
    TransformLoadPhase,
    TransformPhase,
    unique_id_validator,
)
from core.plugins import PluginRegistry, PluginWrapper
from tests.resources.plugins import (
    simple_dummy_plugin,
    simple_extractor_plugin,
    simple_loader_plugin,
    simple_merge_plugin,
    simple_transform_load_plugin,
    simple_transform_plugin,
)


def test_unique_id_validator_with_single_id() -> None:
    steps = [
        PluginWrapper(id="extractor_id", func=simple_dummy_plugin()),
    ]

    assert unique_id_validator(steps=steps) == steps


def test_unique_id_validator_with_different_ids() -> None:
    # Should pass with multiple different IDs
    steps = [
        PluginWrapper(id="extractor_id_1", func=simple_dummy_plugin()),
        PluginWrapper(id="extractor_id_2", func=simple_dummy_plugin()),
    ]

    assert unique_id_validator(steps=steps) == steps


def test_unique_id_validator_with_multiple_duplicate_ids() -> None:
    with pytest.raises(ValueError, match="The `ID` is not unique. There already exists an 'ID' with this name."):
        unique_id_validator(
            steps=[
                PluginWrapper(id="extractor_id", func=simple_dummy_plugin()),
                PluginWrapper(id="extractor_id", func=simple_dummy_plugin()),
            ]
        )


@pytest.mark.asyncio
async def test_run_async_extractor() -> None:
    extract_mock = AsyncMock(side_effect=simple_extractor_plugin(delay=0))
    executor = PluginWrapper(
        id="async_extractor_id",
        func=extract_mock,
    )

    result = await plugin_async_executor(executor)

    extract_mock.assert_awaited_once()
    assert result == "extracted_data"


def test_run_transform_data() -> None:
    transform_mock = Mock(side_effect=simple_transform_plugin(delay=0))
    transformer = PluginWrapper(id="transformer_id", func=transform_mock)

    result = plugin_sync_executor(transformer, data="extracted_data")

    transform_mock.assert_called_once_with(data="extracted_data")
    assert result == "transformed_extracted_data"


def test_run_transform_load_data() -> None:
    mock_load_tf = Mock(side_effect=simple_transform_load_plugin(query="SELECT 1", delay=0))

    load_transformer = PluginWrapper(
        id="mock_transform_loader_id",
        func=mock_load_tf,
    )

    plugin_sync_executor(load_transformer)

    mock_load_tf.assert_called_once()


@pytest.mark.asyncio
async def test_run_load_data() -> None:
    mock_loader = AsyncMock(side_effect=simple_loader_plugin(delay=0))
    loader = PluginWrapper(id="loader_id", func=mock_loader)
    await plugin_async_executor(loader, data="data")

    mock_loader.assert_awaited_once_with(data="data")


@pytest.mark.parametrize("phase_class", [(ExtractPhase), (LoadPhase), (TransformLoadPhase)])
def test_create_phase_without_mandary_phase(phase_class: Phase) -> None:
    with pytest.raises(ValidationError, match="List should have at least 1 item after validation"):
        phase_class(steps=[])  # type: ignore[reportArgumentType] - The dict is parsed into PluginWrapper object


@pytest.mark.asyncio
async def test_create_phase_extract(mocker: MockerFixture) -> None:
    registry_mock = mocker.patch.object(PluginRegistry, "get", return_value=simple_dummy_plugin)

    extract = ExtractPhase(
        steps=[  # type: ignore[reportArgumentType] - The dict is parsed into PluginWrapper object
            {
                "id": "extractor_id",
                "plugin": "mock_extractor",
            }
        ]
    )

    registry_mock.assert_called_once_with(PipelinePhase.EXTRACT_PHASE, "mock_extractor")

    assert extract == ExtractPhase.model_construct(steps=[PluginWrapper(id="extractor_id", func=simple_dummy_plugin())])


def test_extract_with_merge_success(mocker: MockerFixture) -> None:
    registry_mock = mocker.patch.object(
        PluginRegistry,
        "get",
        side_effect=[simple_dummy_plugin, simple_dummy_plugin, simple_merge_plugin],
    )

    extract = ExtractPhase(
        steps=[  # type: ignore[reportArgumentType] - The dict is parsed into PluginWrapper object
            {
                "id": "extractor_id",
                "plugin": "mock_extractor",
            },
            {"id": "extractor_id_2", "plugin": "mock_extractor_2"},
        ],
        merge={"id": "simple_merge_id", "plugin": "mock_merger"},  # type: ignore[reportArgumentType] - The dict is parsed into PluginWrapper object
    )

    # Behaviour check of the mock
    assert len(registry_mock.mock_calls) == 3
    registry_mock.assert_any_call(PipelinePhase.EXTRACT_PHASE, "mock_extractor")
    registry_mock.assert_any_call(PipelinePhase.EXTRACT_PHASE, "mock_extractor_2")
    registry_mock.assert_any_call(PipelinePhase.EXTRACT_PHASE, "mock_merger")

    assert extract.merge == PluginWrapper(id="simple_merge_id", func=simple_merge_plugin())
    assert extract.steps[0] == PluginWrapper(id="extractor_id", func=simple_dummy_plugin())
    assert extract.steps[1] == PluginWrapper(id="extractor_id_2", func=simple_dummy_plugin())


def test_extract_phase_rejects_merge_with_single_step(mocker: MockerFixture) -> None:
    mocker.patch.object(PluginRegistry, "get", side_effect=[simple_dummy_plugin, simple_merge_plugin])
    with pytest.raises(
        ValidationError,
        match="Validation Error! Merge can only be used if there is more than one extract step.",
    ):
        ExtractPhase(
            steps=[  # type: ignore[reportArgumentType] - The dict is parsed into PluginWrapper object
                {
                    "id": "extractor_id",
                    "plugin": "mock_extractor",
                }
            ],
            merge={"id": "simple_merge_id", "plugin": "mock_merger"},  # type: ignore[reportArgumentType] - The dict is parsed into PluginWrapper object
        )


def test_extract_phase_rejects_merge_with_two_steps_and_no_merge(mocker: MockerFixture) -> None:
    mocker.patch.object(PluginRegistry, "get", side_effect=[simple_dummy_plugin, simple_merge_plugin])

    with pytest.raises(
        ValidationError,
        match="Validation Error! Merge is required when there are more than one extract step.",
    ):
        ExtractPhase(
            steps=[  # type: ignore[reportArgumentType] - The dict is parsed into PluginWrapper object
                {
                    "id": "extractor_id",
                    "plugin": "mock_extractor",
                },
                {"id": "extractor_id_2", "plugin": "mock_extractor_2"},
            ],
        )


def test_create_phase_with_same_id_failure(mocker: MockerFixture) -> None:
    mocker.patch.object(PluginRegistry, "get", side_effect=[simple_dummy_plugin, simple_merge_plugin])

    with pytest.raises(
        ValueError,
        match="The `ID` is not unique. There already exists an 'ID' with this name",
    ):
        ExtractPhase(
            steps=[  # type: ignore[reportArgumentType] - The dict is parsed into PluginWrapper object
                {
                    "id": "extractor_id",
                    "plugin": "mock_extractor",
                },
                {"id": "extractor_id", "plugin": "mock_extractor_2"},
            ]
        )


def test_create_phase_transform_without_steps_success() -> None:
    transform = TransformPhase(steps=[])

    assert transform == TransformPhase.model_construct(steps=[])


def test_create_phase_function_transform(mocker: MockerFixture) -> None:
    registry_mock = mocker.patch.object(PluginRegistry, "get", side_effect=[simple_dummy_plugin])

    transform = TransformPhase(steps=[{"id": "transformer_id", "plugin": "mock_transformer"}])  # type: ignore[reportArgumentType] - The dict is parsed into PluginWrapper object

    registry_mock.assert_called_once_with(PipelinePhase.TRANSFORM_PHASE, "mock_transformer")
    assert transform == TransformPhase.model_construct(
        steps=[PluginWrapper(id="transformer_id", func=simple_dummy_plugin())]
    )


def test_create_phase_load(mocker: MockerFixture) -> None:
    registry_mock = mocker.patch.object(PluginRegistry, "get", side_effect=[simple_dummy_plugin])

    loader = LoadPhase(steps=[{"id": "loader_id", "plugin": "mock_loader"}])  # type: ignore[reportArgumentType] - The dict is parsed into PluginWrapper object

    registry_mock.assert_called_once_with(PipelinePhase.LOAD_PHASE, "mock_loader")
    assert loader == LoadPhase.model_construct(steps=[PluginWrapper(id="loader_id", func=simple_dummy_plugin())])


def test_create_phase_transform_at_load(mocker: MockerFixture) -> None:
    registry_mock = mocker.patch.object(PluginRegistry, "get", side_effect=[simple_transform_load_plugin])

    tf_load = TransformLoadPhase(
        steps=[  # type: ignore[reportArgumentType] - The dict is parsed into PluginWrapper object
            {
                "id": "mock_transform_loader_id",
                "plugin": "mock_transformer_loader",
                "params": {"query": "SELECT 1"},
            }
        ]
    )
    registry_mock.assert_called_once_with(PipelinePhase.TRANSFORM_AT_LOAD_PHASE, "mock_transformer_loader")

    assert tf_load == TransformLoadPhase.model_construct(
        steps=[
            PluginWrapper(
                id="mock_transform_loader_id",
                func=simple_transform_load_plugin(query="SELECT 1"),
            )
        ]
    )
