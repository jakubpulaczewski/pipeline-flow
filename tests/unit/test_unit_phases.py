# Standard Imports

# Third-party Imports
import pytest
from pydantic import ValidationError
from pytest_mock import MockerFixture

# Project Imports
from pipeline_flow.core.executor import plugin_async_executor, plugin_sync_executor
from pipeline_flow.core.models.phases import (
    ExtractPhase,
    LoadPhase,
    Phase,
    TransformLoadPhase,
    TransformPhase,
    unique_id_validator,
)
from pipeline_flow.core.registry import PluginRegistry
from tests.resources.plugins import (
    SimpleDummyPlugin,
    SimpleExtractorPlugin,
    SimpleLoaderPlugin,
    SimpleMergePlugin,
    SimpleTransformLoadPlugin,
    SimpleTransformPlugin,
)


def test_unique_id_validator_with_single_id() -> None:
    steps = [SimpleDummyPlugin(plugin_id="extractor_id")]

    assert unique_id_validator(steps=steps) == steps


def test_unique_id_validator_with_different_ids() -> None:
    # Should pass with multiple different IDs
    steps = [
        SimpleDummyPlugin(plugin_id="extractor_id_1"),
        SimpleDummyPlugin(plugin_id="extractor_id_2"),
    ]

    assert unique_id_validator(steps=steps) == steps


def test_unique_id_validator_with_multiple_duplicate_ids() -> None:
    with pytest.raises(ValueError, match="The `ID` is not unique. There already exists an 'ID' with this name."):
        unique_id_validator(
            steps=[
                SimpleDummyPlugin(plugin_id="extractor_id"),
                SimpleDummyPlugin(plugin_id="extractor_id"),
            ]
        )


@pytest.mark.asyncio
async def test_run_async_extractor() -> None:
    simple_plugin = SimpleExtractorPlugin(plugin_id="extractor_id", delay=0)

    result = await plugin_async_executor(simple_plugin)

    assert result == "extracted_data"


def test_run_transform_data() -> None:
    transform_plugin = SimpleTransformPlugin(plugin_id="transformer_id", delay=0)

    result = plugin_sync_executor(transform_plugin, data="extracted_data")

    assert result == "transformed_extracted_data"


def test_run_transform_load_data(mocker: MockerFixture) -> None:
    tf_loader_plugin = SimpleTransformLoadPlugin(plugin_id="transform_loader_id", query="SELECT 1", delay=0)

    spy = mocker.spy(SimpleTransformLoadPlugin, "__call__")

    plugin_sync_executor(tf_loader_plugin)

    spy.assert_called_once()


@pytest.mark.asyncio
async def test_run_load_data(mocker: MockerFixture) -> None:
    loader_plugin = SimpleLoaderPlugin(plugin_id="loader_id", delay=0)

    spy = mocker.spy(SimpleLoaderPlugin, "__call__")

    await plugin_async_executor(loader_plugin, data="data")

    spy.assert_called_once_with(loader_plugin, data="data")  # The first argument is "self"


@pytest.mark.parametrize("phase_class", [(ExtractPhase), (LoadPhase), (TransformLoadPhase)])
def test_create_phase_without_mandary_phase(phase_class: Phase) -> None:
    with pytest.raises(ValidationError, match="List should have at least 1 item after validation"):
        phase_class(steps=[])  # type: ignore[reportArgumentType] - The dict is parsed into Plugin object


@pytest.mark.asyncio
async def test_create_phase_extract(mocker: MockerFixture) -> None:
    registry_mock = mocker.patch.object(PluginRegistry, "get", return_value=SimpleExtractorPlugin)

    extract = ExtractPhase(
        steps=[  # type: ignore[reportArgumentType] - The dict is parsed into Plugin object
            {
                "id": "extractor_id",
                "plugin": "mock_extractor",
            }
        ]
    )

    registry_mock.assert_called_once_with("mock_extractor")

    assert isinstance(extract, ExtractPhase)
    assert isinstance(extract.steps[0], SimpleExtractorPlugin)


def test_extract_with_merge_success(mocker: MockerFixture) -> None:
    registry_mock = mocker.patch.object(
        PluginRegistry,
        "get",
        side_effect=[SimpleExtractorPlugin, SimpleExtractorPlugin, SimpleMergePlugin],
    )

    extract = ExtractPhase(
        steps=[  # type: ignore[reportArgumentType] - The dict is parsed into Plugin object
            {
                "id": "extractor_id",
                "plugin": "mock_extractor",
            },
            {"id": "extractor_id_2", "plugin": "mock_extractor_2"},
        ],
        merge={"id": "simple_merge_id", "plugin": "mock_merger"},  # type: ignore[reportArgumentType] - The dict is parsed into Plugin object
    )

    # Behaviour check of the mock
    assert len(registry_mock.mock_calls) == 3
    registry_mock.assert_any_call("mock_extractor")
    registry_mock.assert_any_call("mock_extractor_2")
    registry_mock.assert_any_call("mock_merger")

    assert isinstance(extract, ExtractPhase)
    assert extract.steps[0].id == "extractor_id"
    assert isinstance(extract.steps[0], SimpleExtractorPlugin)
    assert isinstance(extract.steps[1], SimpleExtractorPlugin)
    assert extract.steps[1].id == "extractor_id_2"
    assert isinstance(extract.merge, SimpleMergePlugin)


def test_extract_phase_rejects_merge_with_single_step(mocker: MockerFixture) -> None:
    mocker.patch.object(PluginRegistry, "get", side_effect=[SimpleExtractorPlugin, SimpleMergePlugin])
    with pytest.raises(
        ValidationError,
        match="Validation Error! Merge can only be used if there is more than one extract step.",
    ):
        ExtractPhase(
            steps=[  # type: ignore[reportArgumentType] - The dict is parsed into Plugin object
                {
                    "id": "extractor_id",
                    "plugin": "mock_extractor",
                }
            ],
            merge={"id": "simple_merge_id", "plugin": "mock_merger"},  # type: ignore[reportArgumentType] - The dict is parsed into Plugin object
        )


def test_extract_phase_rejects_merge_with_two_steps_and_no_merge(mocker: MockerFixture) -> None:
    mocker.patch.object(PluginRegistry, "get", side_effect=[SimpleExtractorPlugin, SimpleExtractorPlugin])

    with pytest.raises(
        ValidationError,
        match="Validation Error! Merge is required when there are more than one extract step.",
    ):
        ExtractPhase(
            steps=[  # type: ignore[reportArgumentType] - The dict is parsed into Plugin object
                {
                    "id": "extractor_id",
                    "plugin": "mock_extractor",
                },
                {"id": "extractor_id_2", "plugin": "mock_extractor_2"},
            ],
        )


def test_create_phase_with_same_id_failure(mocker: MockerFixture) -> None:
    mocker.patch.object(PluginRegistry, "get", side_effect=[SimpleExtractorPlugin, SimpleExtractorPlugin])

    with pytest.raises(
        ValueError,
        match="The `ID` is not unique. There already exists an 'ID' with this name",
    ):
        ExtractPhase(
            steps=[  # type: ignore[reportArgumentType] - The dict is parsed into Plugin object
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
    registry_mock = mocker.patch.object(PluginRegistry, "get", side_effect=[SimpleTransformPlugin])

    transform = TransformPhase(steps=[{"id": "transformer_id", "plugin": "mock_transformer"}])  # type: ignore[reportArgumentType] - The dict is parsed into Plugin object

    registry_mock.assert_called_once_with("mock_transformer")

    assert isinstance(transform, TransformPhase)
    assert transform.steps[0].id == "transformer_id"
    assert isinstance(transform.steps[0], SimpleTransformPlugin)


def test_create_phase_load(mocker: MockerFixture) -> None:
    registry_mock = mocker.patch.object(PluginRegistry, "get", side_effect=[SimpleLoaderPlugin])

    loader = LoadPhase(steps=[{"id": "loader_id", "plugin": "mock_loader"}])  # type: ignore[reportArgumentType] - The dict is parsed into Plugin object

    registry_mock.assert_called_once_with("mock_loader")

    assert isinstance(loader, LoadPhase)
    assert isinstance(loader.steps[0], SimpleLoaderPlugin)
    assert loader.steps[0].id == "loader_id"


def test_create_phase_transform_at_load(mocker: MockerFixture) -> None:
    registry_mock = mocker.patch.object(PluginRegistry, "get", side_effect=[SimpleTransformLoadPlugin])

    tf_load = TransformLoadPhase(
        steps=[  # type: ignore[reportArgumentType] - The dict is parsed into Plugin object
            {
                "id": "mock_transform_loader_id",
                "plugin": "mock_transformer_loader",
                "params": {"query": "SELECT 1"},
            }
        ]
    )
    registry_mock.assert_called_once_with("mock_transformer_loader")

    assert isinstance(tf_load, TransformLoadPhase)
    assert isinstance(tf_load.steps[0], SimpleTransformLoadPlugin)
    assert tf_load.steps[0].id == "mock_transform_loader_id"
