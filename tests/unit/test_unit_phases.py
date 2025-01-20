# Standard Imports

# Third-party Imports
import pytest
from pydantic import ValidationError
from pytest_mock import MockerFixture, MockType

# Project Imports
from common.type_def import Plugin, PluginName
from core.executor import plugin_async_executor, plugin_sync_executor
from core.models.phases import ExtractPhase, LoadPhase, PhaseInstance, PipelinePhase, TransformLoadPhase, TransformPhase
from core.plugins import PluginRegistry, PluginWrapper
from tests.resources import mocks


@pytest.fixture
def plugin_mock(mocker: MockerFixture) -> MockType:
    return mocker.patch.object(PluginRegistry, "get")


@pytest.fixture
def plugin_registry_mock(plugin_mock: MockType) -> MockType:
    def plugin_fetcher(phase_pipeline: PipelinePhase, plugin_name: PluginName) -> Plugin:  # noqa: ARG001
        if "mock_extractor" in plugin_name:  # noqa: RET503
            return mocks.mock_extractor
        elif "mock_merger" in plugin_name:  # noqa: RET505
            return mocks.mock_merger

    plugin_mock.side_effect = plugin_fetcher

    return plugin_mock


@pytest.mark.asyncio
async def test_run_async_extractor() -> None:
    executor = PluginWrapper(
        id="async_extractor_id",
        func=mocks.mock_async_extractor(id="async_extractor_id"),
    )
    assert await plugin_async_executor(executor) == "async_extracted_data"


def test_run_transform_data() -> None:
    transformer = PluginWrapper(id="transformer_id", func=mocks.mock_transformer(id="transformer_id"))
    assert plugin_sync_executor(transformer, data="extracted_data") == "transformed_etl_data"


def test_run_transform_load_data() -> None:
    load_transformer = PluginWrapper(
        id="mock_transform_loader_id",
        func=mocks.mock_load_transformer(id="mock_transform_loader_id", query="SELECT 1"),
    )
    assert plugin_sync_executor(load_transformer) is None


@pytest.mark.asyncio
async def test_run_load_data() -> None:
    loader = PluginWrapper(id="loader_id", func=mocks.mock_loader(id="loader_id"))
    assert await plugin_async_executor(loader, data="data") is None


@pytest.mark.parametrize("phase_class", [(ExtractPhase), (LoadPhase), (TransformLoadPhase)])
def test_create_phase_without_mandary_phase(phase_class: PhaseInstance) -> None:
    with pytest.raises(ValidationError, match="List should have at least 1 item after validation"):
        phase_class(steps=[])


def test_create_phase_extract(plugin_mock: MockType) -> None:
    plugin_mock.return_value = mocks.mock_extractor

    extract = ExtractPhase(
        steps=[
            {
                "id": "extractor_id",
                "plugin": "mock_extractor",
            }
        ]
    )

    plugin_mock.assert_called_once()
    plugin_mock.assert_called_once_with(PipelinePhase.EXTRACT_PHASE, "mock_extractor")

    assert extract == ExtractPhase.model_construct(
        steps=[PluginWrapper(id="extractor_id", func=mocks.mock_extractor(id="extractor_id"))]
    )


def test_create_phase_extract_with_merge_success(mocker: MockerFixture, plugin_registry_mock: MockType) -> None:
    mocker.patch("uuid.uuid4", return_value="12345678")

    extract = ExtractPhase(
        steps=[
            {
                "id": "extractor_id",
                "plugin": "mock_extractor",
            },
            {"id": "extractor_id_2", "plugin": "mock_extractor_2"},
        ],
        merge={"plugin": "mock_merger"},
    )

    # Behaviour check of the mock
    assert len(plugin_registry_mock.mock_calls) == 3
    plugin_registry_mock.assert_any_call(PipelinePhase.EXTRACT_PHASE, "mock_extractor")
    plugin_registry_mock.assert_any_call(PipelinePhase.EXTRACT_PHASE, "mock_extractor_2")
    plugin_registry_mock.assert_any_call(PipelinePhase.EXTRACT_PHASE, "mock_merger")

    assert extract.merge == PluginWrapper(id="mock_merger_12345678", func=mocks.mock_merger())
    assert extract.steps[0] == PluginWrapper(id="extractor_id", func=mocks.mock_extractor(id="extractor_id"))
    assert extract.steps[1] == PluginWrapper(id="extractor_id_2", func=mocks.mock_extractor(id="extractor_id_2"))


@pytest.mark.usefixtures("plugin_registry_mock")
def test_create_phase_extract_with_merge_failure() -> None:
    with pytest.raises(
        ValidationError,
        match="Validation Error! Merge can only be used if there is more than one extract step.",
    ):
        ExtractPhase(
            steps=[
                {
                    "id": "extractor_id",
                    "plugin": "mock_extractor",
                }
            ],
            merge={"plugin": "mock_merger"},
        )


@pytest.mark.usefixtures("plugin_registry_mock")
def test_create_phase_with_same_id_failure() -> None:
    with pytest.raises(
        ValueError,
        match="The `ID` is not unique. There already exists an 'ID' with this name",
    ):
        ExtractPhase(
            steps=[
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


def test_create_phase_function_transform(plugin_mock: MockType) -> None:
    plugin_mock.return_value = mocks.mock_transformer

    transform = TransformPhase(steps=[{"id": "transformer_id", "plugin": "mock_transformer"}])

    plugin_mock.assert_called_once()
    plugin_mock.assert_called_once_with(PipelinePhase.TRANSFORM_PHASE, "mock_transformer")
    assert transform == TransformPhase.model_construct(
        steps=[PluginWrapper(id="transformer_id", func=mocks.mock_transformer(id="transformer_id"))]
    )


def test_create_phase_load(plugin_mock: MockType) -> None:
    plugin_mock.return_value = mocks.mock_loader

    loader = LoadPhase(steps=[{"id": "loader_id", "plugin": "mock_loader"}])

    plugin_mock.assert_called_once()
    plugin_mock.assert_called_once_with(PipelinePhase.LOAD_PHASE, "mock_loader")

    assert loader == LoadPhase.model_construct(
        steps=[PluginWrapper(id="loader_id", func=mocks.mock_loader(id="loader_id"))]
    )


def test_create_phase_transform_at_load(plugin_mock: MockType) -> None:
    plugin_mock.return_value = mocks.mock_load_transformer

    tf_load = TransformLoadPhase(
        steps=[
            {
                "id": "mock_transform_loader_id",
                "query": "SELECT 1",
                "plugin": "mock_transformer_loader",
            }
        ]
    )

    plugin_mock.assert_called_once()
    plugin_mock.assert_called_once_with(PipelinePhase.TRANSFORM_AT_LOAD_PHASE, "mock_transformer_loader")

    assert tf_load == TransformLoadPhase.model_construct(
        steps=[
            PluginWrapper(
                id="mock_transform_loader_id",
                func=mocks.mock_load_transformer(id="mock_transform_loader_id", query="SELECT 1"),
            )
        ]
    )
