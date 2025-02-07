# Standard Imports
from unittest.mock import MagicMock

# Third Party Imports
import pytest

# Project
from core.models.phases import PipelinePhase
from core.plugins import PluginRegistry, PluginWrapper, plugin
from pytest_mock import MockerFixture, MockType

from tests.resources import plugins


@pytest.fixture
def simple_dummy_plugin_mock() -> MagicMock:
    mock = MagicMock(side_effect=plugins.simple_dummy_plugin)
    mock.__name__ = "simple_dummy_plugin"
    return mock


def test_plugin_decorator(simple_dummy_plugin_mock: MockType) -> None:
    plugin_func = plugin(PipelinePhase.EXTRACT_PHASE, "mock_plugin")(simple_dummy_plugin_mock)

    result = plugin_func()

    simple_dummy_plugin_mock.assert_called_once()
    assert result.__name__ == "simple_dummy_plugin"

    plugin_callable = PluginRegistry.get(PipelinePhase.EXTRACT_PHASE, "mock_plugin")
    assert plugin_callable is simple_dummy_plugin_mock


def test_register_plugin(simple_dummy_plugin_mock: MockType) -> None:
    result = PluginRegistry.register(PipelinePhase.EXTRACT_PHASE, "mock_plugin", simple_dummy_plugin_mock)
    assert result is True
    assert PipelinePhase.EXTRACT_PHASE in PluginRegistry._registry
    assert PluginRegistry._registry[PipelinePhase.EXTRACT_PHASE] == {"mock_plugin": simple_dummy_plugin_mock}


def test_register_duplicate_plugin(simple_dummy_plugin_mock: MockType) -> None:
    result = PluginRegistry.register(PipelinePhase.EXTRACT_PHASE, "fake_plugin", simple_dummy_plugin_mock)
    assert result is True
    result = PluginRegistry.register(PipelinePhase.EXTRACT_PHASE, "fake_plugin", simple_dummy_plugin_mock)
    assert result is False


def test_remove_plugin(simple_dummy_plugin_mock: MockType) -> None:
    plugin_name = "fake_plugin"
    # Add the plugin
    PluginRegistry.register(PipelinePhase.EXTRACT_PHASE, plugin_name, simple_dummy_plugin_mock)

    # Removes the plugin
    result = PluginRegistry.remove(PipelinePhase.EXTRACT_PHASE, plugin_name)
    assert result is True

    # Try to remove the same plugin, should return False
    result = PluginRegistry.remove(PipelinePhase.EXTRACT_PHASE, plugin_name)
    assert result is False


def test_remove_nonexistent_plugin() -> None:
    result = PluginRegistry.remove(PipelinePhase.EXTRACT_PHASE, "fake_plugin")
    assert result is False


def test_get_plugin(simple_dummy_plugin_mock: MockType) -> None:
    plugin_name = "mock_plugin"

    # Registering the plugin
    PluginRegistry.register(PipelinePhase.EXTRACT_PHASE, plugin_name, simple_dummy_plugin_mock)

    # Fetch it
    plugin_class = PluginRegistry.get(PipelinePhase.EXTRACT_PHASE, plugin_name)
    assert plugin_class is simple_dummy_plugin_mock

    # Try to get the same plugin plugin, after its removed, should raise an exception
    PluginRegistry.remove(PipelinePhase.EXTRACT_PHASE, plugin_name)

    with pytest.raises(ValueError, match="Plugin class was not found for following plugin `mock_plugin`."):
        PluginRegistry.get(PipelinePhase.EXTRACT_PHASE, plugin_name)


def test_get_nonexistent_plugin() -> None:
    with pytest.raises(ValueError, match="Plugin class was not found for following plugin `fake_plugin`."):
        PluginRegistry.get(PipelinePhase.EXTRACT_PHASE, "fake_plugin")


def test_instantiate_plugin_with_optional_id(mocker: MockerFixture, simple_dummy_plugin_mock: MockType) -> None:
    mocker.patch.object(PluginRegistry, "get", return_value=simple_dummy_plugin_mock)
    mocker.patch("uuid.uuid4", return_value="12345678")

    plugin_payload = {
        "plugin": "plugin_name",
    }

    resolved_plugin = PluginRegistry.instantiate_plugin(PipelinePhase.EXTRACT_PHASE, plugin_payload)

    simple_dummy_plugin_mock.assert_called_once()
    assert isinstance(resolved_plugin, PluginWrapper)
    assert resolved_plugin.id == "simple_dummy_plugin_12345678"


@pytest.mark.parametrize(
    "plugin_id, plugin_name, phase",  # noqa: PT006
    [
        (
            "extractor_id",
            "mock_extractor",
            PipelinePhase.EXTRACT_PHASE,
        ),
        (
            "transformer_id",
            "mock_transformer",
            PipelinePhase.TRANSFORM_PHASE,
        ),
        (
            "loader_id",
            "mock_loader",
            PipelinePhase.LOAD_PHASE,
        ),
    ],
)
def test_instantiate_plugin(
    plugin_id: str, plugin_name: str, phase: PipelinePhase, simple_dummy_plugin_mock: MockType, mocker: MockerFixture
) -> None:
    mocker.patch.object(PluginRegistry, "get", return_value=simple_dummy_plugin_mock)

    plugin_payload = {
        "id": plugin_id,
        "plugin": plugin_name,
    }

    resolved_plugin = PluginRegistry.instantiate_plugin(phase, plugin_payload)

    simple_dummy_plugin_mock.assert_called_once()
    assert isinstance(resolved_plugin, PluginWrapper)
    assert resolved_plugin.id == plugin_id
    assert resolved_plugin.func.__name__ == simple_dummy_plugin_mock.__name__
