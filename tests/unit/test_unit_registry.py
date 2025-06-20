# Standard Imports
from unittest.mock import MagicMock

# Third Party Imports
import pytest
from pytest_mock import MockerFixture, MockType

# Project
from pipeline_flow.core.registry import PluginRegistry
from pipeline_flow.plugins import IPlugin
from tests.resources import plugins


@pytest.fixture
def simple_dummy_plugin_mock() -> MagicMock:
    mock = MagicMock(side_effect=plugins.SimpleDummyPlugin, spec=IPlugin)
    mock.__name__ = "SimpleDummyPlugin"
    return mock


def test_register_plugin(simple_dummy_plugin_mock: MockType) -> None:
    PluginRegistry.register("dummy_plugin", simple_dummy_plugin_mock)

    assert "dummy_plugin" in PluginRegistry._registry
    assert PluginRegistry._registry["dummy_plugin"] is simple_dummy_plugin_mock


def test_get_plugin(simple_dummy_plugin_mock: MockType) -> None:
    # Registering the plugin
    PluginRegistry.register("another_dummy", simple_dummy_plugin_mock)

    # Fetch it
    plugin_class = PluginRegistry.get("another_dummy")
    assert plugin_class is simple_dummy_plugin_mock


def test_get_nonexistent_plugin() -> None:
    with pytest.raises(ValueError, match="Plugin class was not found for following plugin `nonexistent_plugin`."):
        PluginRegistry.get("nonexistent_plugin")


def test_instantiate_plugin_with_optional_id(mocker: MockerFixture, simple_dummy_plugin_mock: MockType) -> None:
    mocker.patch.object(PluginRegistry, "get", return_value=simple_dummy_plugin_mock)
    mock_uuid = mocker.patch("uuid.uuid4")
    mock_uuid.return_value = MagicMock(hex="12345678")

    plugin_payload = {
        "plugin": "simple_dummy_plugin",
    }

    resolved_plugin = PluginRegistry.instantiate_plugin(plugin_payload)

    simple_dummy_plugin_mock.assert_called_once()

    assert isinstance(resolved_plugin, IPlugin)
    assert resolved_plugin.id == "simple_dummy_plugin_12345678"


@pytest.mark.parametrize(
    ("plugin_id", "plugin_name"),
    [
        (
            "extractor_id",
            "mock_extractor",
        ),
        (
            "transformer_id",
            "mock_transformer",
        ),
        (
            "loader_id",
            "mock_loader",
        ),
    ],
)
def test_instantiate_plugin(
    plugin_id: str, plugin_name: str, simple_dummy_plugin_mock: MockType, mocker: MockerFixture
) -> None:
    mocker.patch.object(PluginRegistry, "get", return_value=simple_dummy_plugin_mock)

    plugin_payload = {
        "id": plugin_id,
        "plugin": plugin_name,
    }

    resolved_plugin = PluginRegistry.instantiate_plugin(plugin_payload)

    simple_dummy_plugin_mock.assert_called_once()

    assert isinstance(resolved_plugin, IPlugin)
    assert resolved_plugin.id == plugin_id
