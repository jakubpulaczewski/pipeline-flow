# Standard Imports
from functools import wraps

# Third Party
import pytest

# Project
import tests.resources.mocks as mocks

from core.models.phases import PipelinePhase
from plugins.registry import PluginRegistry, PluginWrapper, plugin
from tests.resources.constants import EXTRACT_PHASE


def mock_plugin(id: str):
    @wraps(mock_plugin)
    async def inner():
        return "EXTRACT OF DATA"
    return inner

def mock_plugin_no_params():
    @wraps(mock_plugin_no_params)
    async def inner():
        return "DATA"
    return inner



class TestUnitPluginDecorator:

    @staticmethod
    def test_plugin_with_id() -> None:
        plugin_func = plugin(PipelinePhase.EXTRACT_PHASE, "mock_plugin")(mock_plugin)

        result = plugin_func(id='mock_plugin_id')

        assert result.__name__ == mock_plugin(id='mock_plugin_id').__name__

    @staticmethod
    def test_plugin_with_optional_id(mocker) -> None:
        mocker.patch('uuid.uuid4', return_value='12345678-1234-5678-1234-567812345678')

        plugin_func = plugin(PipelinePhase.EXTRACT_PHASE, "mock_plugin")(mock_plugin_no_params)

        assert plugin_func().__name__ == mock_plugin_no_params().__name__

    @staticmethod
    def test_get_plugin_using_plugin_decorator() -> None:
        plugin(PipelinePhase.EXTRACT_PHASE, "mock_plugin")(mock_plugin)

        plugin_func = PluginRegistry.get(PipelinePhase.EXTRACT_PHASE, "mock_plugin")
        result = plugin_func(id='mock_plugin_id')

        assert result.__name__ == mock_plugin(id='mock_plugin_id').__name__


class TestUnitPluginRegistry:

    @staticmethod
    def test_register_plugin() -> None:
        result = PluginRegistry.register(EXTRACT_PHASE, "mock_plugin", mock_plugin)
        assert result == True
        assert PipelinePhase.EXTRACT_PHASE in PluginRegistry._registry
        assert PluginRegistry._registry[PipelinePhase.EXTRACT_PHASE] == {"mock_plugin": mock_plugin}

    @staticmethod
    def test_register_duplicate_plugin() -> None:
        result = PluginRegistry.register(EXTRACT_PHASE, "fake_plugin", mock_plugin)
        assert result == True
        result = PluginRegistry.register(EXTRACT_PHASE, "fake_plugin", mock_plugin)
        assert result == False

    @staticmethod
    def test_remove_plugin() -> None:
        plugin_name = "fake_plugin"
        # Add the plugin
        PluginRegistry.register(EXTRACT_PHASE, plugin_name, mock_plugin)

        # Removes the plugin
        result = PluginRegistry.remove(EXTRACT_PHASE, plugin_name)
        assert result == True

        # Try to remove the same plugin, should return False
        result = PluginRegistry.remove(EXTRACT_PHASE, plugin_name)
        assert result == False

    @staticmethod
    def test_remove_nonexistent_plugin() -> None:
        result = PluginRegistry.remove(EXTRACT_PHASE, "fake_plugin")
        assert result == False

    @staticmethod
    def test_get_plugin() -> None:
        plugin_name = "mock_plugin"

        # Registering the plugin
        PluginRegistry.register(EXTRACT_PHASE, plugin_name, mock_plugin)

        # Fetch it
        plugin_class = PluginRegistry.get(EXTRACT_PHASE, plugin_name)
        assert plugin_class is mock_plugin

        # Try to get the same plugin plugin, after its removed, should raise an exception
        PluginRegistry.remove(EXTRACT_PHASE, plugin_name)

        with pytest.raises(ValueError):
            PluginRegistry.get(EXTRACT_PHASE, plugin_name)


    @staticmethod
    def test_get_nonexistent_plugin() -> None:
        with pytest.raises(ValueError):
            PluginRegistry.get(EXTRACT_PHASE, "fake_plugin")


    @staticmethod
    @pytest.mark.parametrize("id, plugin, phase, func", [
        ["extractor_id", "mocks.mock_extractor", PipelinePhase.EXTRACT_PHASE, mocks.mock_extractor],
        ["transformer_id", "mocks.mock_transformer", PipelinePhase.TRANSFORM_PHASE, mocks.mock_transformer],
        ["loader_id", "mocks.mock_loader", PipelinePhase.LOAD_PHASE, mocks.mock_loader],
    ])
    def test_instantiate_plugin(id, plugin, phase, func, mocker) -> None:
        mocker.patch.object(PluginRegistry, 'get').return_value = func

        plugin_payload = {
            "id": id,
            "plugin": plugin,
        }

        resolved_plugin = PluginRegistry.instantiate_plugin(phase, plugin_payload)
        assert isinstance(resolved_plugin, PluginWrapper)
