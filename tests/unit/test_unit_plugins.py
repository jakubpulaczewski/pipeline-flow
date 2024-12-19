# Standard Imports
import sys
from unittest.mock import MagicMock

# Third Party
import pytest


# Project
from core.models.phases import PipelinePhase
from plugins.registry import PluginFactory, PluginLoader
from tests.resources.constants import EXTRACT_PHASE, TRANSFORM_PHASE
from tests.resources.mocks import MockExtractor, MockLoadTransform



class TestUnitPluginLoader:

    @pytest.fixture(autouse=True)
    def plugin_loader(self) -> PluginLoader:
        self.loader = PluginLoader()

    def test_load_plugin_from_file_new_module(self, mocker) -> None:
        mock_spec = mocker.patch("importlib.util.spec_from_file_location")
        mock_module = mocker.patch("importlib.util.module_from_spec")
        mock_spec.return_value.loader.exec_module = MagicMock()

        plugin = 'new_folder/subfolder/file.py'

        self.loader.load_plugin_from_file(plugin)


        mock_spec.assert_called_once_with("new_folder.subfolder.file", 'new_folder/subfolder/file.py')
        mock_module.assert_called_once()
        mock_spec.return_value.loader.exec_module.assert_called_once()

    
    def test_load_custom_plugins(self, mocker) -> None:
        mock_load_plugin = mocker.patch.object(self.loader, "load_plugin_from_file")

        custom_files = {"file1.py", "file2.py"}
        self.loader.load_custom_plugins(custom_files)

        mock_load_plugin.assert_any_call('file1.py')
        mock_load_plugin.assert_any_call('file2.py')
        assert len(mock_load_plugin.mock_calls) == 2


    def test_load_core_engine_transformations(self) -> None:
        self.loader.load_core_engine_transformations('native')

        assert len(PluginFactory._registry[PipelinePhase.TRANSFORM_PHASE].keys()) > 0



class TestUnitPluginFactory:
    """Tests for the PluginFactory class."""

    @staticmethod
    def test_register_plugin() -> None:
        """Test registering a new plugin."""
        result = PluginFactory.register(EXTRACT_PHASE, "mock_plugin", MockExtractor)
        assert result == True
        assert PipelinePhase.EXTRACT_PHASE in PluginFactory._registry
        assert PluginFactory._registry[PipelinePhase.EXTRACT_PHASE] == {"mock_plugin": MockExtractor}

    @staticmethod
    def test_register_duplicate_plugin() -> None:
        """Test registering a duplicate plugin."""
        result = PluginFactory.register(EXTRACT_PHASE, "fake_plugin", MockExtractor)
        assert result == True
        result = PluginFactory.register(EXTRACT_PHASE, "fake_plugin", MockExtractor)
        assert result == False

    @staticmethod
    def test_remove_plugin() -> None:
        """A test that validates the `removal` functionality of the PluginFactory."""
        plugin_name = "fake_plugin"
        # Add the plugin
        PluginFactory.register(EXTRACT_PHASE, plugin_name, MockExtractor)

        # Removes the plugin
        result = PluginFactory.remove(EXTRACT_PHASE, plugin_name)
        assert result == True

        # Try to remove the same plugin, should return False
        result = PluginFactory.remove(EXTRACT_PHASE, plugin_name)
        assert result == False

    @staticmethod
    def test_remove_nonexistent_plugin() -> None:
        """Test removing a nonexistent plugin."""
        result = PluginFactory.remove(EXTRACT_PHASE, "fake_plugin")
        assert result == False

    @staticmethod
    def test_get_plugin() -> None:
        plugin_name = "mock_plugin"
        """Test retrieving a registered plugin."""
        # Registering the plugin
        PluginFactory.register(EXTRACT_PHASE, plugin_name, MockExtractor)

        # Fetch it
        plugin_class = PluginFactory.get(EXTRACT_PHASE, plugin_name)
        assert plugin_class is MockExtractor

        # Try to get the same plugin plugin, after its removed, should raise an exception
        PluginFactory.remove(EXTRACT_PHASE, plugin_name)

        with pytest.raises(ValueError):
            PluginFactory.get(EXTRACT_PHASE, plugin_name)

    @staticmethod
    def test_get_nonexistent_plugin() -> None:
        """Test retrieving a nonexistent plugin."""
        with pytest.raises(ValueError):
            PluginFactory.get(EXTRACT_PHASE, "fake_plugin")


    @staticmethod
    def test_validate_invalid_subclass_plugin_interface() -> None:
        """Test the subclass inheritance between the plugin with its base class."""

        with pytest.raises(TypeError):
            PluginFactory._validate_plugin_interface(EXTRACT_PHASE, MockLoadTransform)

    @staticmethod
    def test_validate_transform_core_simple_transformation() -> None:
        def native_transform(id: str):
            def inner(data) -> list[str]:
                return [
                    item.capitalize() for item in data
                ]
            return inner
        PluginFactory._validate_plugin_interface(TRANSFORM_PHASE, native_transform(id="id123"))
