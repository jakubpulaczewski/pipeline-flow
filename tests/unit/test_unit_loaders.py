# Standard Imports
from unittest.mock import MagicMock

# Third Party
import pytest


# Project
from core.models.phases import PipelinePhase
from core.loaders import PluginLoader
from core.plugins import PluginRegistry


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

        assert len(PluginRegistry._registry[PipelinePhase.TRANSFORM_PHASE].keys()) > 0