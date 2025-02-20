# Standard Imports
from unittest.mock import MagicMock

# Third Party
import pytest
from pytest_mock import MockerFixture

# Project
from pipeline_flow.core import plugin_loader
from pipeline_flow.core.registry import PluginRegistry


def test_load_plugin_from_file_new_module(mocker: MockerFixture) -> None:
    mock_spec = mocker.patch("importlib.util.spec_from_file_location")
    mock_module = mocker.patch("importlib.util.module_from_spec")
    mock_spec.return_value.loader.return_value_exec_module = MagicMock()

    plugin_loader._load_plugin_from_file("new_folder/subfolder/file.py")

    mock_spec.assert_called_once_with("new_folder.subfolder.file", "new_folder/subfolder/file.py")
    mock_module.assert_called_once()
    mock_spec.return_value.loader.exec_module.assert_called_once()


def test_load_custom_plugins(mocker: MockerFixture) -> None:
    mock_load_plugin = mocker.patch.object(plugin_loader, "_load_plugin_from_file")

    custom_files = {"file1.py", "file2.py"}
    plugin_loader.load_custom_plugins(custom_files)

    mock_load_plugin.assert_any_call("file1.py")
    mock_load_plugin.assert_any_call("file2.py")
    assert len(mock_load_plugin.mock_calls) == 2
