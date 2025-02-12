# Standard Imports
import pytest

# Project Imports
from pipeline_flow.core.plugin_loader import load_custom_plugins
from pipeline_flow.core.registry import PluginRegistry


def test_load_custom_multiple_plugins() -> None:
    # Ensure the registry is empty before loading the plugin
    assert PluginRegistry._registry == {}

    # Load the plugins
    plugins = {"/workspaces/pipeline-flow/tests/resources/custom_plugins.py"}
    load_custom_plugins(plugins)

    # Verify plugins were loaded
    assert len(PluginRegistry._registry) > 0

    # Comparing Class Name because the classes have different module paths but are the same class
    assert PluginRegistry._registry["custom_extract"].__name__ == "CustomExtractor"
    assert PluginRegistry._registry["custom_load"].__name__ == "CustomLoader"


def test_load_plugins_empty_set() -> None:
    # Test loading with empty plugin set
    plugins = set()
    load_custom_plugins(plugins)
    assert PluginRegistry._registry == {}


def test_load_plugins_invalid_path() -> None:
    # Test loading with invalid path
    plugins = {"/invalid/path/plugin.py"}
    with pytest.raises(FileNotFoundError):
        load_custom_plugins(plugins)
