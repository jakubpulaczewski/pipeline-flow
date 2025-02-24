# Standard Imports
from pathlib import Path

import pytest

# Project Imports
from pipeline_flow.core.plugin_loader import load_custom_plugins
from pipeline_flow.core.registry import PluginRegistry


@pytest.mark.usefixtures("restart_plugin_registry")
def test_load_custom_multiple_plugins() -> None:
    # Ensure the registry is empty before loading the plugin
    assert len(PluginRegistry._registry) == 0

    # Load the plugins
    plugin_path = Path.cwd() / "tests" / "resources" / "custom_plugins.py"

    load_custom_plugins({plugin_path.as_posix()})

    # Verify plugins were loaded
    assert len(PluginRegistry._registry) > 0

    # Comparing Class Name because the classes have different module paths but are the same class
    assert PluginRegistry._registry["custom_extract"].__name__ == "CustomExtractor"
    assert PluginRegistry._registry["custom_load"].__name__ == "CustomLoader"


@pytest.mark.usefixtures("restart_plugin_registry")
def test_load_plugins_empty_set() -> None:
    # Test loading with empty plugin set
    plugins = set()
    load_custom_plugins(plugins)
    assert len(PluginRegistry._registry) == 0


def test_load_plugins_invalid_path() -> None:
    # Test loading with invalid path
    plugins = {"/invalid/path/plugin.py"}
    with pytest.raises(FileNotFoundError):
        load_custom_plugins(plugins)
