# Standard Imports
from types import FunctionType
from typing import Self

# Third Party Imports
# Project Imports
from core.loaders import load_custom_plugins
from core.models.phases import PipelinePhase
from core.plugins import PluginRegistry


class TestIntegrationPluginLoader:
    def test_load_custom_multiple_plugins(self: Self) -> None:
        # Ensure the registry is empty before loading the plugin
        assert PluginRegistry._registry == {}

        # Load the custom plugin
        plugins = {
            "/workspaces/workflow/tests/resources/custom_plugins.py",
        }
        load_custom_plugins(plugins)

        # Get the custom plugins
        extractor_plugin = PluginRegistry._registry[PipelinePhase.EXTRACT_PHASE]["custom_extract"]
        loader_plugin = PluginRegistry._registry[PipelinePhase.LOAD_PHASE]["custom_load"]

        assert len(PluginRegistry._registry.keys()) == 2

        assert extractor_plugin.__name__ == "custom_extractor"
        assert type(extractor_plugin) is FunctionType

        assert loader_plugin.__name__ == "custom_loader"
        assert type(loader_plugin) is FunctionType

    # def test_load_plugins_success():
    #     #TODO: Create a test for this.
    #     ...
