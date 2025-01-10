# Standard Imports
from types import FunctionType
# Third Party Imports
import pytest


# Project Imports
from core.loaders import PluginLoader
from core.models.phases import PipelinePhase

from plugins.registry import PluginRegistry


class TestIntegrationPluginLoader:
    
    @pytest.fixture(autouse=True)
    def plugin_loader(self) -> PluginLoader:
        self.loader = PluginLoader()

    def test_load_custom_multiple_plugins(self):
        # Ensure the registry is empty before loading the plugin
        assert PluginRegistry._registry == {}

        # Load the custom plugin
        plugins = {
             '/workspaces/workflow/tests/resources/custom_plugins.py',
        }
        self.loader.load_custom_plugins(plugins)

        # Get the custom plugins
        extractor_plugin = PluginRegistry._registry[PipelinePhase.EXTRACT_PHASE]["custom_extract"]
        loader_plugin = PluginRegistry._registry[PipelinePhase.LOAD_PHASE]["custom_load"]

        assert len(PluginRegistry._registry.keys()) == 2

        assert extractor_plugin.__name__ == 'custom_extractor'
        assert type(extractor_plugin) == FunctionType

        assert loader_plugin.__name__ == 'custom_loader'
        assert type(loader_plugin) == FunctionType


    # def test_load_plugins_success():
    #     #TODO: Create a test for this.
    #     ...