# Standard Imports

# Third Party
import pytest
from plugins.registry import PluginLoader, PluginFactory
from core.models.phases import PipelinePhase, IExtractor, ILoader

# Project


class TestIntegrationPluginLoader:
    
    @pytest.fixture(autouse=True)
    def plugin_loader(self) -> PluginLoader:
        self.loader = PluginLoader()

    @pytest.mark.asyncio
    async def test_load_custom_single_plugin(self):
        # TODO: Investigate why it fails when all tests are run.
        # Ensure the registry is empty before loading the plugin
        assert PluginFactory._registry == {}
        
        # Load the custom plugin
        plugin = {'tests/resources/plugins/custom_extractor.py'}
        self.loader.load_custom_plugins(plugin)

        # Get the loaded custom plugin and run extract data
        plugin_class = PluginFactory._registry[PipelinePhase.EXTRACT_PHASE]["custom_extract"]
        extracted_data = await plugin_class(id='custom_plugin').extract_data()


        assert issubclass(plugin_class, IExtractor)
        assert extracted_data == 'Pandas S3 Loaded Data'

    
    def test_load_custom_multiple_plugins(self):
        # Ensure the registry is empty before loading the plugin
        assert PluginFactory._registry == {}

        # Load the custom plugin
        plugins = {
             '/workspaces/workflow/tests/resources/plugins/custom_extractor.py',
             '/workspaces/workflow/tests/resources/plugins/custom_loader.py'
        }
        self.loader.load_custom_plugins(plugins)

        # Get the custom plugins
        extractor_plugin = PluginFactory._registry[PipelinePhase.EXTRACT_PHASE]["custom_extract"]
        loader_plugin = PluginFactory._registry[PipelinePhase.LOAD_PHASE]["custom_load"]


        assert issubclass(extractor_plugin, IExtractor)
        assert issubclass(loader_plugin, ILoader)