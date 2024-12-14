
# This would need to be changed here.
import plugins # This will automatically register built-in plugins

from tests.resources.plugins.custom_extractor import CustomExtractor

# @pytest.fixture
# def mock_load_plugin_from_file(mocker) -> MagicMock:
#     return mocker.patch('plugins.registry.load_plugin_from_file')
# def test_load_plugins_from_path_using_dir(mock_load_plugin_from_file, custom_plugin_directory) -> None:
#     load_plugins_from_path([custom_plugin_directory])

#     print(mock_load_plugin_from_file.mock_calls)
#     assert len(mock_load_plugin_from_file.mock_calls) == 2
#     assert call('tests/resources/plugins/custom_extractor.py') in mock_load_plugin_from_file.mock_calls
#     assert call('tests/resources/plugins/custom_loader.py') in mock_load_plugin_from_file.mock_calls


# def test_load_plugins_from_path_using_file(mock_load_plugin_from_file, custom_plugin_file_path) -> None:

#     load_plugins_from_path([custom_plugin_file_path])

#     mock_load_plugin_from_file.assert_called_once_with('tests/resources/plugins/custom_extractor.py')


# @pytest.mark.asyncio
# async def test_load_plugin_from_file_success(custom_plugin_file_path) -> None:
#     # Register the dynamically loaded module under a consistent name
#     load_plugin_from_file(custom_plugin_file_path)

#     plugin_class = PluginFactory._registry[PipelinePhase.EXTRACT_PHASE]["custom_extract"]
#     extracted_data = await plugin_class(id='custom_plugin').extract_data()

#     # Compare classes by their full-qualified name
#     assert plugin_class.__module__ == CustomExtractor.__module__
#     assert plugin_class.__name__ == CustomExtractor.__name__
#     assert issubclass(plugin_class, IExtractor)

#     assert extracted_data == 'Pandas S3 Loaded Data'