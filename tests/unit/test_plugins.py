# Standard Imports
from unittest.mock import call, MagicMock
from typing import TypeVar

# Third Party
import pytest

# Project
import core.models as models

from common.config import ETLConfig
from core.plugins import (
    PluginConfig,
    PluginFactory,
    PluginLoader
)

T = TypeVar('T')

class FakeExtractPlugin(models.ExtractInterface):
    id: str = 'FakeExtractPlugin'
    type: str = "s3"
    data_flow_strategy: str = "direct"

    def extract(self) -> T:
        return "Pandas S3 Extracted Data"

class FakeTransformPlugin(models.TransformInterface):
    id: str = "FakeTransformPlugin"
    data_flow_strategy: str = "direct"

    def transform(self, data: T) -> T:
        return f"Pandas Transformed {data}"

class FakeLoadPlugin(models.LoadInterface):
    id: str = "FakeLoadPlugin"
    type: str = "s3"

    def load(self, data: T) -> bool:
        return True

class InvalidPlugin:

    def extract_invalid(self) -> T:
        return "invalid_data"

@pytest.fixture
def setup_plugin_factory():
    PluginFactory._registry = {}  # Ensure a clean state before each test
    yield
    PluginFactory._registry = {}  # Clean up after each test

@pytest.fixture
def mock_validate_plugins(mocker) -> MagicMock:
    return mocker.patch.object(PluginLoader, '_validate_plugins')

@pytest.fixture
def mock_initialize_plugin(mocker) -> MagicMock:
    return mocker.patch.object(PluginLoader, '_initialize_plugin')

@pytest.fixture
def mock_plugin_stage_mapper(mocker) -> MagicMock:
    return mocker.patch.object(PluginConfig, 'plugin_stage_mapper')

@pytest.fixture
def mock_importlib(mocker) -> MagicMock:
    return mocker.patch("core.plugins.importlib.import_module")


class TestPluginFactory:

    @staticmethod
    @pytest.mark.parametrize("test_input, expected", [
        ("exTract", "extract"),
        ("transform", "transform"),
        ("LOAD", "load")
    ])
    def test_lowercase_etl_stage(test_input: str, expected: str) -> None:
        """ Ensures it correctly converts input strings to lowercase. """
        assert PluginFactory.lowercase_etl_stage(test_input) == expected

    @staticmethod
    @pytest.mark.parametrize("etl_stage, plugin_class", [
        ("extracttt", FakeExtractPlugin),
        ("transformm", FakeTransformPlugin),
        ("looooad", FakeLoadPlugin)
    ])
    def test_validate_invalid_etl_stage_plugin_interface(etl_stage: str, plugin_class: ETLConfig.ETL_CALLABLE, mocker) -> None:
        """ Verify that specified etl stage is identifiable from specified one (extract, transform, load)"""
        mock_base_class = mocker.patch.object(ETLConfig, 'get_base_class', return_value=None)

        with pytest.raises(TypeError):
            PluginFactory._validate_plugin_interface(etl_stage, plugin_class)

        # Check if mocks were called properly
        mock_base_class.assert_called_once()
        mock_base_class.assert_called_with(etl_stage)

    @staticmethod
    def test_validate_invalid_subclass_plugin_interface(mocker) -> None:
        """ Verify that the provided class is a subclass of the expected base class """
        mock_base_class = mocker.patch.object(ETLConfig, 'get_base_class', return_value=models.ExtractInterface)

        with pytest.raises(TypeError):
            PluginFactory._validate_plugin_interface('extract', InvalidPlugin)

        # Check if mocks were called properly
        mock_base_class.assert_called_once()
        mock_base_class.assert_called_with('extract')

    @staticmethod
    @pytest.mark.parametrize("etl_stage, plugin_class, base_class", [
        ("extract", FakeExtractPlugin, models.ExtractInterface),
        ("transform", FakeTransformPlugin, models.TransformInterface),
        ("load", FakeLoadPlugin, models.LoadInterface)
    ])
    def test_validate_plugin_interface_valid_etl_stage_and_subclass(
        etl_stage: str,
        plugin_class: ETLConfig.ETL_CALLABLE,
        base_class: ETLConfig.ETL_CALLABLE,
        mocker) -> None:
        """ Ensures that validation works for positive cases"""
        mock_base_class = mocker.patch.object(ETLConfig, 'get_base_class', return_value=base_class)

        PluginFactory._validate_plugin_interface(etl_stage, plugin_class)

    @staticmethod
    def test_register_plugin(setup_plugin_factory) -> None:
        result = PluginFactory.register('extract', 'fake_plugin', FakeExtractPlugin)
        assert result is True
        assert 'extract' in PluginFactory._registry
        assert 'fake_plugin' in PluginFactory._registry['extract']

    @staticmethod
    def test_register_duplicate_plugin(setup_plugin_factory) -> None:
        PluginFactory.register('extract', 'fake_plugin', FakeExtractPlugin)
        result = PluginFactory.register('extract', 'fake_plugin', FakeExtractPlugin)
        assert result is False

    @staticmethod
    def test_remove_nonexistent_plugin(setup_plugin_factory) -> None:
        result = PluginFactory.remove('extract', 'fake_plugin')
        assert result is False

    @staticmethod
    def test_get_plugin(setup_plugin_factory) -> None:
        PluginFactory.register('extract', 'fake_plugin', FakeExtractPlugin)
        plugin_class = PluginFactory.get('extract', 'fake_plugin')
        assert plugin_class is FakeExtractPlugin

    @staticmethod
    def test_get_nonexistent_plugin(setup_plugin_factory) -> None:
        with pytest.raises(ValueError):
            PluginFactory.get('extract', 'fake_plugin')


class TestPluginConfig:

    @staticmethod
    @pytest.mark.parametrize("etl_stage, expected_config", [
        ("extract", PluginConfig.EXTRACT_PLUGINS),
        ("EXTRACT", PluginConfig.EXTRACT_PLUGINS),
        ("load", PluginConfig.LOAD_PLUGINS)
    ])
    def test_get_plugin_stage_mapper(etl_stage: str, expected_config: dict[str, list[str]]) -> None:
        assert PluginConfig.plugin_stage_mapper(etl_stage) == expected_config
        

    @staticmethod
    def test_get_nonexisting_plugin_stage_mapper() -> None:
        assert PluginConfig.plugin_stage_mapper("UNKNOWN") == None
        


class TestPluginLoader:

    
    @staticmethod
    def test_validate_invalid_plugins_type() -> None:
        with pytest.raises(TypeError):
            PluginLoader._validate_plugins(['plugin1', 'plugin2'])

   
    @staticmethod
    def test_validate_invalid_etl_stages() -> None:
        plugins={
            "INVALID_STAGE": ["fake_extract_plugin"]
        }
        with pytest.raises(ValueError):
            PluginLoader._validate_plugins(plugins)


    @staticmethod
    def test_validate_plugins_valid_plugin_type_and_etl_stages() -> None:
        plugins={
            "extract": ["fake_extract_plugin"]
        }
        PluginLoader._validate_plugins(plugins)


    @staticmethod
    def test_initialize_empty_plugin(mock_plugin_stage_mapper) -> None:
        mock_plugin_stage_mapper.return_value = {"fake_engine": []}

        with pytest.raises(ValueError):
            loader = PluginLoader(plugins={}, engine="fake_engine")
            loader._initialize_plugin('extract', [])


    @staticmethod
    def test_initialize_default_plugin(
        mock_validate_plugins, 
        mock_plugin_stage_mapper, 
        mock_importlib
    ) -> None:
        mock_plugin_stage_mapper.return_value = {
            "fake_engine": ['default_plugin1']
        }
    
        loader = PluginLoader({}, engine="fake_engine")
        loader._initialize_plugin('extract', [])

        assert mock_importlib.call_count == 1
        mock_importlib.assert_called_with('plugins.extract.fake_engine.default_plugin1')


    @staticmethod
    def test_initialize_aditional_plugin(
        mock_validate_plugins,
        mock_plugin_stage_mapper,
        mock_importlib
    ) -> None:
        mock_plugin_stage_mapper.return_value = {
            "fake_engine": []
        }

        loader = PluginLoader({}, engine="fake_engine")
        loader._initialize_plugin('extract', ["fake_extract_plugin"])

        assert mock_importlib.call_count == 1
        mock_importlib.assert_called_with('plugins.extract.fake_engine.fake_extract_plugin')

    @staticmethod
    def test_loader_to_initialize_empty_plugins(mock_validate_plugins, mock_initialize_plugin) -> None:        
        loader = PluginLoader(plugins={}, engine='pandas')
        loader.loader()

        mock_validate_plugins.assert_called_once()
        assert mock_initialize_plugin.call_count == 0


    @staticmethod
    def test_loader_to_initialize_one_plugin(mock_validate_plugins, mock_initialize_plugin) -> None:
        plugins={
            "extract": ["fake_extract_plugin"]
        }
        loader = PluginLoader(plugins=plugins, engine='pandas')
        loader.loader()

        mock_validate_plugins.assert_called_once()
        mock_initialize_plugin.assert_called_once()
    

    @staticmethod
    def test_loader_to_initialize_many_plugins(mock_validate_plugins, mock_initialize_plugin) -> None:        
        plugins = {
            "extract": ["fake_extract_plugin"],
            "load": ["fake_load_plugin"]
        }
        loader = PluginLoader(plugins=plugins, engine='pandas')
        loader.loader()

        mock_validate_plugins.assert_called_once()
        assert mock_initialize_plugin.call_count == 2