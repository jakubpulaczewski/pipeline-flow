# Standard Imports
from typing import TypeVar
from unittest.mock import MagicMock, call

# Third Party
import pytest

from common.config import ETLConfig
from common.type_def import ETL_PHASE_CALLABLE

# Project
from core.models.extract import IExtractor
from core.plugins import PluginConfig, PluginFactory, PluginLoader
from tests.common.mocks import (
    MockExtractor,
    MockLoad,
    MockTransformELT,
    MockTransformETL,
)


@pytest.fixture
def setup_plugin_env():
    PluginFactory._registry = {}  # Ensure a clean state before each test
    yield
    PluginFactory._registry = {}  # Clean up after each test


@pytest.fixture
def mock_validate_plugins(mocker) -> MagicMock:
    return mocker.patch.object(PluginLoader, "_validate_plugins")


@pytest.fixture
def mock_initialize_plugin(mocker) -> MagicMock:
    return mocker.patch.object(PluginLoader, "_initialize_plugin")


@pytest.fixture
def mock_plugin_phase_mapper(mocker) -> MagicMock:
    return mocker.patch.object(PluginConfig, "plugin_phase_mapper")


@pytest.fixture
def mock_importlib(mocker) -> MagicMock:
    return mocker.patch("core.plugins.importlib.import_module")


class TestPluginFactory:
    """Tests for the PluginFactory class."""

    @staticmethod
    def test_register_plugin(setup_plugin_env) -> None:
        """Test registering a new plugin."""
        result = PluginFactory.register("extract", "mock_plugin", MockExtractor)
        assert result == True
        assert "extract" in PluginFactory._registry
        assert "mock_plugin" in PluginFactory._registry["extract"]

    @staticmethod
    def test_register_duplicate_plugin(setup_plugin_env) -> None:
        """Test registering a duplicate plugin."""
        PluginFactory.register("extract", "fake_plugin", MockExtractor)
        result = PluginFactory.register("extract", "fake_plugin", MockExtractor)
        assert result == False

    @staticmethod
    def test_remove_nonexistent_plugin(setup_plugin_env) -> None:
        """Test removing a nonexistent plugin."""
        result = PluginFactory.remove("extract", "fake_plugin")
        assert result == False

    @staticmethod
    def test_get_plugin(setup_plugin_env) -> None:
        """Test retrieving a registered plugin."""
        PluginFactory.register("extract", "mock_plugin", MockExtractor)
        plugin_class = PluginFactory.get("extract", "mock_plugin")
        assert plugin_class is MockExtractor

    @staticmethod
    def test_get_nonexistent_plugin(setup_plugin_env) -> None:
        """Test retrieving a nonexistent plugin."""
        with pytest.raises(ValueError):
            PluginFactory.get("extract", "fake_plugin")

    @staticmethod
    @pytest.mark.parametrize(
        "test_input, expected",
        [("exTract", "extract"), ("transform", "transform"), ("LOAD", "load")],
    )
    def test_lowercase_etl_phase(test_input: str, expected: str) -> None:
        """Ensures it correctly converts input strings to lowercase."""
        assert PluginFactory.lowercase_etl_phase(test_input) == expected

    @staticmethod
    @pytest.mark.parametrize(
        "etl_phase, plugin_class",
        [
            ("extracttt", MockExtractor),
            ("transformm", MockTransformETL),
            ("looooad", MockLoad),
        ],
    )
    def test_validate_invalid_etl_phase_plugin_interface(
        etl_phase: str, plugin_class: ETL_PHASE_CALLABLE
    ) -> None:
        """Test the validation of process by providing an invalid etl phase."""

        with pytest.raises(ValueError):
            PluginFactory._validate_plugin_interface(etl_phase, plugin_class)

    @staticmethod
    def test_validate_invalid_subclass_plugin_interface(mocker) -> None:
        """Test the subclass inheritance between the plugin with its base class."""
        mock_base_class = mocker.patch.object(
            ETLConfig, "get_base_class", return_value=IExtractor
        )

        with pytest.raises(TypeError):
            PluginFactory._validate_plugin_interface("extract", MockTransformELT)

        # Check if mocks were called properly
        mock_base_class.assert_called_once()
        mock_base_class.assert_called_with("extract")

    @staticmethod
    @pytest.mark.parametrize(
        "etl_phase, plugin_class, base_class",
        [
            ("extract", MockExtractor),
            ("transform", MockTransformETL),
            ("load", MockLoad),
        ],
    )
    def test_validate_plugin_interface_valid_etl_phase_and_subclass(
        etl_phase: str, plugin_class: ETL_PHASE_CALLABLE
    ) -> None:
        """Ensures that validation works for valid cases"""

        PluginFactory._validate_plugin_interface(etl_phase, plugin_class)


class TestPluginLoader:

    @staticmethod
    def test_validate_invalid_plugins_type() -> None:
        with pytest.raises(TypeError):
            PluginLoader._validate_plugins(["plugin1", "plugin2"])

    @staticmethod
    def test_validate_invalid_etl_phases() -> None:
        plugins = {"INVALID_STAGE": ["fake_extract_plugin"]}
        with pytest.raises(ValueError):
            PluginLoader._validate_plugins(plugins)

    @staticmethod
    def test_validate_plugins_valid_plugin_type_and_etl_phases() -> None:
        plugins = {"extract": ["fake_extract_plugin"]}
        PluginLoader._validate_plugins(plugins)

    @staticmethod
    def test_initialize_empty_plugin(mock_plugin_phase_mapper) -> None:
        mock_plugin_phase_mapper.return_value = {"fake_engine": []}

        with pytest.raises(ValueError):
            loader = PluginLoader(plugins={}, engine="fake_engine")
            loader._initialize_plugin("extract", [])

    @staticmethod
    def test_initialize_default_plugin(
        mock_validate_plugins, mock_plugin_phase_mapper, mock_importlib
    ) -> None:
        mock_plugin_phase_mapper.return_value = {"fake_engine": ["default_plugin1"]}

        loader = PluginLoader({}, engine="fake_engine")
        loader._initialize_plugin("extract", [])

        assert mock_importlib.call_count == 1
        mock_importlib.assert_called_with("plugins.extract.fake_engine.default_plugin1")

    @staticmethod
    def test_initialize_aditional_plugin(
        mock_validate_plugins, mock_plugin_phase_mapper, mock_importlib
    ) -> None:
        mock_plugin_phase_mapper.return_value = {"fake_engine": []}

        loader = PluginLoader({}, engine="fake_engine")
        loader._initialize_plugin("extract", ["fake_extract_plugin"])

        assert mock_importlib.call_count == 1
        mock_importlib.assert_called_with(
            "plugins.extract.fake_engine.fake_extract_plugin"
        )

    @staticmethod
    def test_loader_to_initialize_empty_plugins(
        mock_validate_plugins, mock_initialize_plugin
    ) -> None:
        loader = PluginLoader(plugins={}, engine="pandas")
        loader.loader()

        mock_validate_plugins.assert_called_once()
        assert mock_initialize_plugin.call_count == 0

    @staticmethod
    def test_loader_to_initialize_one_plugin(
        mock_validate_plugins, mock_initialize_plugin
    ) -> None:
        plugins = {"extract": ["fake_extract_plugin"]}
        loader = PluginLoader(plugins=plugins, engine="pandas")
        loader.loader()

        mock_validate_plugins.assert_called_once()
        mock_initialize_plugin.assert_called_once()

    @staticmethod
    def test_loader_to_initialize_many_plugins(
        mock_validate_plugins, mock_initialize_plugin
    ) -> None:
        plugins = {"extract": ["fake_extract_plugin"], "load": ["fake_load_plugin"]}
        loader = PluginLoader(plugins=plugins, engine="pandas")
        loader.loader()

        mock_validate_plugins.assert_called_once()
        assert mock_initialize_plugin.call_count == 2
