# Standard Imports
from unittest.mock import MagicMock

# Third Party
import pytest
import pydantic

from common.type_def import PLUGIN_BASE_CALLABLE

# Project
from core.models.extract import IExtractor
from core.models.phases import PipelinePhase
from core.plugins import PluginConfig, PluginFactory, PluginLoader
from tests.common.constants import EXTRACT_PHASE, LOAD_PHASE, ELT, ETL
from tests.common.mocks import MockExtractor, MockLoad, MockLoadTransform, MockTransform


@pytest.fixture(autouse=True)
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
    def test_register_plugin() -> None:
        """Test registering a new plugin."""
        result = PluginFactory.register(EXTRACT_PHASE, "mock_plugin", MockExtractor)
        assert result == True
        assert PipelinePhase.EXTRACT_PHASE in PluginFactory._registry
        assert PluginFactory._registry[PipelinePhase.EXTRACT_PHASE] == {"mock_plugin": MockExtractor}

    @staticmethod
    def test_register_duplicate_plugin() -> None:
        """Test registering a duplicate plugin."""
        result = PluginFactory.register(EXTRACT_PHASE, "fake_plugin", MockExtractor)
        assert result == True
        result = PluginFactory.register(EXTRACT_PHASE, "fake_plugin", MockExtractor)
        assert result == False

    @staticmethod
    def test_remove_plugin() -> None:
        """A test that validates the `removal` functionality of the PluginFactory."""
        plugin_name = "fake_plugin"
        # Add the plugin
        PluginFactory.register(EXTRACT_PHASE, plugin_name, MockExtractor)

        # Removes the plugin
        result = PluginFactory.remove(EXTRACT_PHASE, plugin_name)
        assert result == True

        # Try to remove the same plugin, should return False
        result = PluginFactory.remove(EXTRACT_PHASE, plugin_name)
        assert result == False

    @staticmethod
    def test_remove_nonexistent_plugin() -> None:
        """Test removing a nonexistent plugin."""
        result = PluginFactory.remove(EXTRACT_PHASE, "fake_plugin")
        assert result == False

    @staticmethod
    def test_get_plugin() -> None:
        plugin_name = "mock_plugin"
        """Test retrieving a registered plugin."""
        # Registering the plugin
        PluginFactory.register(EXTRACT_PHASE, plugin_name, MockExtractor)

        # Fetch it
        plugin_class = PluginFactory.get(EXTRACT_PHASE, plugin_name)
        assert plugin_class is MockExtractor

        # Try to get the same plugin plugin, after its removed, should raise an exception
        PluginFactory.remove(EXTRACT_PHASE, plugin_name)

        with pytest.raises(ValueError):
            PluginFactory.get(EXTRACT_PHASE, plugin_name)

    @staticmethod
    def test_get_nonexistent_plugin() -> None:
        """Test retrieving a nonexistent plugin."""
        with pytest.raises(ValueError):
            PluginFactory.get(EXTRACT_PHASE, "fake_plugin")

    @staticmethod
    @pytest.mark.parametrize(
        "etl_phase",
        [
            ("extracttt"),
            ("transformm"),
            ("looooad"),
        ],
    )
    def test_validate_invalid_pipeline_phase_name(etl_phase: str) -> None:
        """Test the validation process by providing an invalid pipeline phase name."""

        with pytest.raises(KeyError):
            PluginFactory._validate_plugin_registration(etl_phase, MockLoadTransform)

    @staticmethod
    def test_validate_invalid_subclass_plugin_interface(mocker) -> None:
        """Test the subclass inheritance between the plugin with its base class."""
        mock_base_class = mocker.patch.object(
            PipelinePhase, "get_plugin_interface_for_phase", return_value=IExtractor
        )

        with pytest.raises(TypeError):
            PluginFactory._validate_plugin_registration(EXTRACT_PHASE, MockLoadTransform)

        # Check if mocks were called properly
        mock_base_class.assert_called_once()
        mock_base_class.assert_called_with(EXTRACT_PHASE)



# class TestPluginLoader:

#     @staticmethod
#     def test_validate_invalid_plugins_type() -> None:
#         with pytest.raises(TypeError):
#             PluginLoader._validate_plugins(["plugin1", "plugin2"])

#     @staticmethod
#     def test_validate_invalid_etl_phases() -> None:
#         plugins = {"INVALID_STAGE": ["fake_extract_plugin"]}
#         with pytest.raises(ValueError):
#             PluginLoader._validate_plugins(plugins)

#     @staticmethod
#     def test_validate_plugins_valid_plugin_type_and_etl_phases() -> None:
#         plugins = {EXTRACT_PHASE: ["fake_extract_plugin"]}
#         PluginLoader._validate_plugins(plugins)

#     @staticmethod
#     def test_initialize_empty_plugin(mock_plugin_phase_mapper) -> None:
#         mock_plugin_phase_mapper.return_value = {"fake_engine": []}

#         with pytest.raises(ValueError):
#             loader = PluginLoader(plugins={}, engine="fake_engine")
#             loader._initialize_plugin(EXTRACT_PHASE, [])

#     @staticmethod
#     def test_initialize_default_plugin(
#         mock_plugin_phase_mapper, mock_importlib
#     ) -> None:
#         mock_plugin_phase_mapper.return_value = {"fake_engine": ["default_plugin1"]}

#         loader = PluginLoader({}, engine="fake_engine")
#         loader._initialize_plugin(EXTRACT_PHASE, [])

#         assert mock_importlib.call_count == 1
#         mock_importlib.assert_called_with("plugins.extract.fake_engine.default_plugin1")

#     @staticmethod
#     def test_initialize_aditional_plugin(
#         mock_plugin_phase_mapper, mock_importlib
#     ) -> None:
#         mock_plugin_phase_mapper.return_value = {"fake_engine": []}

#         loader = PluginLoader({}, engine="fake_engine")
#         loader._initialize_plugin(EXTRACT_PHASE, ["fake_extract_plugin"])

#         assert mock_importlib.call_count == 1
#         mock_importlib.assert_called_with(
#             "plugins.extract.fake_engine.fake_extract_plugin"
#         )

#     @staticmethod
#     def test_loader_to_initialize_empty_plugins(
#         mock_validate_plugins, mock_initialize_plugin
#     ) -> None:
#         loader = PluginLoader(plugins={}, engine="pandas")
#         loader.loader()

#         mock_validate_plugins.assert_called_once()
#         assert mock_initialize_plugin.call_count == 0

#     @staticmethod
#     def test_loader_to_initialize_one_plugin(
#         mock_validate_plugins, mock_initialize_plugin
#     ) -> None:
#         plugins = {EXTRACT_PHASE: ["fake_extract_plugin"]}
#         loader = PluginLoader(plugins=plugins, engine="pandas")
#         loader.loader()

#         mock_validate_plugins.assert_called_once()
#         mock_initialize_plugin.assert_called_once()

#     @staticmethod
#     def test_loader_to_initialize_many_plugins(
#         mock_validate_plugins, mock_initialize_plugin
#     ) -> None:
#         plugins = {EXTRACT_PHASE: ["fake_extract_plugin"], LOAD_PHASE: ["fake_load_plugin"]}
#         loader = PluginLoader(plugins=plugins, engine="pandas")
#         loader.loader()

#         mock_validate_plugins.assert_called_once()
#         assert mock_initialize_plugin.call_count == 2
