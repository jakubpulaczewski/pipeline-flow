# Standard Imports
import sys
from unittest.mock import MagicMock

# Third Party
import pytest


# Project
from core.models.phases import PipelinePhase
from plugins.registry import PluginRegistry
from tests.resources.constants import EXTRACT_PHASE, TRANSFORM_PHASE
from tests.resources.mocks import MockExtractor, MockLoadTransform


class TestUnitPluginRegistry:
    """Tests for the PluginRegistry class."""

    @staticmethod
    def test_register_plugin() -> None:
        """Test registering a new plugin."""
        result = PluginRegistry.register(EXTRACT_PHASE, "mock_plugin", MockExtractor)
        assert result == True
        assert PipelinePhase.EXTRACT_PHASE in PluginRegistry._registry
        assert PluginRegistry._registry[PipelinePhase.EXTRACT_PHASE] == {"mock_plugin": MockExtractor}

    @staticmethod
    def test_register_duplicate_plugin() -> None:
        """Test registering a duplicate plugin."""
        result = PluginRegistry.register(EXTRACT_PHASE, "fake_plugin", MockExtractor)
        assert result == True
        result = PluginRegistry.register(EXTRACT_PHASE, "fake_plugin", MockExtractor)
        assert result == False

    @staticmethod
    def test_remove_plugin() -> None:
        """A test that validates the `removal` functionality of the PluginRegistry."""
        plugin_name = "fake_plugin"
        # Add the plugin
        PluginRegistry.register(EXTRACT_PHASE, plugin_name, MockExtractor)

        # Removes the plugin
        result = PluginRegistry.remove(EXTRACT_PHASE, plugin_name)
        assert result == True

        # Try to remove the same plugin, should return False
        result = PluginRegistry.remove(EXTRACT_PHASE, plugin_name)
        assert result == False

    @staticmethod
    def test_remove_nonexistent_plugin() -> None:
        """Test removing a nonexistent plugin."""
        result = PluginRegistry.remove(EXTRACT_PHASE, "fake_plugin")
        assert result == False

    @staticmethod
    def test_get_plugin() -> None:
        plugin_name = "mock_plugin"
        """Test retrieving a registered plugin."""
        # Registering the plugin
        PluginRegistry.register(EXTRACT_PHASE, plugin_name, MockExtractor)

        # Fetch it
        plugin_class = PluginRegistry.get(EXTRACT_PHASE, plugin_name)
        assert plugin_class is MockExtractor

        # Try to get the same plugin plugin, after its removed, should raise an exception
        PluginRegistry.remove(EXTRACT_PHASE, plugin_name)

        with pytest.raises(ValueError):
            PluginRegistry.get(EXTRACT_PHASE, plugin_name)

    @staticmethod
    def test_get_nonexistent_plugin() -> None:
        """Test retrieving a nonexistent plugin."""
        with pytest.raises(ValueError):
            PluginRegistry.get(EXTRACT_PHASE, "fake_plugin")


    @staticmethod
    def test_validate_invalid_subclass_plugin_interface() -> None:
        """Test the subclass inheritance between the plugin with its base class."""

        with pytest.raises(TypeError):
            PluginRegistry._validate_plugin_interface(EXTRACT_PHASE, MockLoadTransform)

    @staticmethod
    def test_validate_transform_core_simple_transformation() -> None:
        def native_transform(id: str):
            def inner(data) -> list[str]:
                return [
                    item.capitalize() for item in data
                ]
            return inner
        PluginRegistry._validate_plugin_interface(TRANSFORM_PHASE, native_transform(id="id123"))
