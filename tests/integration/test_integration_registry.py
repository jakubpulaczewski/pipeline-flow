# Standard Imports

# Third Party Imports
import pytest

# Project
from pipeline_flow.core.registry import PluginRegistry
from tests.resources import plugins


@pytest.mark.usefixtures("restart_plugin_registry")
def test_instantiate_plugin_in_plugin_with_provided_plugin() -> None:
    """Test that the PluginRegistry can instantiate a plugin that contains another plugin as an argument."""
    # Normally the plugin would be registered but because of singleton nature of the PluginRegistry
    # The state of the registry is shared and changed between tests.
    PluginRegistry.register("extended_plugin", plugins.NestedPlugin)
    PluginRegistry.register("simple_plugin_with_nested_plugin", plugins.SimplePluginWithNestedPlugin)

    plugin_payload = {
        "id": "test_id",
        "plugin": "simple_plugin_with_nested_plugin",
        "args": {
            "pagination": {
                "plugin": "extended_plugin",
                "args": {
                    "arg1": "value1",
                    "arg2": "value2",
                },
            }
        },
    }

    resolved_plugin = PluginRegistry.instantiate_plugin(plugin_payload)

    assert resolved_plugin.id == "test_id"
    assert isinstance(resolved_plugin, plugins.SimplePluginWithNestedPlugin)

    # Check the inner plugin (pagination type)
    assert isinstance(resolved_plugin.pagination, plugins.NestedPlugin)


@pytest.mark.usefixtures("restart_plugin_registry")
def test_instantiate_plugin_in_plugin_with_default_plugin() -> None:
    """Test that the PluginRegistry can instantiate a default plugin within another plugin."""
    # Normally the plugin would be registered but because of singleton nature of the PluginRegistry
    # The state of the registry is shared and changed between tests.
    PluginRegistry.register("extended_plugin", plugins.NestedPlugin)
    PluginRegistry.register("simple_plugin_with_nested_plugin", plugins.SimplePluginWithNestedPlugin)

    plugin_payload = {
        "id": "test_id",
        "plugin": "simple_plugin_with_nested_plugin",
        "args": {},
    }

    resolved_plugin = PluginRegistry.instantiate_plugin(plugin_payload)

    assert resolved_plugin.id == "test_id"
    assert isinstance(resolved_plugin, plugins.SimplePluginWithNestedPlugin)

    # Check the inner plugin (pagination type)
    assert isinstance(resolved_plugin.pagination, plugins.NestedPlugin)
    assert resolved_plugin.pagination.arg1 == "default_value1"
    assert resolved_plugin.pagination.arg2 == "default_value2"
