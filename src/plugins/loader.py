import importlib

class PluginModuleInterface:
    """ A plugin has a single function called initialize."""

    @staticmethod
    def initialize() -> None:
        """initialize the plugin module."""
        ...


def load_plugins(plugins: list[str]) -> None:
    """ Load the plugins defined in the plugins list. """
    for plugin_name in plugins:
        plugin = importlib.import_module(f"plugins.{plugin_name}")
        plugin.initialize()