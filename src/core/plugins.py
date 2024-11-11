# Standard Imports
import importlib
from typing import Type, TypeVar

# Project Imports
from common.config import ETLConfig
from common.type_def import ETL_PHASE_CALLABLE
from common.utils.logger import setup_logger

# Third Party Imports


logger = setup_logger(__name__)

PluginClass = Type[TypeVar("PluginType")]


class PluginFactory:
    """A Plugin Factory that dynamically registers, removes and fetches plugins.

    Registry example: {'extract': {'s3': S3Plugin}}

    """

    _registry: dict[str, dict[str, ETL_PHASE_CALLABLE]] = {}

    @staticmethod
    def lowercase_etl_phase(etl_phase: str) -> str:
        return etl_phase.lower()

    @staticmethod
    def _validate_plugin_interface(etl_phase: str, plugin_class: PluginClass) -> bool:
        """
        Validates that the provided plugin_class is a valid implementation
        for the given ETL phase.

        Args:
            etl_phase (str): The ETL phase (e.g., "extract", "transform", "load").
            plugin_class (type): The class to validate.

        Returns:
            bool: True if validation succeeds.

        Raises:
            ValueError: If the ETL phase is invalid.
            TypeError: If the plugin_class does not inherit from the required base class.
        """
        phase_class = ETLConfig.get_base_class(etl_phase)

        if not issubclass(plugin_class, phase_class):
            raise TypeError(
                f"Plugin class '{plugin_class.__name__}' must be a subclass of '{phase_class.__name__}' "
                f"to be registered under the '{etl_phase}' phase."
            )

        return True

    @classmethod
    def register(cls, etl_phase: str, plugin: str, plugin_class: PluginClass) -> bool:
        """Regisers a plugin for a given ETL type and plugin."""
        etl_phase = cls.lowercase_etl_phase(etl_phase)

        cls._validate_plugin_interface(etl_phase, plugin_class)

        # Initialise the ETL phase in the registry.
        if etl_phase not in cls._registry:
            cls._registry[etl_phase] = {}

        # Check if the plugin has been registered.
        if plugin in cls._registry[etl_phase]:
            logger.warning(
                "Plugin for `%s` phase already exists in PluginFactory class.",
                etl_phase,
            )
            return False

        cls._registry[etl_phase][plugin] = plugin_class
        logger.debug(
            "Plugin `%s` for phase `%s` registered successfully.", plugin, etl_phase
        )
        return True

    @classmethod
    def remove(cls, etl_phase: str, plugin: str) -> bool:
        """Remove a plugin for a given ETL type and plugin."""
        etl_phase = cls.lowercase_etl_phase(etl_phase)

        if etl_phase in cls._registry and plugin in cls._registry[etl_phase]:
            del cls._registry[etl_phase][plugin]
            logger.debug(
                "Plugin '%s' for phase '%s' has been removed.", plugin, etl_phase
            )

            # Remove the ETL type dict if empty after removing the plugin.
            if not cls._registry[etl_phase]:
                logger.debug("Stage '%s' has been removed.", etl_phase)
                del cls._registry[etl_phase]
            return True
        return False

    @classmethod
    def get(cls, etl_phase: str, plugin: str) -> ETL_PHASE_CALLABLE:
        """Retrieve a plugin for a given ETL type and plugin."""
        etl_phase = cls.lowercase_etl_phase(etl_phase)

        etl_class = cls._registry.get(etl_phase, {}).get(plugin, None)
        if not etl_class:
            raise ValueError("Plugin class '%s' was not found", etl_class)
        logger.debug("Plugin class '%s' has been successfully retrieved.", etl_class)
        return etl_class


class PluginConfig:
    """A config plugin class that contains default Plugins"""

    _DEFAULT_EXTRACT_PLUGINS = {"pandas": ["s3"]}
    _DEFAULT_LOAD_PLUGINS = {"pandas": ["s3"]}

    # Mapping ETL phases to their respective default plugins
    _STAGE_PLUGIN_MAP = {
        ETLConfig.EXTRACT_PHASE: _DEFAULT_EXTRACT_PLUGINS,
        ETLConfig.LOAD_PHASE: _DEFAULT_LOAD_PLUGINS,
    }

    @classmethod
    def plugin_phase_mapper(
        cls: "PluginConfig", etl_phase: str
    ) -> dict[str, list[str]] | None:
        return cls._STAGE_PLUGIN_MAP.get(etl_phase.lower())


class PluginLoader:
    """A plugin loader class that initialiazes plugins at run time.

    An example of plugins using yaml syntax:

    plugins:
        extract: [s3]
        load: [s3, jdbc]
    """

    def __init__(self, plugins: dict[str, list[str]], engine: str) -> None:
        self._validate_plugins(plugins)
        self.plugins = plugins
        self.engine = engine

    @staticmethod
    def _validate_plugins(plugins: dict[str, list[str]]) -> None:
        if not isinstance(plugins, dict):
            logger.error(
                "Invalid type for 'plugins': expected 'dict', got '%s'",
                type(plugins).__name__,
            )
            raise TypeError(
                f"Expected plugins to be a dict, got {type(plugins).__name__}"
            )

        allowed_phases = {ETLConfig.EXTRACT_PHASE, ETLConfig.LOAD_PHASE}

        if not all(key in allowed_phases for key in plugins.keys()):
            err_message = "Plugins must only contain 'EXTRACT' and 'LOAD' keys."
            logger.error(err_message)
            raise ValueError(err_message)

    def _initialize_plugin(self, phase: str, plugin_list: list[str]) -> None:
        """Initializes plugin modules for a given ETL phase."""
        logger.info(
            "Initializing plugin list %s for ETL phase %s with engine %s",
            plugin_list,
            phase,
            self.engine,
        )

        default_plugins = PluginConfig.plugin_phase_mapper(phase)[self.engine]
        combined_plugins = set(default_plugins + plugin_list)

        if not combined_plugins:
            raise ValueError("No plugins to load for phase {}".format(phase))

        for plugin in combined_plugins:
            try:
                module = importlib.import_module(
                    f"plugins.{phase}.{self.engine}.{plugin}"
                )
                module.initialize()
            except ModuleNotFoundError:
                logger.error(
                    "Plugin module not found: plugins.%s.%s.%s",
                    phase,
                    self.engine,
                    plugin,
                )
                raise

        logger.info("All plugins have been initialized.")

    def loader(self) -> None:
        """Load the plugins defined in the plugins list."""
        # For each phase in plugins, initialize the plugins for that phase.
        for phase, plugin_list in self.plugins.items():
            self._initialize_plugin(phase.lower(), plugin_list)


class PluginModuleInterface:
    """A plugin has a single function called initialize."""

    @staticmethod
    def initialize() -> None:
        """initialize the plugin module."""
        raise NotImplementedError(
            "The method has not been implemented. You must implement it in your python module."
        )
