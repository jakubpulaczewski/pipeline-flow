# Standard Imports
import importlib
from typing import Type, TypeVar
# Third Party Imports

# Project Imports
from common.config import ETLConfig
from common.utils.logger import setup_logger

logger = setup_logger(__name__)

PluginClass = Type[TypeVar('PluginType')]

class PluginFactory:
    """A Plugin Factory that dynamically registers, removes and fetches plugins.

    Registry example: {'extract': {'s3': S3Plugin}}

    """

    _registry: dict[str, dict[str, ETLConfig.ETL_CALLABLE]] = {}

    @staticmethod
    def lowercase_etl_stage(etl_stage: str) -> str:
        return etl_stage.lower()


    @staticmethod
    def _validate_plugin_interface(etl_stage: str, plugin_class: PluginClass) -> bool:
        stage_class = ETLConfig.get_base_class(etl_stage)
        if stage_class is None:
            error_message = ("Not Identifiable ETL Stage %s", etl_stage)
            logger.error(error_message)
            raise TypeError(error_message)

        if not issubclass(plugin_class, stage_class):
            error_message = f"{plugin_class.__name__} is not a subclass of {stage_class.__name__}"
            logger.error(error_message)
            raise TypeError(error_message)

        return True
    
    @classmethod
    def register(cls, etl_stage: str, plugin: str, plugin_class: PluginClass) -> bool:
        """Regisers a plugin for a given ETL type and plugin."""
        etl_stage = cls.lowercase_etl_stage(etl_stage)

        cls._validate_plugin_interface(etl_stage, plugin_class)

        # Initialise the ETL stage in the registry.
        if etl_stage not in cls._registry:
            cls._registry[etl_stage] = {}

        # Check if the plugin has been registered.
        if plugin in cls._registry[etl_stage]:
            logger.warning(
                "Plugin for `%s` stage already exists in PluginFactory class.",
                etl_stage,
            )
            return False

        cls._registry[etl_stage][plugin] = plugin_class
        logger.debug(
            "Plugin `%s` for stage `%s` registered successfully.", plugin, etl_stage
        )
        return True

    @classmethod
    def remove(cls, etl_stage: str, plugin: str) -> bool:
        """Remove a plugin for a given ETL type and plugin."""
        etl_stage = cls.lowercase_etl_stage(etl_stage)

        if etl_stage in cls._registry and plugin in cls._registry[etl_stage]:
            del cls._registry[etl_stage][plugin]
            logger.debug(
                "Plugin '%s' for stage '%s' has been removed.", plugin, etl_stage
            )

            # Remove the ETL type dict if empty after removing the plugin.
            if not cls._registry[etl_stage]:
                logger.debug("Stage '%s' has been removed.", etl_stage)
                del cls._registry[etl_stage]
            return True
        return False

    @classmethod
    def get(cls, etl_stage: str, plugin: str) -> ETLConfig.ETL_CALLABLE:
        """Retrieve a plugin for a given ETL type and plugin."""
        etl_stage = cls.lowercase_etl_stage(etl_stage)

        etl_class = cls._registry.get(etl_stage, {}).get(plugin, None)
        if not etl_class:
            raise ValueError("Plugin class '%s' was not found", etl_class)
        logger.debug("Plugin class '%s' has been successfully retrieved.", etl_class)
        return etl_class

class PluginConfig:
    """ A config plugin class that contains default Plugins"""
    EXTRACT_PLUGINS = {
        "pandas": ["s3"]
    }
    LOAD_PLUGINS = {
        "pandas" : ["s3"] 
    }

    @classmethod
    def plugin_stage_mapper(cls: "PluginConfig", etl_stage: str) -> dict[str, list[str]]:
        etl_stage = etl_stage.lower()

        if etl_stage == ETLConfig.EXTRACT:
            return cls.EXTRACT_PLUGINS
        if etl_stage == ETLConfig.LOAD:
            return cls.LOAD_PLUGINS
        return None
        
class PluginLoader:
    """ A plugin loader class that initialiazes plugins at run time.

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
            logger.error("Invalid type for 'plugins': expected 'dict', got '%s'", type(plugins).__name__)
            raise TypeError(f"Expected plugins to be a dict, got {type(plugins).__name__}")
            
        allowed_stages = {ETLConfig.EXTRACT, ETLConfig.LOAD}

        if not all(key in allowed_stages for key in plugins.keys()):
            err_message = "Plugins must only contain 'EXTRACT' and 'LOAD' keys."
            logger.error(err_message)
            raise ValueError(err_message)
        
        
    def _initialize_plugin(self, stage: str, plugin_list: list[str]) -> None:
        """Initializes plugin modules for a given ETL stage."""
        logger.info("Initializing plugin list %s for ETL stage %s with engine %s", plugin_list, stage, self.engine)
        
        default_plugins = PluginConfig.plugin_stage_mapper(stage)[self.engine]
        combined_plugins = set(default_plugins + plugin_list)

        if not combined_plugins:
            raise ValueError("No plugins to load for stage {}".format(stage))

        for plugin in combined_plugins:
            try:
                module = importlib.import_module(f"plugins.{stage}.{self.engine}.{plugin}")
                module.initialize()
            except ModuleNotFoundError:
                logger.error("Plugin module not found: plugins.%s.%s.%s", stage, self.engine, plugin)
                raise

        logger.info("All plugins have been initialized.")

                
    def loader(self) -> None:
        """Load the plugins defined in the plugins list."""
        # For each stage in plugins, initialize the plugins for that stage.
        for stage, plugin_list in self.plugins.items():
            self._initialize_plugin(stage.lower(), plugin_list)


class PluginModuleInterface:
    """A plugin has a single function called initialize."""

    @staticmethod
    def initialize() -> None:
        """initialize the plugin module."""
        raise NotImplementedError(
            "The method has not been implemented. You must implement it in your python module."
        )
