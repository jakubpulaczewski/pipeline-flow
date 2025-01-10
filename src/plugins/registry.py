# Standard Imports
from __future__ import annotations
import asyncio
import functools
import uuid

from typing import TYPE_CHECKING, Awaitable,  Callable, ParamSpec, TypeVar, Self, Protocol
import logging

# Third Party Imports
from pydantic.dataclasses import dataclass

# Project Imports
import plugins # Imports core plugins defined in __init__.py

from common.utils import SingletonMeta
if TYPE_CHECKING:
    from core.models.phases import  PipelinePhase

logger = logging.getLogger(__name__)

PLUGIN_NAME = str
P = ParamSpec("P")
R = TypeVar("R")

Plugin = Callable[P, R | Awaitable[R]]
from typing import Any
@dataclass
class PluginWrapper:
    id: str
    func: Any #Plugin #TODO: Change it.

    async def _execute_async(self, *args: P.args, **kwargs: P.kwargs) -> Awaitable[R]:
        return await self.func(*args, *kwargs)

    def _execute_sync(self, *args: P.args, **kwargs: P.kwargs) -> R:
        return self.func(*args, **kwargs)

    def execute(self: Self, *args: P.args, **kwargs: P.kwargs) -> R | Awaitable[R]:
        if asyncio.iscoroutinefunction(self.func):
            return self.func(*args, **kwargs)
        return self.func(*args, **kwargs)

    def __eq__(self, other):
        if not isinstance(other, PluginWrapper):
            return False
        return self.id == other.id and self.func.__name__ == other.func.__name__



# TODO: This needs Type fixing.
def plugin(plugin_phase: PipelinePhase, plugin_name: str | None = None) -> Callable[[Plugin], Plugin]:

    def decorator(func: Plugin) -> Plugin:
        nonlocal plugin_name
        plugin_name = func.__name__  if plugin_name is None else plugin_name

        PluginRegistry.register(plugin_phase, plugin_name, func)
        return func
    return decorator


class PluginRegistry(metaclass=SingletonMeta):
    """A Plugin Factory that dynamically registers, removes and fetches plugins.

    Registry example: {EXTRACT_PHASE: {'s3': S3Plugin}}
    """

    _registry: dict[PipelinePhase, dict[PLUGIN_NAME, Plugin]] = {}

    @classmethod
    def register(
        cls,
        pipeline_phase: PipelinePhase,
        plugin: str,
        plugin_callable: Plugin,
    ) -> bool:
        """Regisers a plugin for a given pipeline type and plugin."""
        # Initialise the Pipeline phase in the registry.
        if pipeline_phase not in cls._registry:
            cls._registry[pipeline_phase] = {}

        # Check if the plugin has been registered.
        if plugin in cls._registry[pipeline_phase]:
            logger.warning(
                "Plugin for `%s` phase already exists in PluginRegistry class.",
                pipeline_phase,
            )
            return False

        cls._registry[pipeline_phase][plugin] = plugin_callable
        logger.debug(
            "Plugin `%s` for phase `%s` registered successfully.",
            plugin,
            pipeline_phase,
        )
        return True

    @classmethod
    def remove(cls, pipeline_phase: PipelinePhase, plugin: str) -> bool:
        """Remove a plugin for a given pipeline type and plugin."""
        if pipeline_phase in cls._registry and plugin in cls._registry[pipeline_phase]:
            del cls._registry[pipeline_phase][plugin]
            logger.debug(
                "Plugin '%s' for phase '%s' has been removed.", plugin, pipeline_phase
            )

            # Remove the ETL type dict if empty after removing the plugin.
            if not cls._registry[pipeline_phase]:
                logger.debug("Stage '%s' has been removed.", pipeline_phase)
                del cls._registry[pipeline_phase]
            return True
        return False

    @classmethod
    def get(cls, pipeline_phase: PipelinePhase, plugin: str) -> Plugin:
        """Retrieve a plugin for a given pipeline type and plugin."""
        plugin_factory = cls._registry.get(pipeline_phase, {}).get(plugin, None)

        if not plugin_factory:
            raise ValueError("Plugin class was not found for following plugin `{}`.".format(plugin))
        
        logger.debug("Plugin class '%s' has been successfully retrieved.", plugin_factory)
        return plugin_factory

    
    @classmethod
    def instantiate_plugin(cls, phase_pipeline: PipelinePhase, plugin_data: dict) -> PluginWrapper:
        """Resolve and return a single plugin instance."""
        plugin_name = plugin_data.pop("plugin", None)
        if not plugin_name:
            raise ValueError("The attribute 'plugin' is empty.")

        plugin_factory = cls.get(phase_pipeline, plugin_name)
    
        id = plugin_data.get("id") or f"{plugin_factory.__name__}_{uuid.uuid4()}"
        return PluginWrapper(id=id, func=plugin_factory(**plugin_data))