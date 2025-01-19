# Standard Imports
from __future__ import annotations

import logging
import uuid
from functools import wraps
from typing import TYPE_CHECKING, Any, Awaitable, Callable, ClassVar, Generic, Self

# Third Party Imports
from pydantic.dataclasses import dataclass

from common.type_def import PluginOutput

# Project Imports
from common.utils import SingletonMeta

if TYPE_CHECKING:
    from common.type_def import Plugin, PluginName
    from core.models.phases import PipelinePhase

logger = logging.getLogger(__name__)


def plugin(plugin_phase: PipelinePhase, plugin_name: str | None = None) -> Callable[[Plugin], Plugin]:
    @wraps(plugin)
    def decorator(func: Plugin) -> Plugin:
        nonlocal plugin_name
        plugin_name = func.__name__ if plugin_name is None else plugin_name

        PluginRegistry.register(plugin_phase, plugin_name, func)
        return func

    return decorator


@dataclass
class PluginWrapper(Generic[PluginOutput]):
    id: str
    func: Callable[..., PluginOutput] | Callable[..., Awaitable[PluginOutput]]

    def __eq__(self: Self, other: object) -> bool:
        if not isinstance(other, PluginWrapper):
            return False
        return self.id == other.id and self.func.__name__ == other.func.__name__


class PluginRegistry(metaclass=SingletonMeta):
    """A Plugin Factory that dynamically registers, removes and fetches plugins.

    Registry example: {EXTRACT_PHASE: {'s3': S3Plugin}}
    """

    _registry: ClassVar[dict[PipelinePhase, dict[PluginName, Plugin]]] = {}

    @classmethod
    def register(
        cls,
        pipeline_phase: PipelinePhase,
        plugin_name: PluginName,
        plugin_callable: Plugin,
    ) -> bool:
        """Regisers a plugin for a given pipeline type and plugin."""
        # Initialise the Pipeline phase in the registry.
        if pipeline_phase not in cls._registry:
            cls._registry[pipeline_phase] = {}

        # Check if the plugin has been registered.
        if plugin_name in cls._registry[pipeline_phase]:
            logger.warning(
                "Plugin for `%s` phase already exists in PluginRegistry class.",
                pipeline_phase,
            )
            return False

        cls._registry[pipeline_phase][plugin_name] = plugin_callable
        logger.debug(
            "Plugin `%s` for phase `%s` registered successfully.",
            plugin,
            pipeline_phase,
        )
        return True

    @classmethod
    def remove(cls, pipeline_phase: PipelinePhase, plugin_name: PluginName) -> bool:
        """Remove a plugin for a given pipeline type and plugin."""
        if pipeline_phase in cls._registry and plugin_name in cls._registry[pipeline_phase]:
            del cls._registry[pipeline_phase][plugin_name]
            logger.debug("Plugin '%s' for phase '%s' has been removed.", plugin_name, pipeline_phase)

            # Remove the ETL type dict if empty after removing the plugin.
            if not cls._registry[pipeline_phase]:
                logger.debug("Stage '%s' has been removed.", pipeline_phase)
                del cls._registry[pipeline_phase]
            return True
        return False

    @classmethod
    def get(cls, pipeline_phase: PipelinePhase, plugin_name: PluginName) -> Plugin:
        """Retrieve a plugin for a given pipeline type and plugin."""
        plugin_factory = cls._registry.get(pipeline_phase, {}).get(plugin_name, None)

        if not plugin_factory:
            msg = f"Plugin class was not found for following plugin `{plugin_name}`."
            raise ValueError(msg)

        logger.debug("Plugin class '%s' has been successfully retrieved.", plugin_factory)
        return plugin_factory

    @classmethod
    def instantiate_plugin(cls, phase_pipeline: PipelinePhase, plugin_data: dict[str, Any]) -> PluginWrapper:
        """Resolve and return a single plugin instance."""
        plugin_name = plugin_data.pop("plugin", None)
        if not plugin_name:
            msg = "The attribute 'plugin' is empty."
            raise ValueError(msg)

        plugin_factory = cls.get(phase_pipeline, plugin_name)

        plugin_id = plugin_data.get("id") or f"{plugin_factory.__name__}_{uuid.uuid4()}"
        return PluginWrapper(id=plugin_id, func=plugin_factory(**plugin_data))
