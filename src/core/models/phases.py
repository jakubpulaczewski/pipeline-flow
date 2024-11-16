# Standard Imports
from __future__ import annotations

from abc import ABC
from enum import Enum, unique
from typing import TYPE_CHECKING, Type
# Third-party Imports
import pydantic as pyd



# Project imports
from core.models.extract import IExtractor
from core.models.load import ILoader
from core.models.transform import ILoadTransform, ITransform
from core.storage_phase import StoragePhase


if TYPE_CHECKING:
    from common.type_def import PLUGIN_BASE_CALLABLE, PHASE_TYPE


@unique
class PipelinePhase(Enum):
    EXTRACT_PHASE = "extract"
    LOAD_PHASE = "load"
    TRANSFORM_PHASE = "transform"
    TRANSFORM_AT_LOAD_PHASE = "transform_at_load"

    @classmethod
    def get_phase_class(cls: PipelinePhase, phase_name: PipelinePhase) -> PHASE_TYPE:
        PHASE_CLASS_MAP = {
            cls.EXTRACT_PHASE: ExtractPhase,
            cls.TRANSFORM_PHASE: TransformPhase,
            cls.LOAD_PHASE: LoadPhase,
            cls.TRANSFORM_AT_LOAD_PHASE: TransformLoadPhase,
        }

        return PHASE_CLASS_MAP[phase_name]

    @classmethod
    def get_plugin_interface_for_phase(cls: PipelinePhase, phase_name: PipelinePhase) -> PLUGIN_BASE_CALLABLE:
        PLUGIN_PHASE_INTERFACE_MAP: dict[PipelinePhase, PLUGIN_BASE_CALLABLE] = {
            cls.EXTRACT_PHASE: IExtractor,
            cls.TRANSFORM_PHASE: ITransform,
            cls.LOAD_PHASE: ILoader,
            cls.TRANSFORM_AT_LOAD_PHASE: ILoadTransform,
        }

        return PLUGIN_PHASE_INTERFACE_MAP[phase_name]


class BasePhase[T](pyd.BaseModel, ABC):
    model_config = pyd.ConfigDict(arbitrary_types_allowed=True)
    steps: list[T] | None
    storage: StoragePhase | None = None



class ExtractPhase(BasePhase[IExtractor]):
    pass


class TransformPhase(BasePhase[ITransform]):
    pass


class LoadPhase(BasePhase[ILoader]):
    pass


class TransformLoadPhase(BasePhase[ILoadTransform]):
    pass




    # TODO: TO move...
    # @pyd.field_validator('steps', mode='before')
    # @classmethod
    # def parse_plugins(cls: BasePhase, raw_steps: dict[str, str]) -> list[T]:
    #     parsed_plugins = []
    #     phase = cls.get_phase()

    #     for step in raw_steps:
    #         plugin = step.get('plugin')
    #         parsed_plugin = "" # PluginFactory.get(phase, plugin)
    #         parsed_plugins.append(parsed_plugin(**step))
    #     return parsed_plugins

    # @classmethod
    # def get_phase(cls):
    #     """Returns the phase name corresponding to this class."""
    #     if cls is ExtractPhase:
    #         return PipelinePhase.EXTRACT_PHASE
    #     elif cls is TransformPhase:
    #         return PipelinePhase.TRANSFORM_PHASE
    #     elif cls is LoadPhase:
    #         return PipelinePhase.LOAD_PHASE
    #     elif cls is TransformLoadPhase:
    #         return PipelinePhase.TRANSFORM_AT_LOAD_PHASE
    #     raise ValueError(f"Unknown phase class: {cls}")

    # @classmethod
    # def create(cls, raw_steps: list[dict[str, str]], **kwargs):
    #     """Factory method to create a phase object from raw steps."""
    #     return cls(steps=raw_steps, **kwargs)