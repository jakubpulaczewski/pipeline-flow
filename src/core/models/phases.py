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
    from common.type_def import PLUGIN_BASE_CALLABLE

type PHASE_TYPE = ExtractPhase | TransformPhase | LoadPhase | TransformLoadPhase

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

        phase_class = PHASE_CLASS_MAP.get(phase_name)

        if not phase_class:
            return ValueError("Unknown phase: %s", phase_name)
        return phase_class

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