# Standard Imports
from __future__ import annotations

from enum import Enum, unique
from typing import Annotated, Self, TYPE_CHECKING, Callable

# Third-party Imports
from pydantic import (
    AfterValidator,
    BeforeValidator,
    BaseModel,
    Field,
    ConfigDict,
    ValidationInfo,
    field_validator,
    model_validator
)

# Project Imports

from plugins.registry import PluginRegistry, PluginWrapper

# A callable type representing all interfaces of the phases.

# A callable class type representing all phase objects.
type PhaseInstance = ExtractPhase | TransformPhase | LoadPhase | TransformLoadPhase

@unique
class PipelinePhase(Enum):
    EXTRACT_PHASE = "extract"
    LOAD_PHASE = "load"
    TRANSFORM_PHASE = "transform"
    TRANSFORM_AT_LOAD_PHASE = "transform_at_load"



def serialize_plugins(phase_pipeline: PipelinePhase) -> Callable[[list | dict], list[PluginWrapper]]:
    def validator(value: list) -> list[PluginWrapper] :
        if type(value) is list:
            return [PluginRegistry.instantiate_plugin(phase_pipeline, plugin_dict) for plugin_dict in value]

        elif type(value) is dict:
            return PluginRegistry.instantiate_plugin(phase_pipeline, value)
        raise TypeError(f"Expected a list or dict, but got {type(value).__name__}")
    return validator


# TODO: Maybe it needs a specifci tests?? in test_unit_pipelines.py
def unique_id_validator(steps):
    done = []
    for step in steps:
        if step.id in done:
            raise ValueError("The `ID` is not unique. There already exists an 'ID' with this name.")
        done.append(step.id)

    return steps


class ExtractPhase(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    steps: Annotated[
        list[PluginWrapper],
        Field(min_length=1),
        BeforeValidator(serialize_plugins(PipelinePhase.EXTRACT_PHASE)),
        AfterValidator(unique_id_validator)
    ]
    pre: Annotated[
        list[PluginWrapper] | None,
        BeforeValidator(serialize_plugins(PipelinePhase.EXTRACT_PHASE))
    ] = None


    merge: Annotated[
        PluginWrapper | None,
        BeforeValidator(serialize_plugins(PipelinePhase.EXTRACT_PHASE))
    ] = None

    @model_validator(mode='after')
    def check_merge_condition(self: Self) -> Self:
        if self.merge and not len(self.steps) > 1:
            raise ValueError("Validation Error! Merge can only be used if there is more than one extract step.")

        return self


class TransformPhase(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    steps: Annotated[
        list[PluginWrapper],
        BeforeValidator(serialize_plugins(PipelinePhase.TRANSFORM_PHASE))
    ]

class LoadPhase(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    steps: Annotated[
        list[PluginWrapper],
        Field(min_length=1),
        BeforeValidator(serialize_plugins(PipelinePhase.LOAD_PHASE))
    ]
    pre: Annotated[
        list[PluginWrapper] | None,
        BeforeValidator(serialize_plugins(PipelinePhase.LOAD_PHASE))
    ] = None

    post: Annotated[
        list[PluginWrapper] | None,
        BeforeValidator(serialize_plugins(PipelinePhase.LOAD_PHASE))
    ] = None


class TransformLoadPhase(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    steps: Annotated[
        list[PluginWrapper],
        Field(min_length=1),
        BeforeValidator(serialize_plugins(PipelinePhase.TRANSFORM_AT_LOAD_PHASE))
    ]

PHASE_CLASS_MAP = {
    PipelinePhase.EXTRACT_PHASE: ExtractPhase,
    PipelinePhase.TRANSFORM_PHASE: TransformPhase,
    PipelinePhase.LOAD_PHASE: LoadPhase,
    PipelinePhase.TRANSFORM_AT_LOAD_PHASE: TransformLoadPhase,
}
