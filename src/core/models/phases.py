# Standard Imports
from __future__ import annotations

import logging
from enum import Enum, unique
from typing import TYPE_CHECKING, Annotated, Self

if TYPE_CHECKING:
    from collections.abc import Callable

# Third-party Imports
from pydantic import (
    AfterValidator,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    model_validator,
)

# Project Imports
from core.plugins import PluginRegistry, PluginWrapper

# A callable class type representing all phase objects.
type Phase = ExtractPhase | TransformPhase | LoadPhase | TransformLoadPhase


@unique
class PipelinePhase(Enum):
    EXTRACT_PHASE = "extract"
    LOAD_PHASE = "load"
    TRANSFORM_PHASE = "transform"
    TRANSFORM_AT_LOAD_PHASE = "transform_at_load"


def serialize_plugin(phase_pipeline: PipelinePhase) -> Callable[[dict], PluginWrapper]:
    def validator(value: dict) -> PluginWrapper:
        return PluginRegistry.instantiate_plugin(phase_pipeline, value)

    return validator


def serialize_plugins(phase_pipeline: PipelinePhase) -> Callable[[list], list[PluginWrapper]]:
    def validator(value: list) -> list[PluginWrapper]:
        return [PluginRegistry.instantiate_plugin(phase_pipeline, plugin_dict) for plugin_dict in value]

    return validator


def unique_id_validator(steps: list[PluginWrapper]) -> list[PluginWrapper]:
    if not steps:
        logging.debug("No plugins to validate for unique ID.")
        return steps

    ids = {}

    for step in steps:
        if step.id in ids:
            raise ValueError("The `ID` is not unique. There already exists an 'ID' with this name.")

        ids[step.id] = 1

    return steps


class ExtractPhase(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    steps: Annotated[
        list[PluginWrapper],
        Field(min_length=1),
        BeforeValidator(serialize_plugins(PipelinePhase.EXTRACT_PHASE)),
        AfterValidator(unique_id_validator),
    ]
    pre: Annotated[
        list[PluginWrapper] | None,
        BeforeValidator(serialize_plugins(PipelinePhase.EXTRACT_PHASE)),
    ] = None

    merge: Annotated[
        PluginWrapper | None,
        BeforeValidator(serialize_plugin(PipelinePhase.EXTRACT_PHASE)),
    ] = None

    @model_validator(mode="after")
    def check_merge_condition(self: Self) -> Self:
        if self.merge and not len(self.steps) > 1:
            raise ValueError("Validation Error! Merge can only be used if there is more than one extract step.")

        if len(self.steps) > 1 and not self.merge:
            raise ValueError("Validation Error! Merge is required when there are more than one extract step.")

        return self


class TransformPhase(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    steps: Annotated[
        list[PluginWrapper],
        BeforeValidator(serialize_plugins(PipelinePhase.TRANSFORM_PHASE)),
    ]


class LoadPhase(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    steps: Annotated[
        list[PluginWrapper],
        Field(min_length=1),
        BeforeValidator(serialize_plugins(PipelinePhase.LOAD_PHASE)),
    ]
    pre: Annotated[
        list[PluginWrapper] | None,
        BeforeValidator(serialize_plugins(PipelinePhase.LOAD_PHASE)),
    ] = None

    post: Annotated[
        list[PluginWrapper] | None,
        BeforeValidator(serialize_plugins(PipelinePhase.LOAD_PHASE)),
    ] = None


class TransformLoadPhase(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    steps: Annotated[
        list[PluginWrapper],
        Field(min_length=1),
        BeforeValidator(serialize_plugins(PipelinePhase.TRANSFORM_AT_LOAD_PHASE)),
    ]
