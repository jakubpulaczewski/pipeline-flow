# Standard Imports
from __future__ import annotations

from enum import StrEnum, unique
from typing import Annotated, Self

# Project Imports
from pydantic import (
    AfterValidator,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    model_validator,
)

from pipeline_flow.common.utils import serialize_plugin, serialize_plugins, unique_id_validator
from pipeline_flow.plugins import (  # noqa: TC001 - False Positive. These are required to serialiaze the plugins.
    IExtractPlugin,
    ILoadPlugin,
    IMergeExtractPlugin,
    IPlugin,
    ITransformLoadPlugin,
    ITransformPlugin,
)

# Type Imports

# A callable class type representing all phase objects.
type Phase = ExtractPhase | TransformPhase | LoadPhase | TransformLoadPhase


@unique
class PipelinePhase(StrEnum):
    EXTRACT_PHASE = "extract"
    LOAD_PHASE = "load"
    TRANSFORM_PHASE = "transform"
    TRANSFORM_AT_LOAD_PHASE = "transform_at_load"


class ExtractPhase(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    steps: Annotated[
        list[IExtractPlugin],
        Field(min_length=1),
        BeforeValidator(serialize_plugins),
        AfterValidator(unique_id_validator),
    ]
    pre: Annotated[
        list[IPlugin] | None,
        BeforeValidator(serialize_plugins),
    ] = None

    merge: Annotated[
        IMergeExtractPlugin | None,
        BeforeValidator(serialize_plugin),
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
        list[ITransformPlugin],
        BeforeValidator(serialize_plugins),
    ]


class LoadPhase(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    steps: Annotated[
        list[ILoadPlugin],
        Field(min_length=1),
        BeforeValidator(serialize_plugins),
    ]
    pre: Annotated[
        list[IPlugin] | None,
        BeforeValidator(serialize_plugins),
    ] = None

    post: Annotated[
        list[IPlugin] | None,
        BeforeValidator(serialize_plugins),
    ] = None


class TransformLoadPhase(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    steps: Annotated[
        list[ITransformLoadPlugin],
        Field(min_length=1),
        BeforeValidator(serialize_plugins),
    ]
