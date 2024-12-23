# Standard Imports
from __future__ import annotations

from abc import ABCMeta, abstractmethod
from enum import Enum, unique
from typing import Annotated, Self, Type, TYPE_CHECKING
from types import FunctionType

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
if TYPE_CHECKING:
    from common.type_def import ExtractedData, TransformedData

from plugins.registry import PluginRegistry 

# A callable type representing all interfaces of the phases.
type PluginCallable = Type[IExtractor | ILoadTransform | ITransform | ILoader]

# A callable class type representing all phase objects.
type PhaseInstance = ExtractPhase | TransformPhase | LoadPhase | TransformLoadPhase

@unique
class PipelinePhase(Enum):
    EXTRACT_PHASE = "extract"
    LOAD_PHASE = "load"
    TRANSFORM_PHASE = "transform"
    TRANSFORM_AT_LOAD_PHASE = "transform_at_load"

class IExtractor(BaseModel, metaclass=ABCMeta):
    """An interface of the Extract Step."""
    id: str
    config: dict | None = None

    @abstractmethod
    async def extract_data(self) -> ExtractedData:
        """Collects data from a source."""
        raise NotImplementedError(
            "The method has not been implemented. You must implement it"
        )
  

class ILoader(BaseModel, metaclass=ABCMeta):
    """An interface of the Load Step."""
    id: str

    @abstractmethod
    async def load_data(self, data: ExtractedData | TransformedData) -> None:
        """Load data to a destination."""
        raise NotImplementedError(
            "The method has not been implemented. You must implement it"
        )


class ITransform(BaseModel, metaclass=ABCMeta):
    """An interface for the Transform phase."""
    id: str

    @abstractmethod
    def transform_data(self, data: ExtractedData) -> TransformedData:
        """Perform transformations before data is loaded."""
        raise NotImplementedError(
            "The method has not been implemented. You must implement it"
        )


class ILoadTransform(BaseModel, metaclass=ABCMeta):
    """An interface for the Post-Load Transform phase."""
    id: str

    @abstractmethod
    def transform_data(self) -> None:
        """Perform transformations after data is loaded."""
        raise NotImplementedError(
            "The method has not been implemented. You must implement it"
        )


class iMerger(BaseModel, metaclass=ABCMeta):
    
    @abstractmethod
    def merge_data(self, extracted_data: dict[str, ExtractedData]) -> ExtractedData:
        raise NotImplementedError("The method has not been implemented. You must implement it")


def resolve_plugin(phase_pipeline: PipelinePhase, plugin_data: dict) -> object:
    """Resolve and return a single plugin instance."""
    plugin_name = plugin_data.pop("plugin", None)
    if not plugin_name:
        raise ValueError("The attribute 'plugin' is empty.")
    
    plugin = PluginRegistry.get(phase_pipeline, plugin_name)(**plugin_data)
    return plugin


def serialize_plugins(phase_pipeline: PipelinePhase) -> object:
    def validator(value):
        if type(value) is list:
            return [resolve_plugin(phase_pipeline, plugin_dict) for plugin_dict in value]
        elif type(value) is dict:
            return resolve_plugin(phase_pipeline, value)
        raise TypeError(f"Expected a list or dict, but got {type(value).__name__}")
    return validator

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
        list[IExtractor], 
        Field(min_length=1), 
        BeforeValidator(serialize_plugins(PipelinePhase.EXTRACT_PHASE)),
        AfterValidator(unique_id_validator)
    ]
    pre: Annotated[
        list[FunctionType] | None, 
        BeforeValidator(serialize_plugins(PipelinePhase.EXTRACT_PHASE))
    ] = None


    merge: Annotated[
        iMerger | None, 
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
        list[ITransform | FunctionType], 
        BeforeValidator(serialize_plugins(PipelinePhase.TRANSFORM_PHASE))
    ]
  
class LoadPhase(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    steps: Annotated[
        list[ILoader], 
        Field(min_length=1), 
        BeforeValidator(serialize_plugins(PipelinePhase.LOAD_PHASE))
    ]
    pre: Annotated[
        list[FunctionType] | None, 
        BeforeValidator(serialize_plugins(PipelinePhase.LOAD_PHASE))
    ] = None

    post: Annotated[
        list[FunctionType] | None, 
        BeforeValidator(serialize_plugins(PipelinePhase.LOAD_PHASE))
    ] = None


class TransformLoadPhase(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    steps: Annotated[
        list[ILoadTransform | FunctionType], 
        Field(min_length=1), 
        BeforeValidator(serialize_plugins(PipelinePhase.TRANSFORM_AT_LOAD_PHASE))
    ]

PHASE_CLASS_MAP = {
    PipelinePhase.EXTRACT_PHASE: ExtractPhase,
    PipelinePhase.TRANSFORM_PHASE: TransformPhase,
    PipelinePhase.LOAD_PHASE: LoadPhase,
    PipelinePhase.TRANSFORM_AT_LOAD_PHASE: TransformLoadPhase,
}


PLUGIN_PHASE_INTERFACE_MAP: dict[PipelinePhase, PluginCallable] = {
    PipelinePhase.EXTRACT_PHASE: IExtractor,
    PipelinePhase.TRANSFORM_PHASE: ITransform,
    PipelinePhase.LOAD_PHASE: ILoader,
    PipelinePhase.TRANSFORM_AT_LOAD_PHASE: ILoadTransform,
}