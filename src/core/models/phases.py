# Standard Imports
from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum, unique
from typing import Callable, Type, TYPE_CHECKING

# Third-party Imports
import pydantic as pyd

# Project Imports
if TYPE_CHECKING:
    from common.type_def import ExtractedData, TransformedData

from core.storage_phase import StoragePhase

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

class IExtractor(pyd.BaseModel, ABC):
    """An interface of the Extract Step."""
    id: str
    config: dict | None = None

    @abstractmethod
    async def extract_data(self) -> ExtractedData:
        """Collects data from a source."""
        raise NotImplementedError(
            "The method has not been implemented. You must implement it"
        )
  
class iMerger(ABC):
    
    @abstractmethod
    def merge(self, extracted_data: dict[str, ExtractedData]):
        raise NotImplementedError(
            "The method has not been implemented. You must implement it"
        )

class ILoader(pyd.BaseModel, ABC):
    """An interface of the Load Step."""
    id: str

    @abstractmethod
    async def load_data(self, data: ExtractedData | TransformedData) -> None:
        """Load data to a destination."""
        raise NotImplementedError(
            "The method has not been implemented. You must implement it"
        )


class ITransform(pyd.BaseModel, ABC):
    """An interface for the Transform phase."""
    id: str

    @abstractmethod
    def transform_data(self, data: ExtractedData) -> TransformedData:
        """Perform transformations before data is loaded."""
        raise NotImplementedError(
            "The method has not been implemented. You must implement it"
        )


class ILoadTransform(pyd.BaseModel, ABC):
    """An interface for the Post-Load Transform phase."""
    id: str

    @abstractmethod
    def transform_data(self) -> None:
        """Perform transformations after data is loaded."""
        raise NotImplementedError(
            "The method has not been implemented. You must implement it"
        )


class BasePhase[T](pyd.BaseModel, ABC):
    model_config = pyd.ConfigDict(arbitrary_types_allowed=True)
    steps: list[T] | None = []
    storage: StoragePhase | None = None

class ExtractPhase(BasePhase[IExtractor]):
    pre: list[Callable] | None = []
    post: list[Callable] | None = []
    merge: iMerger | None = None

class TransformPhase(BasePhase[ITransform]):
    pass

class LoadPhase(BasePhase[ILoader]):
    pre: list[Callable] = []
    post: list[Callable] = []

class TransformLoadPhase(BasePhase[ILoadTransform]):
    pass


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