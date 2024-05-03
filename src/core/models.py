"""
This module provides functionalities to create Interfaces for the ETL processes. Any plugins
that want to be integrated with this application need to follow these standards.
"""
# Standard Imports
from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import TypeVar

# Third-party Imports
import pydantic as pyd

# Project imports

T = TypeVar('T')


class PipelineTypes:
    """ A config class that contains constants related to Pipelines and its types."""
    ETL_PIPELINE = "ETL"
    ELT_PIPELINE = "ELT"
    ETLT_PIPELINE = "ETLT"

    ALLOWED_PIPELINE_TYPES: set[str] = {ETL_PIPELINE, ELT_PIPELINE}


class ExtractInterface(ABC):
    """An interface of the Extract Step."""
    id: str
    type: str

    @abstractmethod
    async def extract_data(self) -> T:
        """Collects data from a source."""
        raise NotImplementedError(
            "The method has not been implemented. You must implement it"
        )
    
class TransformInterface(ABC):
    """An interface of the Transform Step."""
    id: str

class ELTTransformInterface(ABC):
    @abstractmethod
    def transform_elt(self, data):
        """Perform transformations after data is loaded into the target system."""
        pass

class ETLTransformInterface(ABC):

    @abstractmethod
    def transform_etl(self, data: T) -> T:
        """Perform transformations before data is loaded into the target system."""
        raise NotImplementedError(
            "The method has not been implemented. You must implement it"
        )

class LoadInterface(ABC):
    """An interface of the Load Step."""
    id: str
    type: str

    @abstractmethod
    async def load_data(self, data: T) -> bool:
        """Load data to a destination."""
        raise NotImplementedError(
            "The method has not been implemented. You must implement it"
        )

class MergerStrategy(ABC):
    """ An Interface of the Load Step."""
    
    @abstractmethod
    def merger(self, extracted_sources: Iterable[T]) -> T:
        """Merges multiple extracted data."""
        raise NotImplementedError(
            "The method has not been implemented. You must implement it"
        )
    
class Pipeline(pyd.BaseModel):
    """A pipeline class that executes ETL steps."""
    model_config = pyd.ConfigDict(arbitrary_types_allowed=True)

    name: str
    type: str

    # ETL Phases
    extract: list[ExtractInterface]
    transform: list[TransformInterface]
    load: list[LoadInterface]

    # manage how data is handled immediately after it has been extracted/transformed
    # and before it moves to the next stage. 
    extractor_data_flow_type: str = "direct"
    transformer_data_flow_type: str = "direct"

    # Private
    __is_executed: bool = False

    # Optional
    description: str | None = None
    needs: str | list[str] | None = None
    extract_merger_strategy: MergerStrategy | None = None #TODO: STILL IDK HOW TO PARSE IT...


    @property
    def is_executed(self) -> bool:
        return self.__is_executed
    
    @is_executed.setter
    def is_executed(self, value: bool) -> None:
        self.__is_executed = value

    @pyd.field_validator("type")
    def pipeline_type_match(cls: "Pipeline", value: str) -> str:
        """A validator that validates pipeline type against the allowed pipeline types."""
        upper_cased_pipeline = value.upper()
        if upper_cased_pipeline not in PipelineTypes.ALLOWED_PIPELINE_TYPES:
            raise ValueError("A pipeline must be of following types: ETL, ELT, ETLT")
        return upper_cased_pipeline
