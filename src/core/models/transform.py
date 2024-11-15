# Standard Imports
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import wraps
from typing import Callable

# Third Party Imports
import pydantic as pyd

# Project Imports
import common.config as config
from common.type_def import ExtractedData, TransformedData
from common.utils.logger import setup_logger
from core.models.exceptions import TransformException
from core.storage_phase import StoragePhase

logger = setup_logger(__name__)


@dataclass
class TransformResult:
    name: str
    type: str
    success: bool
    result: TransformedData | None = None
    error: Exception | None = None


TransformFunction = Callable[[str | ExtractedData], TransformResult]


def transform_decorator(transform_function: TransformFunction) -> TransformFunction:
    @wraps(transform_function)
    def wrapper(self, *args, **kwargs) -> TransformResult:
        try:
            result = transform_function(self, *args, **kwargs)
            if isinstance(self, ITransformerETL):
                return TransformResult(
                    name=self.id,
                    success=True,
                    result=result,
                    type=config.PipelineType.ETL.name,
                )
            elif isinstance(self, ITransformerELT):
                return TransformResult(
                    name=self.id, success=True, type=config.PipelineType.ELT.name
                )

        except Exception as e:
            error_message = f"A problem occurred when trying to execute following transformation {self.id}: {str(e)}"
            logger.error(error_message)
            transform_result = TransformResult(
                name=self.id, success=False, error=str(e), type="UNKNOWN"
            )
            raise TransformException(error_message, transform_result) from e

    return wrapper


class ITransformer(pyd.BaseModel):
    """An interface of the Transform Step."""

    id: str


class ITransformerETL(ITransformer, ABC):
    """An interface of the Transformation in ETL."""

    @abstractmethod
    def transform_data(self, data: ExtractedData) -> TransformResult:
        """Perform transformations before data is loaded into the target system."""
        raise NotImplementedError(
            "The method has not been implemented. You must implement it"
        )


class ITransformerELT(ITransformer, ABC):
    "A separate interface of the Transformation in ELT."
    query: str

    @abstractmethod
    def transform_data(self) -> None:
        """Perform transformations after data is loaded into the target system."""
        raise NotImplementedError(
            "The method has not been implemented. You must implement it"
        )


class TransformPhase(pyd.BaseModel):
    model_config = pyd.ConfigDict(arbitrary_types_allowed=True)

    steps: list[ITransformer] | None = None
    storage: StoragePhase | None = None
