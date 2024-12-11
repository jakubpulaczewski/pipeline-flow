# Standard Imports
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass # TODO: Potentially might need to be changed to pydantic's data class.
from functools import wraps
from typing import Callable

# Third Party Imports
import pydantic as pyd

# Project Imports
from common.type_def import ExtractedData, TransformedData
from common.utils.logger import setup_logger
from core.models.exceptions import TransformException

logger = setup_logger(__name__)


@dataclass
class TransformResult:
    name: str
    success: bool
    result: TransformedData | None = None
    error: Exception | None = None


TransformFunction = Callable[[str | ExtractedData], TransformResult]


def transform_decorator(id: str, transform_function: TransformFunction) -> TransformFunction:
    @wraps(transform_function)
    def wrapper(*args, **kwargs) -> TransformResult:
        try:
            result = transform_function(*args, **kwargs)
            return TransformResult(
                name=id, success=True, result=result or None
            )

        except Exception as e:
            error_message = f"A problem occurred when trying to execute following transformation {transform_function}: {str(e)}"
            logger.error(error_message)
            transform_result = TransformResult(name=id, success=False, error=str(e))
            raise TransformException(error_message, transform_result) from e

    return wrapper


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
