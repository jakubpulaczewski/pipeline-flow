# Standard Imports
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import wraps
from typing import Callable

# Third Party Imports
import pydantic as pyd

from common.type_def import ExtractedData
from common.utils.logger import setup_logger
from core.models.exceptions import ExtractException

# Project Imports
from core.storage_phase import StoragePhase

logger = setup_logger(__name__)


@dataclass
class ExtractResult:
    name: str
    success: bool
    result: ExtractedData | None = None
    error: Exception | None = None


ExtractFunction = Callable[[], ExtractResult]


def extract_decorator(extract_function: ExtractFunction) -> ExtractFunction:
    @wraps(extract_function)
    async def wrapper(self, *args, **kwargs) -> ExtractResult:
        try:
            result = await extract_function(self, *args, **kwargs)
            return ExtractResult(name=self.id, success=True, result=result)

        except Exception as e:
            error_message = f"A problem occurred when trying to extract data from {self.id}: {str(e)}"
            logger.error(error_message)
            extract_result = ExtractResult(name=self.id, success=False, error=str(e))
            raise ExtractException(error_message, extract_result) from e

    return wrapper


class IExtractor(pyd.BaseModel, ABC):
    """An interface of the Extract Step."""

    id: str
    type: str
    config: dict | None = None

    @abstractmethod
    async def extract_data(self) -> ExtractedData:
        """Collects data from a source."""
        raise NotImplementedError(
            "The method has not been implemented. You must implement it"
        )


class ExtractPhase(pyd.BaseModel):
    model_config = pyd.ConfigDict(arbitrary_types_allowed=True)

    steps: list[IExtractor]
    storage: StoragePhase | None = None
