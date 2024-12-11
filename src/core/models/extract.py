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

logger = setup_logger(__name__)


@dataclass
class ExtractResult:
    name: str
    success: bool
    result: ExtractedData | None = None
    error: Exception | None = None

          

ExtractFunction = Callable[[], ExtractResult]


def extract_decorator(id: str, extract_function: ExtractFunction) -> ExtractFunction:
    @wraps(extract_function)
    async def wrapper(*args, **kwargs) -> ExtractResult:
        try:
            result = await extract_function(*args, **kwargs)
            return ExtractResult(name=id, success=True, result=result)

        except Exception as e:
            error_message = f"Error during extraction: {e}"
            logger.error(error_message)
            extract_result = ExtractResult(name=id, success=False, error=str(e))
            raise ExtractException(error_message, extract_result) from e

    return wrapper


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
