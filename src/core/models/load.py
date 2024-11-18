# Standard Imports
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import wraps
from typing import Callable

# Third Party Imports
import pydantic as pyd

from common.type_def import ExtractedData, TransformedData
from common.utils.logger import setup_logger
from core.models.exceptions import LoadException

# Project Imports
from core.storage_phase import StoragePhase

logger = setup_logger(__name__)


@dataclass
class LoadResult:
    name: str
    success: bool
    error: Exception | None = None


LoadFunction = Callable[[ExtractedData | TransformedData], LoadResult]


def load_decorator(load_function: LoadFunction) -> LoadFunction:
    @wraps(load_function)
    async def wrapper(self, *args, **kwargs) -> LoadResult:
        try:
            load_function(self, *args, **kwargs)
            return LoadResult(name=self.id, success=True)

        except Exception as e:
            error_message = f"A problem occurred when trying to load data to the following destination {self.id}: {str(e)}"
            logger.error(error_message)
            load_result = LoadResult(name=self.id, success=False, error=str(e))
            raise LoadException(error_message, load_result) from e

    return wrapper


class ILoader(pyd.BaseModel, ABC):
    """An interface of the Load Step."""

    id: str

    @abstractmethod
    @load_decorator
    async def load_data(self, data: ExtractedData | TransformedData) -> None:
        """Load data to a destination."""
        raise NotImplementedError(
            "The method has not been implemented. You must implement it"
        )
