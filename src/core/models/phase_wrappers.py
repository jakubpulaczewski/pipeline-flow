# Standard Imports
from __future__ import annotations

from dataclasses import dataclass
from functools import wraps
from typing import Callable

# Third Party Imports
from common.type_def import ExtractedData, TransformedData
    
from common.logger import setup_logger
from core.models.exceptions import (
    ExtractException,
    LoadException,
    TransformException
)

# Project Imports

logger = setup_logger(__name__)


@dataclass
class ExtractResult:
    name: str
    success: bool
    result: ExtractedData | None = None
    error: Exception | None = None

@dataclass
class LoadResult:
    name: str
    success: bool
    error: Exception | None = None

@dataclass
class TransformResult:
    name: str
    success: bool
    result: TransformedData | None = None
    error: Exception | None = None


ExtractFunction = Callable[[], ExtractResult]
TransformFunction = Callable[[str | ExtractedData], TransformResult]
LoadFunction = Callable[[ExtractedData | TransformedData], LoadResult]


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

def load_decorator(id: str, load_function: LoadFunction) -> LoadFunction:
    @wraps(load_function)
    async def wrapper(*args, **kwargs) -> LoadResult:
        try:
            await load_function(*args, **kwargs)
            return LoadResult(name=id, success=True)

        except Exception as e:
            error_message = f"A problem occurred when trying to load data to the following destination {self.id}: {str(e)}"
            logger.error(error_message)
            load_result = LoadResult(name=id, success=False, error=str(e))
            raise LoadException(error_message, load_result) from e

    return wrapper


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