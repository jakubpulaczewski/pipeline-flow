# Standard Imports
from __future__ import annotations

import logging

from functools import wraps
from typing import Callable

# Third Party Imports
from common.type_def import ExtractedData, TransformedData, TransformLoadedData

from core.models.exceptions import (
    ExtractException,
    LoadException,
    TransformException
)

# Project Imports

logger = logging.getLogger(__name__)


ExtractFunction = Callable[[], ExtractedData]
TransformFunction = Callable[[str | ExtractedData], TransformedData]
LoadFunction = Callable[[ExtractedData | TransformedData], None]
TransformLoadFunction = Callable[[], None]



def extract_decorator(id: str, extract_function: ExtractFunction) -> ExtractFunction:
    @wraps(extract_function)
    async def wrapper(*args, **kwargs) -> ExtractedData:
        try:
            result = await extract_function(*args, **kwargs)
            return result

        except Exception as e:
            error_message = f"Error during `extraction` (ID: {id}): {e}"
            logger.error(error_message)
            raise ExtractException(error_message) from e

    return wrapper

def load_decorator(id: str, load_function: LoadFunction) -> LoadFunction:
    @wraps(load_function)
    async def wrapper(*args, **kwargs) -> None:
        try:
            await load_function(*args, **kwargs)
            return {'id': id, 'success': True}
        except Exception as e:
            error_message = f"Error during `load` (ID: {id}): {e}"
            logger.error(error_message)
            raise LoadException(error_message) from e

    return wrapper


def transform_decorator(id: str, transform_function: TransformFunction) -> TransformFunction:
    @wraps(transform_function)
    def wrapper(*args, **kwargs) -> TransformedData:
        try:
            result = transform_function(*args, **kwargs)
            return result
        except Exception as e:
            error_message = f"Error during `transform` (ID: {id}): {e}"
            logger.error(error_message)
            raise TransformException(error_message) from e

    return wrapper



def transform_load_decorator(id: str, transform_function: TransformFunction) -> TransformFunction:
    @wraps(transform_function)
    def wrapper(*args, **kwargs) -> TransformLoadedData:
        try:
            transform_function(*args, **kwargs)
            return {'id': id, 'success': True}
        except Exception as e:
            error_message = f"Error during `transform_at_load` (ID: {id}): {e}"
            logger.error(error_message)
            raise TransformException(error_message) from e

    return wrapper