# Standard Imports
from __future__ import annotations

from typing import TYPE_CHECKING

# Third Party Imports

# Project Imports
if TYPE_CHECKING:
    from core.models.phase_wrappers import (
        TransformResult,
        LoadResult
    )



class ExtractException(Exception):
    """A custom exception is raised when a problem occurs when trying to extract data from a source"""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message

    def __str__(self) -> str:
        return f"{super().__str__()}"


class TransformException(Exception):
    """A custom exception is raised when a problem occurs when trying to execute transformation on data"""

    def __init__(self, message: str, transform_result: TransformResult) -> None:
        super().__init__(message)
        self.transform_result = transform_result

    def __str__(self) -> str:
        return f"{super().__str__()}\nTransformResult: {self.transform_result}"


class LoadException(Exception):
    """A custom exception is raised when a problem occurs when trying to load data into a destination"""

    def __init__(self, message: str, load_result: LoadResult) -> None:
        super().__init__(message)
        self.load_result = load_result

    def __str__(self) -> str:
        return f"{super().__str__()}\LoadResult: {self.load_result}"
