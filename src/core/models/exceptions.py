# Standard Imports
from __future__ import annotations
from typing import TYPE_CHECKING


# Third Party Imports

# Project Imports
if TYPE_CHECKING:
    from core.models.extract import ExtractResult
    from core.models.transform import TransformResult
    from core.models.load import LoadResult


class ExtractException(Exception):
    """ A custom exception is raised when a problem occurs when trying to extract data from a source"""
    
    def __init__(self, message: str, extract_result: ExtractResult) -> None:
        super().__init__(message)
        self.extract_result = extract_result
    
    def __str__(self) -> str:
        return f"{super().__str__()}\ExtractResult: {self.extract_result}"


class TransformException(Exception):
    """ A custom exception is raised when a problem occurs when trying to execute transformation on data"""
    
    def __init__(self, message: str, transform_result: TransformResult) -> None:
        super().__init__(message)
        self.transform_result = transform_result

    def __str__(self) -> str:
        return f"{super().__str__()}\nTransformResult: {self.transform_result}"
    
class LoadException(Exception):
    """ A custom exception is raised when a problem occurs when trying to load data into a destination"""

    def __init__(self, message: str, load_result: LoadResult) -> None:
        super().__init__(message)
        self.load_result = self.load_result

    
    def __str__(self) -> str:
        return f"{super().__str__()}\LoadResult: {self.load_result}"
