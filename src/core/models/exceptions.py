# Standard Imports
from __future__ import annotations

from typing import Self

# Third Party Imports

# Project Imports


class PipelineBaseError(Exception):
    """A base exception class for any pipeline errors."""

    def __init__(self: Self, message: str, original_exception: Exception | None = None) -> None:
        self.message = message
        self.original_exception = original_exception
        super().__init__(self.message)

    def __str__(self: Self) -> str:
        if self.original_exception:
            return f"{self.message} | Caused by: {self.original_exception}"
        return self.message


class ExtractError(PipelineBaseError): ...


class TransformError(PipelineBaseError): ...


class LoadError(PipelineBaseError): ...


class TransformLoadError(PipelineBaseError): ...
