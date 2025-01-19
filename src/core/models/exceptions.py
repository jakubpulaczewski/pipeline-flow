# Standard Imports
from __future__ import annotations

from typing import Self

# Third Party Imports

# Project Imports


class ExtractError(Exception):
    """A custom exception is raised when a problem occurs when trying to extract data from a source."""

    def __init__(self: Self, message: str) -> None:
        super().__init__(message)
        self.message = message

    def __str__(self: Self) -> str:
        return f"{super().__str__()}"


class TransformError(Exception):
    """A custom exception is raised when a problem occurs when trying to execute transformation on data."""

    def __init__(self: Self, message: str) -> None:
        super().__init__(message)
        self.message = message

    def __str__(self: Self) -> str:
        return f"{super().__str__()}"


class LoadError(Exception):
    """A custom exception is raised when a problem occurs when trying to load data into a destination."""

    def __init__(self: Self, message: str) -> None:
        super().__init__(message)
        self.message = message

    def __str__(self: Self) -> str:
        return f"{super().__str__()}"


class TransformLoadError(Exception):
    """A custom exception is raised when a problem occurs when trying to execute transformation on data."""

    def __init__(self: Self, message: str) -> None:
        super().__init__(message)
        self.message = message

    def __str__(self: Self) -> str:
        return f"{super().__str__()}"
