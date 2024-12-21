# Standard Imports
from __future__ import annotations

# Third Party Imports

# Project Imports


class ExtractException(Exception):
    """A custom exception is raised when a problem occurs when trying to extract data from a source"""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message

    def __str__(self) -> str:
        return f"{super().__str__()}"


class TransformException(Exception):
    """A custom exception is raised when a problem occurs when trying to execute transformation on data"""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message

    def __str__(self) -> str:
        return f"{super().__str__()}"


class LoadException(Exception):
    """A custom exception is raised when a problem occurs when trying to load data into a destination"""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message

    def __str__(self) -> str:
        return f"{super().__str__()}"

class TransformLoadException(Exception):
    """A custom exception is raised when a problem occurs when trying to execute transformation on data"""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message

    def __str__(self) -> str:
        return f"{super().__str__()}"
