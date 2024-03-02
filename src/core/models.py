"""
This module provides functionalities to create Interfaces for the ETL processes. Any plugins
that want to be integrated with this application need to follow these standards.
"""

from typing import Protocol

import pydantic as pyd

from common.utils.logger import setup_logger

logger = setup_logger(__name__)


class ExtractInterface(Protocol):
    """An interface of the Extract Step."""

    def extract(self) -> str:
        """Collects data from a source."""
        raise NotImplementedError(
            "The method has not been implemented. You must implement it"
        )


class TransformInterface(Protocol):
    """An interface of the Transform Step."""

    name: str

    def transform(self, data) -> str:
        """Performs transformations on the extracted data"""
        raise NotImplementedError(
            "The method has not been implemented. You must implement it"
        )


class LoadInterface(Protocol):
    """An interface of the Load Step."""

    def load(self, data) -> str:
        """Load data to a destination."""
        raise NotImplementedError(
            "The method has not been implemented. You must implement it"
        )


class Job(pyd.BaseModel):
    """A job class that executes ETL steps."""

    name: str
    extract: list
    transform: list
    load: list
    needs: str | list[str] = None

    is_executed: bool = False

    async def execute(self):
        """Asynchronously executes a given job."""
        # await asyncio.sleep(5)  # Simulating some asynchronous task.
        logger.info("Executing: %s ", self.name)
        logger.info("Completed: %s", self.name)
        self.is_executed = True
