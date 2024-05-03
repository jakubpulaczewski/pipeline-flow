""" A Transition step to move data from one stage to next one i.e.
    from Extract To Transform, and Transform to Load. """

# Standard Imports
import asyncio
from abc import ABC, abstractmethod
from typing import TypeVar

# Third Party Imports

# Project Imports


T = TypeVar("T")


class DataFlow(ABC):
    """An interface for defining the transition process in an ETL workflow."""

    @abstractmethod
    def transfer_data(self, data: T) -> T:
        """
        Store the data in the designated storage system.
        This method should be implemented by any class that follows this protocol.
        """
        raise NotImplementedError("This method must be implemented by a subclass.")


class PersistentDataFlow(DataFlow):
    """A class that represents storing data in a persistent storage system."""

    def transfer_data(self, data: T) -> T:
        """
        Store the data in a persistent storage.
        This method should be overridden by subclasses to provide specific storage mechanisms.
        """
        raise NotImplementedError("This method must be implemented by a subclass.")


class DirectDataFlow(DataFlow):
    """A a simple class that stores data directly in memory."""

    def transfer_data(self, data: T) -> T:
        """
        Store the data in memory.
        Subclasses should implement this method to define how data is stored in memory.
        """
        return data


class QueueDataFlow(DataFlow):
    """A class for managing data storage in a queue structure."""

    def __init__(self) -> None:
        self.queue = asyncio.Queue()

    def transfer_data(self, data: T) -> asyncio.Queue[T]:
        """
        Store the data in a queue.
        This method should be implemented by subclasses to define how data is enqueued.
        """
        
        return self.queue


class DataFlowManager:

    @staticmethod
    def _get_data_flow(data_flow_strategy: str) -> DataFlow:
        """A factory function for creating data flow object.

        Args:
            data_flow_strategy (str): A data flow strategy for moving data from one stage to another.

        Raises:
            ValueError: If Invalid data flow type is provided.

        Returns:
            DataFlow: an object.
        """
        if data_flow_strategy == "queue":
            return QueueDataFlow
        elif data_flow_strategy == "direct":
            return DirectDataFlow
        elif data_flow_strategy == "persistent":
            return PersistentDataFlow
        else:
            raise ValueError("Invalid data flow type.")

    def execute_flow(self, data: T, data_flow_strategy: str, **kwargs) -> T:
        """Executes the data flow from one stage to another i.e. E -> T

        Example Usage:
            data_flow_strategy = DataFlowManager()
            data_flow_strategy.execute_flow("extracted data", "queue")

        Args:
            data (T): A generic data type of the actual data
            data_flow_strategy (str): Specifies the data flow strategy i.e "queue"

        Returns:
            T: Transfers the actual data from one stage to another
        """
        data_flow: DataFlow = self._get_data_flow(data_flow_strategy)(**kwargs)
        
        return data_flow.transfer_data(data)
