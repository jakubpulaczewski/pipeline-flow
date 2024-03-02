""" A Transition step to move data from one stage to next one i.e.
    from Extract To Transform, and Transform to Load. """

from typing import Protocol


class TransitionInterface(Protocol):
    """An interface for defining the transition process in an ETL workflow."""

    def store(self):
        """
        Store the data in the designated storage system.
        This method should be implemented by any class that follows this protocol.
        """
        raise NotImplementedError("This method must be implemented by a subclass.")


class DataStorage:
    """A class that represents storing data in a persistent storage system."""

    def store(self):
        """
        Store the data in a persistent storage.
        This method should be overridden by subclasses to provide specific storage mechanisms.
        """
        raise NotImplementedError("This method must be implemented by a subclass.")


class InMemory:
    """A class for storing data in memory."""

    def store(self):
        """
        Store the data in memory.
        Subclasses should implement this method to define how data is stored in memory.
        """
        raise NotImplementedError("This method must be implemented by a subclass.")


class Queue:
    """A class for managing data storage in a queue structure."""

    def store(self):
        """
        Store the data in a queue.
        This method should be implemented by subclasses to define how data is enqueued.
        """
        raise NotImplementedError("This method must be implemented by a subclass.")
