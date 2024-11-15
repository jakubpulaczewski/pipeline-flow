import asyncio
from abc import ABC, abstractmethod
from typing import TypeVar


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
    
class QueueDataFlow(DataFlow):
    """A class for managing data storage in a queue structure."""

    def __init__(self) -> None:
        self.queue = asyncio.Queue()

    async def producer(self, data):
        for d in data:
            await self.queue.put(d)


    async def transfer_data(self, data: list[T]) ->  asyncio.Queue[T]:
        """
        Store the data in a queue.
        This method should be implemented by subclasses to define how data is enqueued.
        """
        for d in data:
            await self.producer(d)
        
        return self.queue
    
class DataManager:


    async def execute_flow(data, **kwargs):
        queue = QueueDataFlow(**kwargs)

        val = await queue.transfer_data(data)
        print(val)
        return val
    

data = ["A", "B", "C"]


asyncio.run(DataManager.execute_flow(data))