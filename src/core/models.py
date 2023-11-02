import asyncio
import os
import pydantic as pyd

from typing import Protocol


class ExtractInterface(Protocol):
 
    def extract(self) -> str:
        """ Collects data from a source.  """
        ...
    
    def store(self) -> str:
        """ Stores the incoming data.  """
        ...

class TransformInterface(Protocol):
    name: str

    def transform(self, data) -> str:
        """ Performs transformations on the extracted data """
        ...

class LoadInterface(Protocol):

    def load(self, data) -> str:
        """ Load data to a destination."""
        ...

class Job(pyd.BaseModel):
    name: str
    extract: list
    transform: list
    load: list
    needs: str = None

    _is_executed: bool  = False
   
    async def execute(self):
        print(f"Executing {self.name}")
        await asyncio.sleep(1)  # Simulating some asynchronous task.
        print(f"Completed {self.name}")
        self._is_executed = True
