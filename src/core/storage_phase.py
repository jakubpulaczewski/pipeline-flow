# Standard Imports
from __future__ import annotations

from abc import ABCMeta, abstractmethod

# Third Party Imports
import pydantic as pyd

# Project Imports
from common.type_def import Data


class StoragePhase(pyd.BaseModel, ABCMeta):
    storage_type: str
    destination: str

    @abstractmethod
    def save(self, data: Data) -> bool:
        raise NotImplementedError("This method needs to be implemented by subclass.")
