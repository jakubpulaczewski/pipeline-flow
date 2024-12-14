# Standard Imports
from __future__ import annotations

from typing import  Any, Type, TypeVar

# Third-party imports

# Project Imports

# Data returns types from each ETL phase
ExtractedData = TypeVar("ExtractedData", bound=Any)
TransformedData = TypeVar("TransformedData", bound=Any)
LoadedData = TypeVar("LoadedData", bound=Any)
type Data = ExtractedData | TransformedData | LoadedData


