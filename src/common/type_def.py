# Standard Imports
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Type, TypeVar

# Third-party imports

# Project Imports
if TYPE_CHECKING:
    from core.models import IExtractor, ILoader, ITransformer

# Data returns types from each ETL phase
ExtractedData = TypeVar("ExtractedData", bound=Any)
TransformedData = TypeVar("TransformedData", bound=Any)
LoadedData = TypeVar("LoadedData", bound=Any)
type Data = ExtractedData | TransformedData | LoadedData


# A callable type representing any ETL phase (Extract, Transform, or Load)
type ETL_PHASE_CALLABLE = Type[IExtractor | ITransformer | ILoader]

# An instance type representing any ETL phase class
type ETL_PHASE_INSTANCE = IExtractor | ITransformer | ILoader
