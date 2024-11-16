# Standard Imports
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Type, TypeVar

# Third-party imports

# Project Imports
if TYPE_CHECKING:
    from core.models.extract import IExtractor
    from core.models.transform import ILoadTransform, ITransform
    from core.models.load import ILoader



# Data returns types from each ETL phase
ExtractedData = TypeVar("ExtractedData", bound=Any)
TransformedData = TypeVar("TransformedData", bound=Any)
LoadedData = TypeVar("LoadedData", bound=Any)
type Data = ExtractedData | TransformedData | LoadedData


# A callable type representing any ETL phase (Extract, Transform, or Load)
type PLUGIN_BASE_CALLABLE = Type[IExtractor | ILoadTransform | ITransform | ILoader]

# An instance type representing any ETL phase class
type PHASE_STEP_INSTANCE = IExtractor | ILoadTransform | ITransform | ILoader

