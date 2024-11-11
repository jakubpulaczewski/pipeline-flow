# Standard Imports
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Type, TypeVar

# Third-party imports

# Project Imports
if TYPE_CHECKING:
    from core.models import IExtractor, ILoader, ITransformer


ExtractedData = TypeVar("ExtractedData", bound=Any)
TransformedData = TypeVar("TransformedData", bound=Any)
LoadedData = TypeVar("LoadedData", bound=Any)
type Data = ExtractedData | TransformedData | LoadedData


type ETL_CALLABLE = Type[IExtractor | ITransformer | ILoader]
type ETL_INSTANCE = IExtractor | ITransformer | ILoader
