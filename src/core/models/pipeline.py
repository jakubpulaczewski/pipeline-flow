# Standard Imports
from __future__ import annotations

# Third-party Imports
import pydantic as pyd

# Project imports
from common.config import PipelineTypes
from core.models.extract import ExtractPhase
from core.models.load import LoadPhase
from core.models.transform import TransformPhase


class Pipeline(pyd.BaseModel):
    """A pipeline class that executes ETL steps."""

    model_config = pyd.ConfigDict(arbitrary_types_allowed=True)

    name: str
    type: str

    # ETL Phases
    extract: ExtractPhase
    transform: TransformPhase
    load: LoadPhase

    # Optional
    description: str | None = None
    needs: str | list[str] | None = None

    # Private
    __is_executed: bool = False

    @property
    def is_executed(self) -> bool:
        return self.__is_executed

    @is_executed.setter
    def is_executed(self, value: bool) -> None:
        self.__is_executed = value

    @pyd.field_validator("type")
    def pipeline_type_match(cls: Pipeline, value: str) -> str:
        """A validator that validates pipeline type against the allowed pipeline types."""
        upper_cased_pipeline = value.upper()
        if upper_cased_pipeline not in PipelineTypes.ALLOWED_PIPELINE_TYPES:
            raise ValueError("A pipeline must be of following types: ETL, ELT, ETLT")
        return upper_cased_pipeline
