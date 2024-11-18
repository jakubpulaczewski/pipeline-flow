# Standard Imports
from __future__ import annotations

from enum import Enum, unique
from typing import Self

# Third-party Imports
import pydantic as pyd

# Project imports
from core.models.phases import PipelinePhase, PHASE_TYPE



@unique
class PipelineType(Enum):
    """A config class that contains constants and utilities related to pipelines."""
    ETL = "ETL"
    ELT = "ELT"
    ETLT = "ETLT"


MANDATORY_PHASES_BY_PIPELINE_TYPE = {
    PipelineType.ETL: {
        PipelinePhase.EXTRACT_PHASE: True,
        PipelinePhase.TRANSFORM_PHASE: False,
        PipelinePhase.LOAD_PHASE: True,
    },
    PipelineType.ELT: {
        PipelinePhase.EXTRACT_PHASE: True,
        PipelinePhase.LOAD_PHASE: True,
        PipelinePhase.TRANSFORM_AT_LOAD_PHASE: True,
    },
    PipelineType.ETLT: {
        PipelinePhase.EXTRACT_PHASE: True,
        PipelinePhase.TRANSFORM_PHASE: True,
        PipelinePhase.LOAD_PHASE: True,
        PipelinePhase.TRANSFORM_AT_LOAD_PHASE: True,
    },
}

class Pipeline(pyd.BaseModel):
    model_config = pyd.ConfigDict(arbitrary_types_allowed=True)

    name: str
    type: PipelineType
    phases: dict[PipelinePhase, PHASE_TYPE]

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

    @pyd.model_validator(mode="after")
    def validate_pipeline_phase_mandatory(self: Self) -> Self:
        for phase, mandatory_phase in MANDATORY_PHASES_BY_PIPELINE_TYPE[self.type].items():
            if mandatory_phase:
                if phase not in self.phases or not self.phases[phase].steps:
                    raise ValueError(
                        f"Validation Failed! Mandatory phase '{phase}' cannot be empty or missing plugins."
                    )

        return self

