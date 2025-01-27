import logging
from enum import Enum, unique
from typing import Annotated, cast

from pydantic import BaseModel, ConfigDict, ValidationInfo, field_validator

from core.models.phases import (
    ExtractPhase,
    LoadPhase,
    PhaseInstance,
    PipelinePhase,
    TransformLoadPhase,
    TransformPhase,
)


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
        PipelinePhase.TRANSFORM_PHASE: False,
        PipelinePhase.LOAD_PHASE: True,
        PipelinePhase.TRANSFORM_AT_LOAD_PHASE: True,
    },
}


class Pipeline(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: Annotated[str, "Name of the pipeline job"]
    type: PipelineType
    phases: dict[PipelinePhase, PhaseInstance]

    # Optional
    description: str | None = None
    needs: str | list[str] | None = None

    # Private
    _is_executed: bool = False

    @property
    def is_executed(self) -> bool:
        return self._is_executed

    @is_executed.setter
    def is_executed(self, value: bool) -> None:
        self._is_executed = value

    @property
    def extract(self) -> ExtractPhase:
        return cast(ExtractPhase, self.phases[PipelinePhase.EXTRACT_PHASE])

    @property
    def transform(self) -> TransformPhase:
        return cast(TransformPhase, self.phases[PipelinePhase.TRANSFORM_PHASE])

    @property
    def load(self) -> LoadPhase:
        return cast(LoadPhase, self.phases[PipelinePhase.LOAD_PHASE])

    @property
    def load_transform(self) -> TransformLoadPhase:
        return cast(TransformLoadPhase, self.phases[PipelinePhase.TRANSFORM_AT_LOAD_PHASE])

    @field_validator("phases")
    @classmethod
    def validate_phase_existence(
        cls, phases: dict[PipelinePhase, PhaseInstance], info: ValidationInfo
    ) -> dict[PipelinePhase, PhaseInstance]:
        pipeline_type = info.data["type"]

        pipeline_phases = MANDATORY_PHASES_BY_PIPELINE_TYPE[pipeline_type]
        mandatory_phases = {phase for phase, is_mandatory in pipeline_phases.items() if is_mandatory}
        optional_phases = {phase for phase, is_mandatory in pipeline_phases.items() if not is_mandatory}
        # Compute missing and extra phases
        provided_phases = set(phases.keys())

        missing_phases = set(mandatory_phases) - set(provided_phases | optional_phases)
        extra_phases = set(provided_phases) - (mandatory_phases | optional_phases)

        if missing_phases or extra_phases:
            if missing_phases:
                logging.error(
                    "Validation Error: Missing phases for pipeline type '%s': %s", pipeline_type, missing_phases
                )
            if extra_phases:
                logging.warning(
                    "Validation Warning: Extra phases provided for pipeline type '%s': %s", pipeline_type, extra_phases
                )

            error_msg = (  # noqa: UP032
                "Validation Error: The provided phases do not match the required phases for pipeline type '{}'. "
                "Missing phases: {}. Extra phases: {}."
            ).format(pipeline_type, missing_phases, extra_phases)
            raise ValueError(error_msg)

        msg = f"Phase validation successful for pipeline type '{pipeline_type}'"
        logging.info(msg)
        return phases
