from __future__ import annotations

from typing import Any

from pipeline_flow.core.models.phases import (
    ExtractPhase,
    LoadPhase,
    Phase,
    PipelinePhase,
    TransformLoadPhase,
    TransformPhase,
)
from pipeline_flow.core.models.pipeline import Pipeline

PHASE_CLASS_MAP = {
    PipelinePhase.EXTRACT_PHASE: ExtractPhase,
    PipelinePhase.TRANSFORM_PHASE: TransformPhase,
    PipelinePhase.LOAD_PHASE: LoadPhase,
    PipelinePhase.TRANSFORM_AT_LOAD_PHASE: TransformLoadPhase,
}


def parse_pipelines(pipelines_data: dict[str, dict[str, Any]]) -> list[Pipeline]:
    if not pipelines_data:
        raise ValueError("No Pipelines detected.")

    return [_create_pipeline(pipeline_name, pipeline_data) for pipeline_name, pipeline_data in pipelines_data.items()]


def _create_pipeline(pipeline_name: str, phase_data: dict[str, Any]) -> Pipeline:
    """Parse a single pipeline's data and return a pipeline instance."""
    if not phase_data:
        raise ValueError("Pipeline attributes are empty")

    phases = {}

    if "phases" not in phase_data:
        raise ValueError("The argument `phases` in pipelines must be specified.")
    for phase_name, phase_details in phase_data["phases"].items():
        phases[phase_name] = _parse_phase(phase_name, phase_details)

    phase_data["phases"] = phases
    return Pipeline(name=pipeline_name, **phase_data)


def _parse_phase(phase_name: str, phase_data: dict[str, Any]) -> Phase:
    phase_pipeline: PipelinePhase = PipelinePhase(phase_name)
    phase_class = PHASE_CLASS_MAP[phase_pipeline]

    return phase_class(**phase_data)
