from __future__ import annotations

from typing import Any

from core.models.phases import PHASE_CLASS_MAP, PhaseInstance, PipelinePhase
from core.models.pipeline import Pipeline


def parse_pipelines(pipelines_data: dict[str, dict[str, Any]]) -> list[Pipeline]:
    if not pipelines_data:
        raise ValueError("No Pipelines detected.")

    return [_create_pipeline(pipeline_name, pipeline_data) for pipeline_name, pipeline_data in pipelines_data.items()]


def _create_pipeline(pipeline_name: str, pipeline_data: dict[str, Any]) -> Pipeline:
    """Parse a single pipeline's data and return a pipeline instance."""
    if not pipeline_data:
        raise ValueError("Pipeline attributes are empty")

    phases = {}

    if "phases" not in pipeline_data:
        raise ValueError("The argument `phases` in pipelines must be specified.")
    for phase_name, phase_details in pipeline_data["phases"].items():
        phases[phase_name] = _create_phase(phase_name, phase_details)

    pipeline_data["phases"] = phases
    return Pipeline(name=pipeline_name, **pipeline_data)


def _create_phase(phase_name: str, phase_data: dict[str, Any]) -> PhaseInstance:
    phase_pipeline: PipelinePhase = PipelinePhase(phase_name)
    phase_class = PHASE_CLASS_MAP[phase_pipeline]

    return phase_class(**phase_data)
