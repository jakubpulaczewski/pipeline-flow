# Standard Imports
from functools import wraps

# Project Imports
from common.type_def import AsyncPlugin
from core.models.phases import PipelinePhase
from core.plugins import plugin


@plugin(PipelinePhase.EXTRACT_PHASE, "custom_extract")
def custom_extractor() -> AsyncPlugin:
    @wraps(custom_extractor)
    async def inner() -> str:
        return "CUSTOM EXTRACTED DATA"

    return inner


@plugin(PipelinePhase.LOAD_PHASE, "custom_load")
def custom_loader() -> AsyncPlugin:
    @wraps(custom_loader)
    async def inner(data: str) -> None:  # noqa: ARG001
        return

    return inner
