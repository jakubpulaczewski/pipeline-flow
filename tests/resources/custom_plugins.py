# Standard Imports
from functools import wraps

from common.type_def import Plugin
from core.models.phases import PipelinePhase

# Third Party Imports
# Project Imports
from core.plugins import plugin


@plugin(PipelinePhase.EXTRACT_PHASE, "custom_extract")
def custom_extractor() -> Plugin:
    @wraps(custom_extractor)
    async def inner() -> str:
        return "CUSTOM EXTRACTED DATA"

    return inner


@plugin(PipelinePhase.LOAD_PHASE, "custom_load")
def custom_loader() -> Plugin:
    @wraps(custom_loader)
    async def inner(data: str) -> None:  # noqa: ARG001
        return

    return inner
