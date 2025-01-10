# Standard Imports
from functools import wraps
# Third Party Imports


# Project Imports
from core.plugins import plugin
from core.models.phases import PipelinePhase




@plugin(PipelinePhase.EXTRACT_PHASE, "custom_extract")
def custom_extractor():
    @wraps(custom_extractor)
    async def inner():
        return "CUSTOM EXTRACTED DATA"
    return inner



@plugin(PipelinePhase.LOAD_PHASE, "custom_load")
def custom_loader():
    @wraps(custom_loader)
    async def inner(data):
        return None
    return inner
