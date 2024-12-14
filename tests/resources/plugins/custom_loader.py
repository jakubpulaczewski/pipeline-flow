from core.models.phases import PipelinePhase, ILoader
from plugins.registry import  plugin


@plugin(PipelinePhase.LOAD_PHASE, "custom_load")
class CustomLoader(ILoader):
    id: str

    async def load_data(self, data):
        return None

