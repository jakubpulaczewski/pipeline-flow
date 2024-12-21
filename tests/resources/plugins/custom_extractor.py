from plugins.registry import  plugin
from core.models.phases import PipelinePhase, IExtractor


@plugin(PipelinePhase.EXTRACT_PHASE, "custom_extract")
class CustomExtractor(IExtractor):
    id: str

    async def extract_data(self):
        return "Pandas S3 Loaded Data"

