from core.models.phases import PipelinePhase, ILoader
from plugins.registry import  plugin



@plugin(PipelinePhase.LOAD_PHASE, "pandas_s3_load")
class PandasS3LoadPlugin(ILoader):
    id: str

    async def load_data(self):
        return "Pandas S3 Loaded Data"

