""" A Pandas plug-in for extracting and loading data from S3. """


from plugins.registry import plugin
from core.models.phases import PipelinePhase, IExtractor
from common.type_def import ExtractedData

@plugin(PipelinePhase.EXTRACT_PHASE, "pandas_s3_extract")
class PandasS3ExtractPlugin(IExtractor):
    name: str

    async def extract_data(self) -> ExtractedData:
        return "Pandas S3 Extracted Data"