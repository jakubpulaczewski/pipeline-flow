# Standard Imports
from typing import TYPE_CHECKING

# Third-party Imports


# Project Imports
from core.models.phases import PipelinePhase, iMerger
from plugins.registry import plugin

if TYPE_CHECKING:
    from common.type_def import ExtractedData

@plugin(PipelinePhase.EXTRACT_PHASE, "pandas_merge")
class PandasMergeStrategy(iMerger):

    def merge(self, extracted_data: dict[str, ExtractedData]):
        result = None
        for id, data in extracted_data:
            result +=data
        return data

