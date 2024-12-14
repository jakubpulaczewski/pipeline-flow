# Standard Imports

# Project Imports
from core.models.phases import PipelinePhase
from core.models.pipeline import PipelineType

# Third-party imports

EXTRACT_PHASE = PipelinePhase.EXTRACT_PHASE
LOAD_PHASE = PipelinePhase.LOAD_PHASE
TRANSFORM_PHASE = PipelinePhase.TRANSFORM_PHASE
LOAD_TRANSFORM_PHASE = PipelinePhase.TRANSFORM_AT_LOAD_PHASE

ETL = PipelineType.ETL
ELT = PipelineType.ELT
ETLT = PipelineType.ETLT
