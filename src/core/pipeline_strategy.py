# Standard Imports
from __future__ import annotations

import asyncio
import logging

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

# Third Party Imports

# Project Imports
import common.utils as utils
from common.type_def import ExtractedData, TransformedData

if TYPE_CHECKING:
    from core.models.phase_wrappers import (
        TransformResult,
        LoadResult,
    )

    from core.models.phases import (
        ExtractPhase,
        TransformPhase,
        LoadPhase,
        TransformLoadPhase,
        iMerger
    )
from core.models.phase_wrappers import (
    extract_decorator,
    transform_decorator,
    load_decorator
)

from core.models.pipeline import Pipeline, PipelineType



logger = logging.getLogger(__name__)


class PipelineStrategy(ABC):

    @abstractmethod
    def execute(self, pipeline: Pipeline) -> bool:
        raise NotImplementedError("This has to be implemented by the subclasses.")


    @staticmethod
    async def run_extractor(extracts: ExtractPhase) -> ExtractedData:
        data = {}

        async with asyncio.TaskGroup() as extract_tg:
            # Concurrently, fetches all extracts.
            for extract in extracts.steps:
                decorated_extract = extract_decorator(extract.id, extract.extract_data)
                extracted_data = await extract_tg.create_task(decorated_extract())

                if extract.id in data:
                    raise ValueError("The `ID` is not unique. There already exists an 'ID' with this name.")
                
                data[extract.id] = extracted_data

            if extracts.merge:
                return extracts.merge.merge_data(data)

        del data
        return extracted_data

    @staticmethod
    def run_transformer(data: ExtractedData, transformations: TransformPhase) -> TransformResult:
        for tf in transformations.steps:
            logger.info(f"Applying transformation: {tf.id}")
            decorated_tf = transform_decorator(tf.id, tf.transform_data)
            transformed_data = decorated_tf(data)

            logger.info(f"Transformation {tf.id} succeeded.")
            # Continue passing the transformed data to the next step
            data = transformed_data.result
            
        return transformed_data

    @staticmethod
    async def run_loader(data: ExtractedData | TransformedData, destinations: LoadPhase) -> LoadResult:
        async with asyncio.TaskGroup() as load_tg:
            results = []
            for load in destinations.steps:
                decorated_load = load_decorator(load.id, load.load_data)
                result = await load_tg.create_task(decorated_load(data))
                results.append(result)
            return results

    @staticmethod
    def run_transformer_after_load(transformations: TransformLoadPhase) -> TransformResult:
        results = []
        for tf in transformations.steps:
            decorated_tf = transform_decorator(tf.id, tf.transform_data)
            result = decorated_tf()
            results.append(result)
        return results


    
class ETLStrategy(PipelineStrategy):
    async def execute(self, pipeline: Pipeline) -> bool:
        extracted_data = await self.run_extractor(pipeline.extract)
        
        # Transform (CPU-bound work, so offload to executor)
        transformed_data = await utils.run_in_executor(
            None, #
            self.run_transformer,
            extracted_data,
            pipeline.transform,
        )

        loaded = await self.run_loader(transformed_data.result, pipeline.load)

        return True


class ELTStrategy(PipelineStrategy):


    async def execute(self, pipeline: Pipeline) -> bool:
        extracted_data = await self.run_extractor(pipeline.extract)

        load_result = await self.run_loader(extracted_data, pipeline.load)

        transform_load_result = self.run_transformer_after_load(pipeline.load_transform)

        return True


class ETLTStrategy(PipelineStrategy):


    async def execute(self, pipeline: Pipeline) -> bool:
        extracted_data = await self.run_extractor(pipeline.extract)
        
        transformed_data = await utils.run_in_executor(
            None, #
            self.run_transformer,
            extracted_data,
            pipeline.transform,
        )

        loaded = await self.run_loader(transformed_data.result, pipeline.load)

        transform_load_result = self.run_transformer_after_load(pipeline.load_transform)

        return True


class PipelineStrategyFactory:

    @staticmethod
    def get_pipeline_strategy(pipeline_type: PipelineType) -> PipelineStrategy:
        if pipeline_type == PipelineType.ETL:
            return ETLStrategy()
        elif pipeline_type == PipelineType.ELT:
            return ELTStrategy()
        elif pipeline_type == PipelineType.ETLT:
            return ETLTStrategy()
        else:
            raise ValueError(f"Unknown pipeline type: {pipeline_type}")
