# Standard Imports
from __future__ import annotations

import asyncio
import logging

from abc import ABC, abstractmethod
from typing import Callable, TYPE_CHECKING

# Third Party Imports

# Project Imports
import common.utils as utils
from common.type_def import ExtractedData, TransformedData, TransformLoadedData

if TYPE_CHECKING:
    from core.models.phases import (
        ExtractPhase,
        TransformPhase,
        LoadPhase,
        TransformLoadPhase
    )
from core.models.exceptions import (
    ExtractException,
    LoadException,
    TransformException,
    TransformLoadException
)

from core.models.pipeline import Pipeline, PipelineType
from core.models.phases import PipelinePhase


logger = logging.getLogger(__name__)


class PipelineStrategy(ABC):

    @abstractmethod
    def execute(self, pipeline: Pipeline) -> bool:
        raise NotImplementedError("This has to be implemented by the subclasses.")


    @staticmethod
    async def _execute_processing_steps(phase: PipelinePhase, processing_steps: list[Callable]):
        async def run_task(task):
            if asyncio.iscoroutinefunction(task):
                return await task()
            else:
                return await asyncio.get_running_loop().run_in_executor(None, task)
        try:
            async with asyncio.TaskGroup() as task_group:
                tasks = [task_group.create_task(run_task(step)) for step in processing_steps]
                        
            results = [task.result() for task in tasks]
            return results
        
        except Exception as e:
            error_message = f"Error during {phase} processing execution: {e}"
            logger.error(error_message)
            raise

    @staticmethod
    async def run_extractor(extracts: ExtractPhase) -> ExtractedData:
        results = {}
 
        # Run Pre-Requisite here using extracts.pre
        if extracts.pre:
            await PipelineStrategy._execute_processing_steps(PipelinePhase.EXTRACT_PHASE, extracts.pre)

        async with asyncio.TaskGroup() as extract_tg:
            try:
                tasks = {step.id: extract_tg.create_task(step.extract_data()) for step in extracts.steps}
            except Exception as e:
                error_message = f"Error during `extraction`: {e}"
                logger.error(error_message)
                raise ExtractException(error_message) from e
            
        if len(extracts.steps) == 1:
            return tasks[extracts.steps[0].id].result()
        
        results = {id: task.result() for id, task in tasks.items()}
        return extracts.merge.merge_data(results)


    @staticmethod
    def run_transformer(data: ExtractedData, transformations: TransformPhase) -> TransformedData:
        try:
            for tf in transformations.steps:
                logger.info(f"Applying transformation: {tf.id}")
                transformed_data = tf.transform_data(data)

                # Continue passing the transformed data to the next step
                data = transformed_data
        except Exception as e:
            error_message = f"Error during `transform` (ID: {tf.id}): {e}"
            logger.error(error_message)
            raise TransformException(error_message) from e


        return transformed_data

    @staticmethod
    async def run_loader(data: ExtractedData | TransformedData, destinations: LoadPhase) -> list[dict]:
        if destinations.pre:
            await PipelineStrategy._execute_processing_steps(PipelinePhase.LOAD_PHASE, destinations.pre)
            
        async with asyncio.TaskGroup() as load_tg:
            try:
                tasks = {step.id: load_tg.create_task(step.load_data(data)) for step in destinations.steps}
            except Exception as e:
                error_message = f"Error during `load`): {e}"
                logger.error(error_message)
                raise LoadException(error_message) from e
                
        if destinations.post:
            await PipelineStrategy._execute_processing_steps(PipelinePhase.LOAD_PHASE, destinations.post)

        results = [{'id': id, 'success': True} for id, task in tasks.items()]
        return results

    @staticmethod
    def run_transformer_after_load(transformations: TransformLoadPhase) -> list[dict]:
        results = []
        for tf in transformations.steps:
            try:
                tf.transform_data()
                results.append({'id': tf.id, 'success': True})
            except Exception as e:
                error_message = f"Error during `transform_at_load` (ID: {tf.id}): {e}"
                logger.error(error_message)
                raise TransformLoadException(error_message) from e
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

        loaded = await self.run_loader(transformed_data, pipeline.load)

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

        loaded = await self.run_loader(transformed_data, pipeline.load)

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
