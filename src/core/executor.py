# Standard Imports
from __future__ import annotations

import asyncio
import logging

from abc import ABCMeta, abstractmethod
from typing import Callable, TYPE_CHECKING

# Third Party Imports

# Project Imports
from common.type_def import ExtractedData, TransformedData, TransformLoadedData
from common.decorator import time_it

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


class PipelineExecutor:

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

    @time_it
    async def run_extractor(self, extracts: ExtractPhase) -> ExtractedData:
        results = {}
 
        # Run Pre-Requisite here using extracts.pre
        if extracts.pre:
            await self._execute_processing_steps(PipelinePhase.EXTRACT_PHASE, extracts.pre)

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


    @time_it
    def run_transformer(self, data: ExtractedData, transformations: TransformPhase) -> TransformedData:
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


    @time_it
    async def run_loader(self, data: ExtractedData | TransformedData, destinations: LoadPhase) -> list[dict]:
        if destinations.pre:
            await self._execute_processing_steps(PipelinePhase.LOAD_PHASE, destinations.pre)
            
        async with asyncio.TaskGroup() as load_tg:
            try:
                tasks = {step.id: load_tg.create_task(step.load_data(data)) for step in destinations.steps}
            except Exception as e:
                error_message = f"Error during `load`): {e}"
                logger.error(error_message)
                raise LoadException(error_message) from e
                
        if destinations.post:
            await self._execute_processing_steps(PipelinePhase.LOAD_PHASE, destinations.post)

        results = [{'id': id, 'success': True} for id, task in tasks.items()]
        return results


    @time_it
    def run_transformer_after_load(self, transformations: TransformLoadPhase) -> list[dict]:
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



class PipelineStrategy(metaclass=ABCMeta):

    def __init__(self, executor: PipelineExecutor = None) -> None:
        self.executor = executor or PipelineExecutor()
        
    @abstractmethod
    def execute(self, pipeline: Pipeline) -> bool:
        raise NotImplementedError("This has to be implemented by the subclasses.")



class ETLStrategy(PipelineStrategy):
    def __init__(self, executor: PipelineExecutor = None) -> None:
        super().__init__(executor)

    async def execute(self, pipeline: Pipeline) -> bool:
        extracted_data = await self.executor.run_extractor(pipeline.extract)
        
        # Transform (CPU-bound work, so offload to executor)
        transformed_data = await asyncio.get_running_loop().run_in_executor(None, self.executor.run_transformer, extracted_data, pipeline.transform)
        
        loaded = await self.executor.run_loader(transformed_data, pipeline.load)

        return True


class ELTStrategy(PipelineStrategy):
    def __init__(self, executor: PipelineExecutor = None) -> None:
        super().__init__(executor)

    async def execute(self, pipeline: Pipeline) -> bool:
        extracted_data = await self.executor.run_extractor(pipeline.extract)

        load_result = await self.executor.run_loader(extracted_data, pipeline.load)

        transform_load_result = self.executor.run_transformer_after_load(pipeline.load_transform)

        return True


class ETLTStrategy(PipelineStrategy):
    def __init__(self, executor: PipelineExecutor = None) -> None:
        super().__init__(executor)

    async def execute(self, pipeline: Pipeline) -> bool:
        extracted_data = await self.executor.run_extractor(pipeline.extract)
        
        transformed_data = await asyncio.get_running_loop().run_in_executor(None, self.executor.run_transformer, extracted_data, pipeline.transform)

        loaded = await self.executor.run_loader(transformed_data, pipeline.load)

        transform_load_result = self.executor.run_transformer_after_load(pipeline.load_transform)

        return True


PIPELINE_STRATEGY_MAP = {
    PipelineType.ETL: ETLStrategy,
    PipelineType.ELT: ELTStrategy,
    PipelineType.ETLT: ETLTStrategy
}
