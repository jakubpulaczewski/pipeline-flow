# Standard Imports
from __future__ import annotations

import asyncio
import functools as fn
from abc import ABC, abstractmethod
from concurrent.futures import Executor, ProcessPoolExecutor
from typing import TYPE_CHECKING, Any

# Third Party Imports

# Project Imports
from common.type_def import ExtractedData, LoadedData, TransformedData
from common.logger import setup_logger

if TYPE_CHECKING:
    from core.models.phase_wrappers import (
        ExtractFunction,
        ExtractResult,
        TransformResult,
        TransformFunction,
        LoadResult,
        LoadFunction
    )

    from core.models.phases import (
        ExtractPhase,
        TransformPhase,
        LoadPhase,
        TransformLoadPhase
    )
from core.models.phase_wrappers import (
    extract_decorator,
    transform_decorator,
    load_decorator
)

from core.models.pipeline import Pipeline, PipelineType



logger = setup_logger(__name__)

async def run_in_executor(
    executor: Executor | None,
    func: ExtractFunction | TransformFunction | LoadFunction,
    *args: Any,
    **kwargs: Any,
) -> ExtractResult | TransformResult | LoadResult:
    """
    Run asyncio's executor asynchronously.

    If the executor is None, use the default ThreadPoolExecutor.
    """
    return await asyncio.get_running_loop().run_in_executor(
        executor,
        fn.partial(func, *args, **kwargs),
    )


class PipelineStrategy:

    @abstractmethod
    def execute(self, pipeline: Pipeline) -> bool:
        raise NotImplementedError("This has to be implemented by the subclasses.")

    @staticmethod
    async def run_extractor(extracts: ExtractPhase) -> dict[str, ExtractResult]:
        data = {}

        async with asyncio.TaskGroup() as extract_tg:
            # Concurrently, fetches all extracts.
            for extract in extracts.steps:
                decorated_extract = extract_decorator(extract.id, extract.extract_data)
                extracted_data = await extract_tg.create_task(decorated_extract())

                # TODO: Should this be moved to Like a validator class that checks everything????
                if extract.id in data:
                    raise ValueError("The `ID` is not unique. There already exists an 'ID' with this name.")
                
                data[extract.id] = extracted_data

        return data

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


    @staticmethod
    async def merg_temp(extracted_data):
        return next(iter(extracted_data.values())).result
    
class ETLStrategy(PipelineStrategy):
    async def execute(self, pipeline: Pipeline) -> bool:
        extracted_data = await self.run_extractor(pipeline.extract)
        
        # TODO: need to perform merger in the main event loop or in a dedicated CPU-bound executor.
        merged_data = await self.merg_temp(extracted_data)

        # Transform (CPU-bound work, so offload to executor)
        transformed_data = await run_in_executor(
            None, #
            self.run_transformer,
            merged_data,
            pipeline.transform,
        )

        loaded = await self.run_loader(transformed_data.result, pipeline.load)

        return True


class ELTStrategy(PipelineStrategy):


    async def execute(self, pipeline: Pipeline) -> bool:
        extracted_data = await self.run_extractor(pipeline.extract)
        # TODO: need to perform merger in the main event loop or in a dedicated CPU-bound executor.
        merged_data = await self.merg_temp(extracted_data)

        # Load
        load_result = await self.run_loader(merged_data, pipeline.load)

        # Transform
        transform_load_result = self.run_transformer_after_load(pipeline.load_transform)

        return True


class ETLTStrategy(PipelineStrategy):


    async def execute(self, pipeline: Pipeline) -> bool:
        extracted_data = await self.run_extractor(pipeline.extract)
        
        # TODO: need to perform merger in the main event loop or in a dedicated CPU-bound executor.
        merged_data = await self.merg_temp(extracted_data)

        transformed_data = await run_in_executor(
            None, #
            self.run_transformer,
            merged_data,
            pipeline.transform,
        )

        loaded = await self.run_loader(transformed_data.result, pipeline.load)

        # Transform
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
