# Standard Imports
from __future__ import annotations

import asyncio
import functools as fn
from abc import ABC, abstractmethod
from concurrent.futures import Executor, ProcessPoolExecutor
from typing import TYPE_CHECKING, Any, Callable

# Project Imports
from common.type_def import Data, ExtractedData, LoadedData, TransformedData

# Third Party Imports

if TYPE_CHECKING:
    from core.models.extract import ExtractFunction, ExtractPhase, ExtractResult
    from core.models.transform import TransformFunction, TransformResult, TransformPhase
    from core.models.load import LoadFunction, LoadResult, LoadPhase

from core.models.extract import IExtractor
from core.models.load import ILoader
from core.models.pipeline import Pipeline, PipelineTypes
from core.models.transform import ITransformer, ITransformerELT, ITransformerETL


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
    async def run_extractor(extracts: ExtractPhase) -> list[ExtractResult]:
        data = []

        async with asyncio.TaskGroup() as extract_tg:
            for extract in extracts.steps:
                extracted_data = await extract_tg.create_task(extract.extract_data())
                data.append(extracted_data)

        return data

    @staticmethod
    def run_transformer(data, transformations: TransformPhase) -> TransformResult:
        result = data

        for tf in transformations.steps:
            if isinstance(tf, ITransformerETL) or isinstance(tf, ITransformerELT):
                result = tf.transform_data(result)

        return result

    @staticmethod
    async def run_loader(data, destinations: LoadPhase) -> LoadResult:
        async with asyncio.TaskGroup() as load_tg:
            for load in destinations.steps:
                await load_tg.create_task(load.load_data(data))


class ETLStrategy(PipelineStrategy):
    async def execute(self, pipeline: Pipeline) -> bool:
        # Extract (presumably I/O-bound, so using threads)
        extracted_data = await run_in_executor(
            None,
            self.run_extractor,
            pipeline.extract,
        )

        # TODO: need to perform merger in the main event loop or in a dedicated CPU-bound executor.
        # Transform
        transformed_data = await run_in_executor(
            ProcessPoolExecutor,
            self.run_transformer,
            extracted_data[0],  # TODO: Need a way to merge it: Look above.
            pipeline.transform,
        )

        # # Load (back to I/O-bound, so using to_thread)
        await run_in_executor(None, self.run_loader, pipeline.load)

        return True


class ELTStrategy(PipelineStrategy):

    async def execute(self, pipeline: Pipeline) -> bool:
        # Extract
        extracted_data, _ = await asyncio.to_thread(pipeline.extract)

        # Load
        pipeline.load(extracted_data)

        # Transform
        pipeline.transform(data=None, enable_data_flow=False)

        return True


# class ETLTStrategy(PipelineStrategy):

#     async def execute(self, pipeline: Pipeline) -> bool:
#         # Extrac
#         extracted_data, extract_data_flow = pipeline.extract()

#         # Transform
#         data_for_transform = self.data_manager.execute_flow(
#             extracted_data, extract_data_flow
#         )
#         transformed_data, transform_data_flow = pipeline.transform(data_for_transform)

#         # Load
#         data_for_load = self.data_manager.execute_flow(
#             transformed_data, transform_data_flow
#         )
#         pipeline.load(data_for_load)

#         # Transform
#         pipeline.transform(data=None, enable_data_flow=False)

#         return True


class PipelineStrategyFactory:

    @staticmethod
    def find_strategy(pipeline_type: str) -> PipelineStrategy:
        if pipeline_type == PipelineTypes.ETL_PIPELINE:
            return ETLStrategy
        elif pipeline_type == PipelineTypes.ELT_PIPELINE:
            return ELTStrategy
        else:
            raise ValueError(f"Unknown pipeline type: {pipeline_type}")
