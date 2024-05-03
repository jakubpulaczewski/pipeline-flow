# Standard Imports
import asyncio
import functools as fn
from concurrent.futures import Executor, ProcessPoolExecutor
from typing import Any, Callable, TypeVar

# Third Party Imports

# Project Imports
from common.utils.logger import setup_logger
from core.data_flow import DataFlowManager
from core.models import (
    Pipeline, 
    PipelineTypes,
    ExtractInterface,
    TransformInterface,
    LoadInterface,
)

logger = setup_logger(__name__)

T = TypeVar("T")

async def run_in_executor(
    executor: Executor | None,
    func: Callable[..., T],
    /,
    *args: Any,
    **kwargs: Any,
) -> T:
    """
    Run `func(*args, **kwargs)` asynchronously, using an executor.

    If the executor is None, use the default ThreadPoolExecutor.
    """
    return await asyncio.get_running_loop().run_in_executor(
        executor,
        fn.partial(func, *args, **kwargs),
    )


class PipelineExecutor:

    @staticmethod
    async def run_extractor(extracts: list[ExtractInterface]) -> list:
        data = []

        async with asyncio.TaskGroup() as extract_tg:
            for extract in extracts:
                extracted_data = await extract_tg.create_task(extract.extract_data())
                data.append(extracted_data)

        return data
      

    @staticmethod
    def run_transformer(data, transformations: list[TransformInterface]):
        result = data

        for tf in transformations:
            result = tf.transform_data(result)
        
        return result

    @staticmethod
    async def run_loader(data, destinations: list[LoadInterface]):
        async with asyncio.TaskGroup() as load_tg:
            for load in destinations:
                await load_tg.create_task(load.load_data(data))
                

class PipelineOrchestrator:
    """Emphasizes the role of the class in executing the pipelines."""

    def __init__(self):
        self.data_manager = DataFlowManager()
        self.executor = PipelineExecutor()
        self.concurrency = 2 # TODO: set to something

    @staticmethod
    def _can_execute(pipeline: Pipeline, executed_pipelines: set[Pipeline]) -> bool:
        """A function that checks if a given pipeline is executable or has an external dependency."""
        if pipeline.needs is None:
            return True
        if isinstance(pipeline.needs, str):
            return pipeline.needs in executed_pipelines
        if isinstance(pipeline.needs, list):
            return all(need in executed_pipelines for need in pipeline.needs)
        return False

    async def _execute_pipeline(self, pipeline_queue: asyncio.Queue) -> None:

        async with asyncio.Semaphore(self.concurrency):
            while not pipeline_queue.empty():
                pipeline = await pipeline_queue.get()

                logger.info("Executing: %s ", pipeline.name)
                if pipeline.type == PipelineTypes.ETL_PIPELINE:
                    await self._execute_etl(pipeline)
                elif pipeline.type == PipelineTypes.ELT_PIPELINE:
                    raise NotImplementedError()
                    #await self._execute_elt(pipeline)
                elif pipeline.type == PipelineTypes.ETLT_PIPELINE:
                    raise NotImplementedError()
                    #self._execute_etlt(pipeline)
                logger.info("Completed: %s", pipeline.name)
                pipeline.is_executed = True
                pipeline_queue.task_done()

    async def _execute_etl(self, pipeline: Pipeline) -> bool:
        # Extract (presumably I/O-bound, so using threads)
        extracted_data = await run_in_executor(
            None,
            self.executor.run_extractor,
            pipeline.extract,
        )

        # Perform merging in the main event loop or in a dedicated CPU-bound executor
        merged_data = pipeline.extract_merger_strategy.merge(extracted_data)

        # Extractor Data Flow - Data for Next Phase
        extractor_data_flow = self.data_manager.execute_flow(merged_data, pipeline.extractor_data_flow_type)

        # Transform
        transformed_data = await run_in_executor(
            ProcessPoolExecutor,
            self.executor.run_transformer,
            extractor_data_flow,
            pipeline.transform
        )

        # Transformator Data Flow - Data for Next Phase
        transformer_data_flow = self.data_manager.execute_flow(transformed_data, pipeline.transformer_data_flow_type)
            
        # # Load (back to I/O-bound, so using to_thread)
        await asyncio.to_thread(pipeline.run_loader, transformer_data_flow, pipeline.load)

        return True

    # async def _execute_elt(self, pipeline: Pipeline) -> bool:
    #     # Extract
    #     extracted_data, _ = await asyncio.to_thread(pipeline.extract)

    #     # Load
    #     pipeline.load(extracted_data)

    #     # Transform
    #     pipeline.transform(data=None, enable_data_flow=False)

    #     return True

    # async def _execute_etlt(self, pipeline: Pipeline) -> bool:
    #     # Extract
    #     extracted_data, extract_data_flow = pipeline.extract()

    #     # Transform
    #     data_for_transform = self.data_manager.execute_flow(
    #         extracted_data, extract_data_flow
    #     )
    #     transformed_data, transform_data_flow = pipeline.transform(data_for_transform)

    #     # Load
    #     data_for_load = self.data_manager.execute_flow(
    #         transformed_data, transform_data_flow
    #     )
    #     pipeline.load(data_for_load)

    #     # Transform
    #     pipeline.transform(data=None, enable_data_flow=False)

    #     return True

    async def pipeline_queue_producer(pipeline_queue: asyncio.Queue, pipelines: list[Pipeline]):
        for pipeline in pipelines:
            logger.debug("Adding %s to central pipeline queue", pipeline.name)
            await pipeline_queue.put(pipeline)
            logger.debug("Added %s to central pipeline queue", pipeline.name)

    async def execute_pipelines(self, pipelines) -> set:
        """Asynchronously executes parsed jobs."""
        executed_pipelines = set()

        tasks = []
        pipeline_queue = asyncio.Queue()

        while pipelines:
            executable_pipelines = [
                pipeline
                for pipeline in pipelines
                if self._can_execute(pipeline, executed_pipelines)
            ]

            if not executable_pipelines:
                raise ValueError("Circular dependency detected!")
            
            # Produces a central queue of executable pipelines
            self.pipeline_queue_producer(pipeline_queue, executable_pipelines)
        
            async with asyncio.TaskGroup() as tg:
                for _ in range(self.concurrency):
                    task = tg.create_task(self._execute_pipeline(pipeline_queue))
                    tasks.append(task)

            executed_pipelines.update(pipeline.name for pipeline in executable_pipelines)
            pipelines = [pipeline for pipeline in pipelines if not pipeline.is_executed]

        return executed_pipelines
