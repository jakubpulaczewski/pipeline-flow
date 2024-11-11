# Standard Imports
import asyncio

# Project Imports
from common.utils.logger import setup_logger
from core.models.pipeline import Pipeline
from core.pipeline_strategy import PipelineStrategyFactory

# Third Party Imports


logger = setup_logger(__name__)


class PipelineOrchestrator:
    """Emphasizes the role of the class in executing the pipelines."""

    def __init__(self):
        self.concurrency = 2  # TODO: set to something

    @staticmethod
    async def pipeline_queue_producer(
        pipeline_queue: asyncio.Queue, pipelines: list[Pipeline]
    ) -> None:
        for pipeline in pipelines:
            logger.debug("Adding %s to central pipeline queue", pipeline.name)
            await pipeline_queue.put(pipeline)
            logger.debug("Added %s to central pipeline queue", pipeline.name)

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

                strategy = PipelineStrategyFactory.find_strategy(pipeline.type)
                await strategy.execute(pipeline)

                logger.info("Completed: %s", pipeline.name)
                pipeline.is_executed = True

                pipeline_queue.task_done()

    async def execute_pipelines(self, pipelines: list[Pipeline]) -> set[str]:
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

            executed_pipelines.update(
                pipeline.name for pipeline in executable_pipelines
            )
            pipelines = [pipeline for pipeline in pipelines if not pipeline.is_executed]

        return executed_pipelines
