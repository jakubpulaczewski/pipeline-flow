# Standard Imports
import asyncio
import logging

from core.executor import PIPELINE_STRATEGY_MAP

# Project Imports
from core.models.pipeline import Pipeline
from core.parser import YamlConfig

# Third Party Imports


logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """Emphasizes the role of the class in executing the pipelines."""

    def __init__(self, config: YamlConfig) -> None:
        self.concurrency = config.concurrency
        self.pipeline_queue = asyncio.Queue()
        self.semaphore = asyncio.Semaphore(config.concurrency)

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

    async def pipeline_queue_producer(self, pipelines: list[Pipeline]) -> None:
        for pipeline in pipelines:
            logger.debug("Adding %s to central pipeline queue", pipeline.name)
            await self.pipeline_queue.put(pipeline)
            logger.debug("Added %s to central pipeline queue", pipeline.name)

    async def _execute_pipeline(self) -> None:
        if self.pipeline_queue.empty():
            return

        async with self.semaphore:
            while not self.pipeline_queue.empty():
                pipeline = await self.pipeline_queue.get()

                logger.info("Executing: %s ", pipeline.name)
                strategy = PIPELINE_STRATEGY_MAP[pipeline.type]
                pipeline.is_executed = await strategy().execute(pipeline)
                logger.info("Completed: %s", pipeline.name)

                self.pipeline_queue.task_done()

    async def execute_pipelines(self, pipelines: list[Pipeline]) -> set[str]:
        """Asynchronously executes parsed jobs."""
        if not pipelines:
            raise ValueError("The Pipeline list is empty. There is nothing to execute.")

        executed_pipelines = set()
        while pipelines:
            executable_pipelines = [
                pipeline for pipeline in pipelines if self._can_execute(pipeline, executed_pipelines)
            ]

            if not executable_pipelines:
                raise ValueError("Circular dependency detected!")

            # Produces a central queue of executable pipelines
            await self.pipeline_queue_producer(executable_pipelines)

            async with asyncio.TaskGroup() as tg:
                tasks = [tg.create_task(self._execute_pipeline()) for _ in range(self.concurrency)]  # noqa: F841

            executed_pipelines.update(pipeline.name for pipeline in executable_pipelines)
            pipelines = [pipeline for pipeline in pipelines if not pipeline.is_executed]

        return executed_pipelines
