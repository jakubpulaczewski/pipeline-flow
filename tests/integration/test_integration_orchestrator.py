# Standard Imports


# Third Party Imports
import pytest


# Project Imports
from core.orchestrator import PipelineOrchestrator


@pytest.fixture
def orchestrator():
    return PipelineOrchestrator()


@pytest.mark.asyncio
async def test_execute_pipeline(etl_pipeline_factory, orchestrator) -> None:
    job1 = etl_pipeline_factory(name="Job1")

    await orchestrator.pipeline_queue_producer([job1])

    await orchestrator._execute_pipeline()

    assert orchestrator.pipeline_queue.qsize() == 0
    assert job1.is_executed == True


@pytest.mark.asyncio
async def test_execute_multiple_pipelines(etl_pipeline_factory, orchestrator) -> None:
    job1 = etl_pipeline_factory(name="Job1")
    job2 = etl_pipeline_factory(name="Job2")
    job3 = etl_pipeline_factory(name="Job3", needs=["Job1", "Job2"])
    jobs = [job1, job2, job3]


    executed = await orchestrator.execute_pipelines(pipelines=jobs)

    assert executed == {'Job1', 'Job2', "Job3"}
    assert job1.is_executed == True
    assert job2.is_executed == True
    assert job3.is_executed == True