# Standard Imports

# Third Party Imports
import pytest


# Project Imports
import tests.resources.mocks as mocks

from core.orchestrator import PipelineOrchestrator
from core.parser import YamlConfig
from plugins.registry import PluginWrapper


@pytest.fixture()
def orchestrator():
    config = YamlConfig(engine='native', concurrency=2)
    return PipelineOrchestrator(config=config)


@pytest.mark.asyncio
async def test_execute_pipelines_with_empty_list(orchestrator):
    with pytest.raises(ValueError, match="The Pipeline list is empty. There is nothing to execute."):
        await orchestrator.execute_pipelines(pipelines=[])

@pytest.mark.asyncio
async def test_execute_pipeline(etl_pipeline_factory, orchestrator) -> None:
    job1 = etl_pipeline_factory(
        name="ETL Pipeline 1",
        extract=[PluginWrapper(id='mock_extractor', func=mocks.mock_extractor(id='mock_extractor'))],
        transform=[PluginWrapper(id='mock_transformer', func=mocks.mock_transformer(id='mock_transformer'))],
        load=[PluginWrapper(id='mock_loader', func=mocks.mock_loader(id='mock_loader'))]
    )
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
