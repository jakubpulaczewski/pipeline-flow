# pylint: disable=redefined-outer-name

import asyncio
from unittest.mock import AsyncMock

import pytest
from pytest_cases import case, parametrize_with_cases

import core.executor as executor  # pylint: disable=consider-using-from-import
from core.models import Job


@pytest.fixture()
def etl_basic_config():
    """An ETL basic configuration needed to create a job."""
    return {"extract": [], "transform": [], "load": []}


@pytest.fixture
def mock_jobs(etl_basic_config: dict[str, list]):
    "A mock to create a list of three jobs."
    job1 = Job(name="job1", **etl_basic_config)
    job2 = Job(name="job2", **etl_basic_config, needs=["job1"])
    job3 = Job(name="job2", **etl_basic_config, needs=["job1", "job2"])

    return [job1, job2, job3]


# Group 1: Success cases
@case(tags="success")
def case_no_dependency_success(etl_basic_config: dict[str, list]):
    """A test case where there is no job dependency."""
    executed_jobs = set()
    job1 = Job(name="job1", **etl_basic_config)

    return job1, executed_jobs


@case(tags="success")
def case_one_job_dependency_success(etl_basic_config: dict[str, list]):
    """A test case where a job has one dependency."""
    executed_jobs = {"job1"}
    job2 = Job(name="job2", **etl_basic_config, needs="job1")

    return job2, executed_jobs


@case(tags="success")
def case_multiple_job_dependency_success(etl_basic_config: dict[str, list]):
    """A test case where a job has multple dependencies."""

    executed_jobs = {"job1", "job2"}
    job3 = Job(name="job3", **etl_basic_config, needs=["job1", "job2"])

    return job3, executed_jobs


# Group 2: Failure cases
@case(tags="failure")
def case_one_job_dependency_failure(etl_basic_config: dict[str, list]):
    """A test case where a job has a dependency but the dependency is not finished executing."""
    executed_jobs = {}
    job2 = Job(name="job2", **etl_basic_config, needs="job1")

    return job2, executed_jobs


@case(tags="failure")
def case_multiple_job_dependency_failure(etl_basic_config: dict[str, list]):
    """A test case where a job has two dependencies but one dependency is not finished executing."""
    executed_jobs = {"job1"}
    job3 = Job(name="job2", **etl_basic_config, needs=["job1", "job2"])

    return job3, executed_jobs


# Test for success cases
@parametrize_with_cases("job, executed_jobs", cases=".", has_tag="success")
def test_can_execute_success(job: Job, executed_jobs: set) -> None:
    """A test that runs positive scenarios."""
    result = executor._can_execute(  # pylint: disable=protected-access
        job, executed_jobs
    )
    assert result is True


# Test for failure cases
@parametrize_with_cases("job, executed_jobs", cases=".", has_tag="failure")
def test_can_execute_failures(job: Job, executed_jobs: set) -> None:
    """A test that runs negative scenarios."""
    result = executor._can_execute(  # pylint: disable=protected-access
        job, executed_jobs
    )
    assert result is False


@pytest.mark.asyncio
async def test_execute_mark(mocker, etl_basic_config):
    """need to be fixed.."""
    async_mock = AsyncMock(return_value=lambda: asyncio.sleep(5))
    mocker.patch("core.models.Job.execute", side_effect=async_mock)

    job1 = Job(name="job1", **etl_basic_config)

    result = await job1.execute()
    print("*" * 10)
    print(result)
    assert result == 4
    print("*" * 10)


# @pytest.mark.asyncio
# async def test_execute_jobs_success(mocker, mock_jobs):
#     with patch('core.models.Job.execute', new_callable=AsyncMock):
#         for job in mock_jobs:
#             job.execute.side_effect = lambda: asyncio.sleep(0.5)

#     await asyncio.gather(mock_jobs[1].execute())

#     # await executor.execute_jobs(mock_jobs)
#     # assert all(job._is_executed for job in mock_jobs)


# # Create Jobs
# job1 = Job("job1")
# job2 = Job("job2", needs=["job1", "job3"])
# job3 = Job("job3", needs="job4")
# job4 = Job("job4", needs="job1")

# # 1 -> 4 -> 3 -> 2
# # Execute
# asyncio.run(execute_jobs([job1, job2, job3, job4]))
