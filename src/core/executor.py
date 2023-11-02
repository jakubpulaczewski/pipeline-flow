import asyncio
import yaml


DEFAULT_PLUGINS = ['s3']

def _can_execute(job, executed_jobs):
    if job.needs is None:
        return True
    elif isinstance(job.needs, str):
        return job.needs in executed_jobs
    elif isinstance(job.needs, list):
        return all(need in executed_jobs for need in job.needs)


async def execute_jobs(jobs):
    executed_jobs = set()

    while jobs:
        executable_jobs = [job for job in jobs if _can_execute(job, executed_jobs)]

        if not executable_jobs:
            raise Exception("Circular dependency detected!")
        
        await asyncio.gather(*(job.execute() for job in executable_jobs))
        
        executed_jobs.update(job.name for job in executable_jobs)
        jobs = [job for job in jobs if not job._is_executed]

