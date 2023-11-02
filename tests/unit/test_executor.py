import asyncio

from functools import reduce


class Job:
    def __init__(self, name, needs=None):
        self.name = name
        self.needs: str | list = needs
        self.is_executed = False
    
    async def execute(self):
        print(f"Executing {self.name}")
        await asyncio.sleep(3)  # Simulating some asynchronous task.
        print(f"Completed {self.name}")
        self.is_executed = True

def can_execute(job, executed_jobs):
    if job.needs is None:
        return True
    elif isinstance(job.needs, str):
        return job.needs in executed_jobs
    elif isinstance(job.needs, list):
        return all(need in executed_jobs for need in job.needs)

                         

async def execute_jobs(jobs):
    executed_jobs = set()

    while jobs:
        executable_jobs = [job for job in jobs if can_execute(job, executed_jobs)]

        if not executable_jobs:
            raise Exception("Circular dependency detected!")
        
        await asyncio.gather(*(job.execute() for job in executable_jobs))
        
        executed_jobs.update(job.name for job in executable_jobs)
        jobs = [job for job in jobs if not job.is_executed]

# Create Jobs
job1 = Job("job1")
job2 = Job("job2", needs=["job1", "job3"])
job3 = Job("job3", needs="job4")
job4 = Job("job4", needs="job1")

# 1 -> 4 -> 3 -> 2
# Execute
asyncio.run(execute_jobs([job1, job2, job3, job4]))



