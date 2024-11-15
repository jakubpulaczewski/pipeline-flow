import asyncio
import time
from timeit import default_timer as timer

def lower(x):
    return x.lower()

def capitalize(x):
    return x.capitalize()

async def extract(name: str, return_value,  wait: int):
    print(f"Extract {name} - START ")
    await asyncio.sleep(wait)
    print(f"Extract {name} - END ")
    return return_value

def transform(name: str, func, value, wait: int):
    print(f"TRANSFORM {name} - START ")
    time.sleep(wait)
    print(f"Transform {name} - END ")
    return func(value)

async def load(name: str, wait):
    print(f"LOAD {name} - START ")
    await asyncio.sleep(wait)
    print(f"LOAD {name} - END ")

async def pipeline1():
    print("Pipeline 1 Start")
    async with asyncio.TaskGroup() as extract_group:
        e1= extract_group.create_task(extract('P1_E1', 'A', 2))
        e2= extract_group.create_task(extract('P1_E2', 'B',  2))

    result = e1.result() + e2.result()

    for tf in [lower, capitalize]:
        #result = transform(tf, tf, result, 5) # 24 sec with this
        result = await asyncio.to_thread(transform, 'P1' + str(tf), tf, result, 2)  # only 9 sec with this.

    print(result)

    async with asyncio.TaskGroup() as load_group:
        l1= load_group.create_task(load('P1_L1', 2))
        l2= load_group.create_task(load('P1_L2', 2))

    print("Pipeline 1 End")

async def pipeline2():
    print("Pipeline 2 Start")
    async with asyncio.TaskGroup() as extract_group:
        e1= extract_group.create_task(extract('P2_E1', 'FF', 2))
        e2= extract_group.create_task(extract('P2_E2', 'GG', 2))
    
    result = e1.result() + e2.result()

    for tf in [lower, capitalize]:
        #result = transform(tf, tf, result, 5) # 24 sec with this.
        result = await asyncio.to_thread(transform, 'P2' + str(tf), tf, result, 2) # only 9 sec with this

    print(result)

    async with asyncio.TaskGroup() as load_group:
        l1= load_group.create_task(load('P2_L1', 2))
        l2= load_group.create_task(load('P2_L2', 2))

    print("Pipeline 2 End")



async def execute_pipelines():
    print("Executing Pipelines")
    start = timer()

    async with asyncio.TaskGroup() as tg:
        tg.create_task(pipeline1())
        tg.create_task(pipeline2())
    
    end = timer()
    print(f"Finished Executing Pipelines:{end-start} ")


asyncio.run(execute_pipelines())