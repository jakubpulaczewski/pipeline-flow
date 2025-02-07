import asyncio
import functools as fn
import time
from timeit import default_timer as timer


def lower(x):
    return x.lower()


def capitalize(x):
    return x.capitalize()


async def extract(name: str, return_value, wait: int):
    print(f"Extract {name} - START ")
    await asyncio.sleep(wait)
    print(f"Extract {name} - END ")
    return return_value


async def extract_consumer(extract_queue, e_results):
    while not extract_queue.empty():
        ext_item = await extract_queue.get()
        result = await ext_item

        e_results.append(result)
        extract_queue.task_done()


def transform(name: str, func, value, wait: int):
    print(f"TRANSFORM {name} - START ")
    time.sleep(wait)
    print(f"Transform {name} - END ")
    return func(value)


async def load(name: str, wait):
    print(f"LOAD {name} - START ")
    await asyncio.sleep(wait)
    print(f"LOAD {name} - END ")


async def load_consumer(load_queue):
    while not load_queue.empty():
        ext_item = await load_queue.get()
        result = await ext_item

        load_queue.task_done()


async def pipeline(number: str):
    extract_queue = asyncio.Queue()
    load_queue = asyncio.Queue()

    # Consumer for Extract
    for val in range(1, 3):
        await extract_queue.put(extract(f"{number}_EXTRACT_{val}", chr(64 + int(val)), 2))  # 65 is A in ASCII

    # Processor
    e_results = []
    async with asyncio.TaskGroup() as extract_group:
        for _ in range(2):  # Two producers
            extract_group.create_task(extract_consumer(extract_queue, e_results))

    result = fn.reduce(lambda a, b: a + b, e_results)

    for tf in [lower, capitalize]:
        # result = transform(tf, tf, result, 5) # 24 sec with this
        result = await asyncio.to_thread(transform, number + str(tf), tf, result, 2)  # only 9 sec with this.

    # Consumer for Load
    for val in range(1, 3):
        await load_queue.put(load(f"{number}_L1", 2))

    # Processor
    async with asyncio.TaskGroup() as load_group:
        for _ in range(2):  # Two producers
            load_group.create_task(load_consumer(load_queue))

    print(f"Pipeline {number} End")


async def pipeline_processor(queue: asyncio.Queue, name: str):
    async with asyncio.Semaphore(2):
        while not queue.empty():
            item = await queue.get()
            print(name, item)
            await item
            queue.task_done()


async def execute_pipelines():
    pipeline_queue = asyncio.Queue()
    # semaphore = asyncio.Semaphore(2) # LImit to 2 concurrent pipelines

    for i in range(1, 3):  # 5
        name = "P" + str(i)
        await pipeline_queue.put(pipeline(name))

    print("Executing Pipelines")
    start = timer()

    async with asyncio.TaskGroup() as consumer_group:
        for i in range(2):
            asyncio.create_task(pipeline_processor(pipeline_queue, f"Consumer {i}"))

    await pipeline_queue.join()

    end = timer()
    print(f"Finished Executing Pipelines:{end - start} ")


asyncio.run(execute_pipelines())
