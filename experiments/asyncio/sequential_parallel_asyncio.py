# SuperFastPython.com
# example of running a blocking function call in asyncio in a new thread
import time
from timeit import default_timer as timer
import asyncio


# blocking function
def blocking_task(number: int):
    # report a message
    print(f"BLOCKING TASK  {number} is running")
    # block
    time.sleep(3)
    # report a message
    print("BLOCKING TASK {number} is done")
    return 100 + number


async def task_generator(number: int):
    print(f"task {number} is running")
    # loop forever
    await asyncio.sleep(2)
    print(f"task {number} is finished")
    return number


# main coroutine
async def main():
    # Will run this and after it's done it will return the control to main thread and execute background
    start = timer()
    result = 0
    for i in range(3):
        result += await task_generator(i)

    for i in range(2):
        blocking_task(i)
    end = timer()
    print(end - start)

    print("*" * 100)
    start = timer()
    results = await asyncio.gather(
        *[task_generator(number) for number in range(3)],
        *[asyncio.to_thread(blocking_task, number) for number in range(2)],
    )
    end = timer()
    print(end - start)
    print(results)


# start the asyncio program
asyncio.run(main())
