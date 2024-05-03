import asyncio


async def producer(queue: asyncio.Queue, name: int):
    await queue.put(item)
    print(f"Producer {name} produced {item}") 
    #await asyncio.sleep(1) # simulate production time

async def consumer(queue: asyncio.Queue, name: int, semaphore: asyncio.Semaphore):
    async with semaphore:
        while not queue.empty():
            item = await queue.get()
            print(queue, item)
            print(f"Consumer {name} consumed {item}")
            await asyncio.sleep(1)  # simulate consumption time
            queue.task_done()
    

async def pipeline_queue():
    pipeline_queue = asyncio.Queue()
    semaphore = asyncio.Semaphore(2) # Allows only 2 consumers to run at the same 

    # Creating a task group for producers
    async with asyncio.TaskGroup() as producer_group:
        for i in range(1): # Two producers
            producer_group.create_task(producer(pipeline_queue, i))
        
    # Creating a task group for consumers
    async with asyncio.TaskGroup() as consumer_group:
        for i in range(3): # Three Consumers
            consumer_group.create_task(consumer(pipeline_queue, i, semaphore))

    # Wait until the queue is fully processed
    print('before')
    await pipeline_queue.join()
    print('after')

async def main():
    await pipeline_queue()


asyncio.run(main())
