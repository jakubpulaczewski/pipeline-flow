# # SuperFastPython.com
# # example of running a blocking function call in asyncio
# import time
# import asyncio
 
# # blocking function
# def blocking_task():
#     # report a message
#     print('task is running')
#     # block
#     time.sleep(2)
#     # report a message
#     print('task is done')
 
# # background coroutine task
# async def background():
#     # loop forever
#     while True:
#         # report a message
#         print('>background task running')
#         # sleep for a moment
#         await asyncio.sleep(0.5)
 
# # main coroutine
# async def main():
#     # run the background task
#     _= asyncio.create_task(background())
#     # execute the blocking call
#     blocking_task()
 
# # start the asyncio program
# asyncio.run(main())

# SuperFastPython.com
# example of running a blocking function call in asyncio in a new thread
import time
import asyncio
 
# blocking function
def blocking_task():
    # report a message
    print('task is running')
    # block
    time.sleep(2)
    # report a message
    print('task is done')
 
# background coroutine task
async def background():
    # loop forever
    while True:
        # report a message
        print('>background task running')
        # sleep for a moment
        await asyncio.sleep(0.5)
 
# main coroutine
async def main():
    # run the background task

    await asyncio.gather(
        *
    )
    _= asyncio.create_task(background())
    # create a coroutine for the blocking function call
    # execute the call in a new thread and await the result
    await asyncio.to_thread(blocking_task)
 
# start the asyncio program
asyncio.run(main())