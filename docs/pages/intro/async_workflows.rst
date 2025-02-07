.. _async_workflows:

Asynchronous Workflows Basics
==============================
Many operations in data pipelines involve I/O-bound task, such as:

- Reading data from a database
- Writing data to a file
- Making API requests


These tasks can be time-consuming due to the latency involved in reading or writing data. 
During this time, the CPU is idle, waiting for the I/O operation to complete. 

In ``pipeline-flow``:

- The Extract and Load phases are asychronous (since they interact with external systems).
- The Transform and Transform-Load phases are synchronous (since they are CPU-bound).

Why Async
----------
Async is a programming paradigm that allows multiple task to run concurrently. ``pipeline-flow`` uses 
`asyncio <https://docs.python.org/3/library/asyncio.html>`_ to implement asynchronous workflows. 

It is a python library that provides support for writing concurrent execution of coroutines using the async/await syntax.
Coroutines are simply functions that can be paused and resumed during exception.

**Some benefits include:**

- Faster Execution: Multiple I/O operations can be executed concurrently, reducing the overall processing time. 
- Efficient Resource Utilization: Asynchronous execution allows the system to utilize system resources more efficiently.
- Non-blocking Execution: Long-running tasks do not block the entire workflow, enabling other tasks to run concurrently.


Next Steps
-----------
# TODO: