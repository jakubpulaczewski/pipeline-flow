.. _concurrency:


Concurrency Basics
==============================
In a :ref:`data pipeline <data_pipeline>`, tasks can be broadly classified into two categories: I/O-bound and CPU-bound.

- **I/O-bound tasks**: Tasks that spend most of their time waiting for I/O operations to complete (e.g., reading from a file, making API requests).
- **CPU-bound tasks**: Tasks that spend most of their time performing computations (e.g., data transformation).

I/O-bound tasks are typically slower than CPU-bound tasks because they involve reading or writing data from external sources. 
During this time, the CPU is idle, waiting for the I/O operation to complete. This is often referred to as blocking I/O.

A common approach to improve the performance of I/O-bound tasks is to use threads or asynchronous programming.

Threading in Python
---------------------
The CPython implementation of Python uses a Global Interpreter Lock (GIL) that allows only one thread to control
the Python interpreter at any given time. While it's technically possible to counter this limitation by running multiple processes,
each with its own Python interpreter on a separate CPU core, this approach is generally not recommended due to 
the large overhead of managing multiple processes. It's also not a feasible solution for I/O-bound tasks, but rather for CPU-bound tasks.

Threading in python can be visualized as follows:

.. figure:: ../../_static/threading_gil.png
   :align: center
   :alt: Threading and GIL illustration

   A simplified illustration of threading and GIL in Python from `Reference <https://velog.io/@yg910524/TIL-46.-GILGlobal-Interpreter-Lock>`_.


Async Programming
-------------------
Asynchronous programming focuses on the concept of using a single thread to manage multiple tasks concurrently. It uses an event loop that 
is responsible for scheduling and executing tasks in a non-blocking manner.

**But how does it work, you might ask?** It uses the concept of coroutines, which are functions that can be paused and resumed at specific points. 
The event loop manages the execution of these coroutines. They are executed until they reach an await statement, at which point they are paused,
and the event loop moves on to the next task.

This approach is particularly useful for I/O bound tasks since it allows the system to be lightweight and efficient, 
while still being able to handle multiple tasks concurrently.


In Python, the `asyncio <https://docs.python.org/3/library/asyncio.html>`_ library provides support for writing concurrent 
execution of coroutines using the async/await syntax.

**Some benefits include:**

- **Faster Execution**: Multiple I/O operations can be executed concurrently, reducing the overall processing time. 
- **Efficient Resource**: Utilization: Asynchronous execution allows the system to utilize system resources more efficiently.
- **Non-blocking Execution**: Long-running tasks do not block the entire workflow, enabling other tasks to run concurrently.


Why Choose Asynchronous Programming Over Threads?
----------------------------------------------------
Sure, threads can boost the performance of I/O bound tasks but they come with a larger overhead compared to using coroutines.
Every thread has its own stack and registers, which the OS needs to manage. When the CPU switches between threads, it has to 
save and restore the state of each one, causing a performance hit. These context switches, though invisible to the user,
can be suprisingly time-consuming.

Now in contrast, asychronous programming can be more efficient because instead of juggling multiple threads, async code
runs on a single thread while still handling multiple tasks concurrently. No heavy overhead. No constant context switching.
Just smooth execution.

And here's the kicker: **no race conditions or deadlocks**. With only one thread running, you don't need to lose sleep
over hard-to-debug concurrency issues that can plague multithreaded applications. Async code lets you focus on building
features, not untangling-related headeaches.

Concurrency in ``pipeline-flow``
--------------------------------
``pipeline-flow`` uses async programming to execute tasks concurrently, ensuring maximum efficiency and better
resource utilisation. By handling I/O-bound and CPU-bound operations differently, it strikes the right balance
between performance and reliability.

|:gear:| **How it works**

- Extract and Load Phases (I/O-bound): Executed asynchronously to reduce the overall processing time, allowing multiple I/O operations to run concurrently.
- Transform and Transform-Load Phases (CPU-bound): TThese computationally intensive phases are executed sychronously in a separate thread to ensure data
  integrity and consistency. It prevents blocking the existing asyncio event loop, where other coroutines may be running.

.. warning:: **Important to Note**
    While ``pipeline-flow`` uses async programming for concurrency, it relies on plugins to support async operations natively.

    |:mag_right:| Why this Matters? 

    - Some popular libraries, like ``pandas`` or ``pyspark``, do not support async operations out of the box.
    - Using such engines will block the asyncio event loop, causing the entire system to run sychronously and defeat the purpose of async programming.

    |:white_check_mark:| Recommendation

    - To avoid blocking the event loop when running multiple tasks concurrently.
    - Check the :ref:`plugin documentation <sync_to_async_plugins>` for guidance on converting sychronous plugins to async plugins.


Next Steps
-----------------
- Explore the :ref:`Plugin Basics <plugin_core_concepts>` guide to learn more about plugins.
- Check out the :ref:`Pipeline Orchestration <core_concepts_pipeline_orchestration>` guide to learn how to manage pipeline dependencies and their execution order.