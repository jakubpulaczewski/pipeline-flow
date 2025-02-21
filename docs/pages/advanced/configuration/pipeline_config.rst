.. _pipeline_config:

Pipeline Configuration 
=================================
This section provides information on various configuration settings that allow you to customise and optimise
your pipelines.

Currently, there is one available configuration setting, but more options such as retry policies
and advanced execution settings will be added in future releases.

.. _pipeline_concurrency:

Concurrency Configuration
-------------------------
The concurrency configuration allows you to specify the number of pipelines that can run concurrently in the system.
This setting helps balance performance and resource usage, ensuring that pipeline execution does not overload the system.

**Why is this important?**

- If your pipelines are lightweight and mostly I/O bound, you can increase the concurrency to improve performance.
- If your pipelines are heavy and CPU bound, you might want to decrease the concurrency to avoid overloading the system.


The default value for concurrency is set to 2, which means that only two pipelines can run concurrently.


.. code:: yaml

    concurrency: 2 # Number of pipelines that can run concurrently

    pipelines:
        ... # Your pipeline configuration here
