.. _core_concepts:

Core Concepts
==============
This section introduces the fundamental concepts behind ``pipeline-orchestrator``, helping
you understand its core principles and how it functions.

Data Pipelines
----------------------
``pipeline-orchestrator`` supports all three types of data pipelines: ETL, ELT, and ETLT. Each pipeline 
type has a unique structure and workflow, as described below:

.. list-table:: Data Pipeline Types
   :widths: 25 50
   :header-rows: 1

   * - Pipeline Type
     - Pipeline Definition
   * - **ETL** (Extract, Transform, Load)
     - A traditional data processing pipeline that extracts data from source systems, transforms it into a usable format, and loads it into a target system like a data warehouse.
   * - **ELT** (Extract, Load, Transform)
     - ELT differs from ETL by loading raw data directly into the target system before transformation, leveraging the power of modern databases for processing.
   * - **ETLT** (Extract, Load, Transform, Load)
     - ETLT extends traditional ETL pipelines by introducing an additional transformation step after loading to enable further refnements and data enrichment.i
     

Asynchronous Workflows Basics
-----------------------------
Unlike traditional sequential workflows, ``pipeline-orchestrator`` utilizes asynchronous pipeline execution to
maximize efficiency and reduce processing time. This approach allows multiple pipelines to run concurrently, and 
can be particularly useful for I/O-bound tasks.

**Key Benefits of Asynchronous Workflows:**

- Faster Processing: Execute tasks in parallel to reduce overall processing time when working with I/O bound tasks.
- Optimised Resource Utilization: Utilize system resources more efficiently by running multiple tasks concurrently.
- Efficiently Manage Long-Running Tasks: Handle long-running tasks without blocking the entire workflow.

Plugins and Dynamic Behavior
----------------------------
``pipeline-orchestrator`` supports a :ref:`dynamic plugin system <plugins>` that allows users and the community to extend functionality
without modifying the core codebase. Plugins can be added or updated on the fly, enabling rapid development and deployment.


**Plugins enable:**

- Custom Data Sources and Sinks: Connect to various data sources like databases, APIs, and cloud storage.
- Custom Transformations: Implement custom data transformations and processing logic.

Orchestration Pipeline
----------------------
Orchestration refers to the automated arrangement, coordination, and management of pipelines in a workflow. 
``pipeline-orchestrator`` uses pipeline configuration file to define the sequence of steps and dependencies


Example YAML configuration for using a plugin:


.. code:: yaml

    plugins:
        custom_plugin:
            ... # Define your plugin configuration here
    pipelines:
      pipeline1:
        type: ETL
        phases:
          extract:
            steps:
              - id: fetch data from api.example.com
                plugin: api_connector_example
                params:
                  endpoint: "https://api.example.com/data"
          transform:
            steps:
              - name: process_data
                plugin: transform_example_plugin
                params:
                    ... # Define your transformation parameters
          load:
            steps:
              - id: load data to database 
                plugin: mysql_connector
                params:
                  table: target_table
                  user: user
                  password: password

For a full list of available plugins, see: :ref:`Plugins <core_plugins>`.


