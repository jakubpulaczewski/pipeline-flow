Data Pipelines
================
A data pipeline a series of steps used to process data from one or more raw sources into an usable
format typically for storage or analysis. Data pipelines are essential for automating data processing tasks,
enabling organisations to extract, transform, and load data efficiently.


``pipeline-flow`` supports all three types of data pipelines: ETL, ELT, and ETLT. Each pipeline 
type has a unique structure and workflow, as described below:

.. list-table:: Pipeline Types
   :widths: 15 75
   :header-rows: 1

   * - Pipeline Type
     - Pipeline Definition
   * - **ETL**
     - A traditional data processing pipeline that extracts data from source systems, transforms it into a usable format, and loads it into a target system like a data warehouse.
   * - **ELT**
     - ELT differs from ETL by loading raw data directly into the target system before transformation, leveraging the power of modern databases for processing.
   * - **ETLT**
     - ETLT extends traditional ETL pipelines by introducing an additional transformation step after loading to enable further refnements and data enrichment.

Next Steps
~~~~~~~~~~~~~~~~
- Check out the :ref:`Concurrency <concurrency>` guide to learn how ``pipeline-flow`` uses asynchronous programming to execute tasks concurrently.
- Explore the :ref:`Plugin Basics <plugin_core_concepts>` guide to learn more about plugins.
- Check out the :ref:`Pipeline Orchestration <core_concepts_pipeline_orchestration>` guide to learn how to manage pipeline dependencies and their execution order.