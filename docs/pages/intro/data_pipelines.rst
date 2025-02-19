.. _data_pipeline:

Data Pipelines
================

What is a Data Pipeline?
~~~~~~~~~~~~~~~~~~~~~~~~~
Let's start with the basics - what exactly is a data pipeline? 

A data pipeline is a series of steps designed to process data from one or more raw sources into 
a usable format, typically for storage, analytics or machine learning.

Data pipelines play a crucial role in automating data processing. While some pipelines may be as simple
as transferring data from one system to another, they can quickly become complex - especially when 
other pipeline depend on the output of another pipeline. 

This is why when building data pipelines, there is much more to consider than simply moving data from point A to point B.

To build truly effective pipelines, you need to account for:

- **Scalability**: Can the pipeline handle increasing data loads?
- **Reliability**: Will the pipeline perform consistently under different conditions?
- **Observability**: Can you easily monitor, debug, and understand what's happening under the hood?
- **Maintainability**: How easy is it to update or modify the pipeline?
- **Security**: Does the pipeline adhere to data security and compliance standards?

That's where ``pipeline-flow`` steps in. It's designed to tackle these problems for you by providing a flexible, scalable
framework for moving and processing data across different systems with minimal overhead and maximum reliability.

Types of Data Pipelines
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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