.. pipeline-flow documentation master file, created by
   sphinx-quickstart on Mon Jan 27 18:21:23 2025.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to pipeline-flow!
================================================
With ``pipeline-flow``, you can focus on building your data workflows without worrying about the underlying infrastructure.
It is a lightweight, scalable and extensible platform designed for managing ELT, ETL and ETLT data pipelines. Being platform agnostic,
it can run anywhere with a Python environment, offering a simple and flexible solution for managing data workflows.

Ideal for small to medium-size data workflows that don't require the overhead of a full-scale orchestration tool, ``pipeline-flow`` is also 
capable for handling more complex workflows. Its plugin-based architecture allows seamless customisation, enabling you to extend its functionality
as needed. Whether you're using Spark, Polaris or any other data processing engine, you can easily integrate it with ``pipeline-flow``.


This documentation will guide you through the process of setting up and using ``pipeline-flow``. 
If you have any questions or need help, please don't hesitate to reach out to us.

Why
---
Managing many complex data workflows is a common challenge in data engineering. Although, traditional tools like **Apache Airflow** and **Luigi**
are powerful, they require complex setups, heavyweight infrastructure, and extensive maintenance with a steep learning curve. 

What if you could have a simple, but yet flexible solution? That's where ``pipeline-flow`` comes in.
It follows a simple core philosophy: define and run data pipelines with minimal overhead in a simple YAML, 
using a asynchronous and plugin-based approach. Enabling you to focus on building your data workflows, 
with minimal effort and maximum flexibility.

As the community grows, so does the number of plugins available, making it easy to use existing solutions or build your own!!

Key Features
-------------
- **Easy Configuration**: Define pipelines with a simple YAML file (or in Python Code if you prefer).
- **Asynchronous Execution**: Execute multiple tasks concurrently to reduce processing time by switching between tasks blocked by I/O.
- **Dynamic Plugin System**: Add or update functionality on the fly without disrupting workflows.
- **Resilient and Fault-Tolerant**: Automatically retries failed tasks and ensures data integrity. (Still in development)
- **Scalable Design**: Can handle data workflows of any size, from small batch jobs to large-scale pipelines.
- **Pipeline Dependency**: Manage pipeline dependencies and their execution order.


Use Cases
------------
``pipeline-flow`` is ideal for:

- **Data Pipelines**: Extract, transform, and load data into warehouses or lakes.
- **Workflow Automation**: Automating data transformation and loading processes.
- **Data Lineage**: Tracking data lineage across pipelines.

How It Works
------------
At a high level, ``pipeline-flow`` works as follows:

1. **Define Pipelines**: Specify the steps (e.g., extract, transform, load) using :ref:`plugins <plugins>`.
2. **Configure Settings**: Customize behaviour with a YAML configuration file.
3. **Run Asynchronously**: Execute tasks in parallel to maximize efficiency.
4. **Monitor and Adjust**: Track progress, debug errors, and make runtime adjustments.


Getting Started
---------------
Ready to dive in? Check out the :ref:`Quick Start <quick_start>` guide to set up your first pipeline in just a few minutes!


Contents
----------
.. toctree::
   :maxdepth: 1
   :caption: Introduction

   pages/intro/introduction.rst
   pages/intro/quick_start.rst
   pages/intro/core_concepts.rst


.. toctree::
   :maxdepth: 2
   :caption: Userguide

   pages/user_guide/plugins/index.rst
   pages/user_guide/configuration/index.rst

   
.. toctree::
   :maxdepth: 2
   :caption: Developer Guide 

   pages/developer_guide/architecture.rst

.. toctree::
   :maxdepth: 2
   :caption: Examples

   pages/examples.rst

.. toctree::
  :maxdepth: 1
  :caption: Changelog

  pages/changelog.rst