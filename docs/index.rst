.. pipeline-flow documentation master file, created by
   sphinx-quickstart on Mon Jan 27 18:21:23 2025.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to pipeline-flow!
================================================
``pipeline-flow`` allows you to concetrate on the logic and structure of your :ref:`data pipelines <data_pipeline>`, abstracting away the complexities of
data orchestration and management.

It is a lightweight, scalable and extensible platform designed for managing ELT, ETL and ETLT data pipelines. Being platform agnostic,
it can run anywhere with a Python environment, offering a simple and yet flexible solution for managing data workflows.

Ideal for small to medium-size data workflows without the overhead of full-scale orchestration tools, ``pipeline-flow`` makes
building data pipelines smple and accessible. With its YAML-based configuration and plugin-based architecture, you can easily define and run
data pipelines with minimal effort and maximum flexibility, as well as extend its functionality as needed. Whether you're using Spark, Polaris
or any other data processing engine, you can easily integrate it with ``pipeline-flow``.


This documentation will guide you through the process of setting up and using ``pipeline-flow``. 
If you have any questions or need help, please don't hesitate to reach out to us.

Why
---
Managing many complex data workflows is a common challenge in data engineering. Although, traditional tools like Apache Luigi and Airflow are powerful,
they often require complex setups, heavyweight infrastructure, and extensive maintenance paired with a steep learning curve.

What if you could have a simple, but yet flexible solution? That's where ``pipeline-flow`` comes in.
It follows a straightforward philisophy: define your pipelines and trigger them with minimal overhead using a simple YAML config!! Simple as that.

As the community grows, so does the number of plugins available, making it easy to use existing solutions or build your own!!

Key Features
-------------
- **Easy Configuration**: Define pipelines with a simple YAML file (or in Python Code if you prefer).
- **Asynchronous Execution**: Execute multiple tasks concurrently to reduce processing time by switching between tasks blocked by I/O.
- **Dynamic Plugin System**: Extend the functionality by creating custom plugins or using existing ones.
- **Resilient and Fault-Tolerant**: Automatically retries failed tasks and ensures data integrity. (Still in development)
- **Scalable Design**: Can handle data workflows of any size, from small batch jobs to large-scale pipelines as long the right data processing engine is used.
- **Pipeline Dependency**: Manage pipeline dependencies and their execution order.


Use Cases
------------
``pipeline-flow`` is ideal for:

- **Data Pipelines**: Extract, transform, and load data into warehouses or lakes.
- **Workflow Automation**: Automating data transformation and loading processes.
- **Data Lineage**: Tracking data lineage across different phases of the pipeline, as well as the interactions between different pipelines.

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

   pages/intro/quick_start.rst
   pages/intro/core_concepts.rst


.. toctree::
   :maxdepth: 2
   :caption: Advanced

   pages/advanced/plugins/index.rst
   pages/advanced/configuration/index.rst

   
.. toctree::
   :maxdepth: 2
   :caption: Architecture

   pages/advanced/architecture/system_overview.rst


.. toctree::
   :maxdepth: 2
   :caption: Examples

   pages/examples/elt_example.rst
   pages/examples/etl_example.rst
   pages/examples/etlt_example.rst


.. toctree::
  :maxdepth: 1
  :caption: Changelog

  pages/changelog.rst


.. toctree::
  :maxdepth: 1
  :caption: Roadmap

  pages/roadmap.rst
