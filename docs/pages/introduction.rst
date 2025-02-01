Introduction
============
Whether you're processing big data, building real-time analytics, or designing scalable workflows,
``pipeline-orchestrator`` provides a lightweight and extensible solution for managing ELT/ETL/ETLT data pipelines.

This documentation will guide you through the process of setting up and using ``pipeline-orchestrator``. 
If you have any questions or need help, please don't hesitate to reach out to us.

Why
---
Managing complex data workflows is a common challenge in data engineering. Traditional tools like **Apache Airflow** and **Luigi** 
are powerful but require complex setups, heavyweight infrastructure, and extensive maintenance.

``pipeline-orchestrator`` provides a lightweight, flexible, and scalable alternative for orchestrating data pipelines 
with minimal overhead.

Key Features
-------------
Key features of ``pipeline-orchestrator`` include:

- **Easy Configuration**: Define pipelines with a simple YAML file.
- **Asynchronous Execution**: Execute multiple tasks concurrently to reduce processing time.
- **Dynamic Plugin System**: Add or update functionality on the fly without disrupting workflows.
- **Resilient and Fault-Tolerant**: Automatically retries failed tasks and ensures data integrity. (Still in development)
- **Scalable Design**: Handle data workflows of any size, from small batch jobs to large-scale pipelines.
- **Pipeline Dependency**: Manage pipeline dependencies and their execution order.


Use Cases
------------
``pipeline-orchestrator`` is ideal for:

- **Data Pipelines**: Extract, transform, and load data into warehouses or lakes.
- **ETL/ELT/ETLT Workflows**: Automating data transformation and loading processes.
- **Workflow Automation**: Orchestrating complex workflows with multiple dependencies.

How It Works
------------
At a high level, ``pipeline-orchestrator`` works as follows:

1. **Define Pipelines**: Specify the steps (e.g., extract, transform, load) using :ref:`plugins <plugins>`.
2. **Configure Settings**: Customize behaviour with a YAML configuration file.
3. **Run Asynchronously**: Execute tasks in parallel to maximize efficiency.
4. **Monitor and Adjust**: Track progress, debug errors, and make runtime adjustments.

Comparison with Traditional Tools
---------------------------------
Unlike traditional tools like Apache Airflow or Luigi, ``pipeline-orchestrator`` focuses on:

- Asynchronous task execution for concurrent processing.
- A lightweight, modular plugin system for easy extensibility.
- Simpler configuratio with YAML, reducing setup overhead.

Getting Started
---------------
Ready to dive in? Check out the :ref:`Quick Start <quick_start>` guide to set up My Orchestrator in just a few minutes!