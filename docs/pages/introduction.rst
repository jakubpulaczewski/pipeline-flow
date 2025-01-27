Introduction
============

Welcome to the documentation for ``pipeline-orchestrator``, a tool for managing ETL/ELT/ETLT pipelines with asynchronous
execution and dynamic plugin support. Whether you're processing big data, building real-time analytics, or designing scalable workflows.

This documentation will guide you through the process of setting up and using ``pipeline-orchestrator``. 
If you have any questions or need help, please don't hesitate to reach out to us.

Why
---
One of the most common challenges in data engineering is managing complex and numerous data workflows. 
Traditional tools like Apache Airflow or Luigi are powerful but can be cumbersome to set up and maintain. 

``pipeline-orchestrator`` aims to provide a lightweight, flexible, and scalable alternative for orchestrating data pipelines.

Key Features:
~~~~~~~~~~~~~
Key features of ``pipeline-orchestrator`` include:

- **Easy Configuration**: Define pipelines with a simple YAML file.
- **Asynchronous Execution**: Execute multiple tasks concurrently to reduce processing time.
- **Dynamic Plugin System**: Add or update functionality on the fly without disrupting workflows.
- **Resilient and Fault-Tolerant**: Automatically retries failed tasks and ensures data integrity. (Still in development)
- **Scalable Design**: Handle data workflows of any size, from small batch jobs to large-scale pipelines.
- **Pipeline Dependency**: Manage pipeline dependencies and their execution order.


Use Cases
~~~~~~~~~
``pipeline-orchestrator`` is ideal for:

- **ETL/ETL/ETLT Pipelines**: Extracting, transforming, and loading data into warehouses or lakes.
- **Real-Time Analytics**: Processing streaming data for dashboards or reports.
- **IoT Data Processing**: Managing asynchronous sensor data ingestion and transformation.
- **Data Science Pipelines**: Automating data preprocessing, feature engineering, and model training workflows.

How It Works
------------
At a high level, ``pipeline-orchestrator`` works as follows:

1. **Define Pipelines**: Specify the steps (e.g., extract, transform, load) using plugins.
2. **Configure Settings**: Customize the orchestrator's behavior with a simple YAML file.
3. **Run Asynchronously**: Execute tasks in parallel to maximize efficiency.
4. **Monitor and Adjust**: Track progress, debug errors, and make runtime adjustments.

Comparison with Traditional Tools
---------------------------------
Unlike traditional tools like Apache Airflow or Luigi, ``pipeline-orchestrator`` focuses on:

- Asynchronous task execution for faster processing.
- A lightweight and modular plugin system.
- Simplicity in configuration and deployment.

Getting Started
---------------
Ready to dive in? Check out the :ref:`Quick Start <quick_start>` guide to set up My Orchestrator in just a few minutes!