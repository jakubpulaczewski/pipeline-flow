``pipeline-flow`` provides a lightweight and extensible solution for managing ELT/ETL/ETLT data pipelines.

## Why

Managing complex data workflows is a common challenge in data engineering. Traditional tools like **Apache Airflow** and **Luigi** 
are powerful but require complex setups, heavyweight infrastructure, and extensive maintenance.

``pipeline-flow`` provides a lightweight, flexible, and scalable alternative for orchestrating data pipelines 
with minimal overhead by using YAML configuration files.


## Key Features
- **Easy Configuration**: Define data pipelines with a simple YAML file.
- **Asynchronous Execution**: Execute multiple tasks concurrently to reduce processing time.
- **Dynamic Plugin System**: Add or update functionality on the fly without disrupting workflows.
- **Scalable Design**: Handle data workflows of any size, from small batch jobs to large-scale pipelines.
- **Pipeline Dependency**: Manage pipeline dependencies and their execution order.
- **Resilient and Fault-Tolerant**: Automatically retries failed tasks and ensures data integrity. (Still in development)

## Use Cases
``pipeline-flow`` is ideal for:

- **Data Pipelines**: Extract, transform, and load data into warehouses or lakes.
- **ETL/ELT/ETLT Workflows**: Automating data transformation and loading processes.
- **Workflow Automation**: Orchestrating complex workflows with multiple dependencies.

## How It Works
