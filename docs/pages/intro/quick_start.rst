.. _quick_start:

Quick Start
===========
This guilde will walk you through setting up ``pipeline-orchestrator``, running your first pipeline and understand the 
basic workflow.

Prerequisites
-------------
Before installing ``pipeline-orchestrator``, ensure you have the following dependencies installed:

- Python 3.12 or later installed on your machine.
- A python package manager (e.g., pip or poetry).
- Basic Knowledge of YAML (used for pipeline configuration).
- Access to required data sources (e.g., S3 Buckets, databases, APIs).

To check your Python version, run:

.. code:: bash

  python --version

Installation
------------
``pipeline-orchestrator`` is available on PyPI and can be installed using pip or poetry. To install using pip, run:

.. code:: bash

  pip install pipeline-orchestrator  # or better use poetry

Setup
------------
After installation, add the following import statement to your Python script:

.. code:: python

  >>> from pipeline-orchestrator.entrypoint import start

You can either provide a yaml file path or a python string to the ``start`` function. 
The yaml file should contain the pipeline configuration. 

Configuration Template
-----------------------
Setup a configuration file for your pipeline. Create a new YAML file (e.g., ``pipeline.yaml``) 
and define your pipeline steps. 

.. code:: yaml

    plugins:
       ... # Step 1. Add your plugins here
    pipelines:
        pipeline1:
          type: ... # Step 2. Define your pipeline type (ETL, ELT or ETLT)
          phases:
            extract:
              steps:
                - ... # Step 3. Define your extract phase
            transform:
              steps:
                - ... # Step 4. Define your transform phase (if ETL or ETLT defined
            load:
              steps:
                - ... # Step 5. Define your load phase
            transform_at_load:
              steps:
                - ... # Step 6. Define your transform at load phase (if ETLT defined)


Next Steps
-------------
- Explore the full documentation to learn more about the pipeline configuration and advanced features.
- Learn more about :ref:`Building Custom Plugins <plugin_development>`.

Happy orchestrating! 🚀