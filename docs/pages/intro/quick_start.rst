.. _quick_start:

Quick Start
===========
This guilde will walk you through setting up ``pipeline-flow``, running your first pipeline and understand the 
basic workflow in less than 5 minutes! 🚀

Prerequisites
-------------
Before installing ``pipeline-flow``, ensure you have the following dependencies installed:

- Python 3.12 or later installed on your machine.
- A Python Package Manager (e.g., pip or poetry).
- Basic Knowledge of YAML (used for pipeline configuration).
- Access to required data sources and data sinks (e.g., S3 Buckets, databases, APIs).

To check your Python version installed, run:

.. code:: bash

  python --version

Installation
------------
``pipeline-flow`` is available on `PyPI <https://pypi.org/project/pipeline-flow/>`_ and can be installed using pip or poetry. To install using pip, run:

.. code:: bash

  pip install pipeline-flow  # or better use poetry

Setup
---------------------------------------
After installation, add import two following dependencies to your Python script:

.. code:: python

  >>> import asyncio     # Required for running asynchronous coroutines                                 
  >>> from pipeline_flow.entrypoint import start_orchestration  # Required for running the pipeline


If you are new to asychronous programming, no need to worrry. Please 
refer to the :ref:`Asynchronous Workflows Basics <concurrency>` section for more information.


Next, you need to define your pipeline configuration, using a YAML file, and then you can run your first pipeline! 
Please refer to the configuration template below to help you get started.

Running Your First Pipeline
----------------------------
When you have your pipeline configuration ready, you can run your first pipeline using ``start_orchestration``
function already imported in the previous step.

To run the pipeline synchronously, you can use the ``asyncio.run`` function to run the ``start_orchestration`` function.

.. code:: python

  >>> import asyncio
  >>> result = asyncio.run(start_orchestration(local_file_path='YOUR_FILE_PATH_TO_PIPELINE.YAML'))

Alternatively, you could start the pipeline within an asynchronous function. To do this, you would need to await 
the ``start_orchestration`` function. 


.. code:: python

  >>> import asyncio
  
  >>> async def some_async_funciton(local_file_path: str):
  >>>    result = await start_orchestration(local_file_path)
  >>>    ... # Some other code here
  >>> 
  >>> asyncio.run(some_async_funciton(local_file_path='YOUR_FILE_PATH_TO_PIPELINE.YAML'))


Configuration Template
-----------------------
Setup a configuration file for your pipeline. Create a new YAML file (e.g., ``pipeline.yaml``) 
and define your pipeline steps in the following order:

#. Define your custom or community plugins in the ``plugins`` section.
#. Define your pipeline type (ETL, ELT or ETLT) in the ``pipelines`` section.
#. Define the extract phase in the ``extract`` section.
#. Define the transform phase in the ``transform`` section (if ETL or ETLT defined).
#. Define the load phase in the ``load`` section.
#. Define the transform at load phase in the ``transform_at_load`` section (içf ETLT defined).


YAML Configuration Example:


.. code:: yaml

    plugins:  # Step 1. Define your plugins here (custom or community)
      custom:
        dirs:
          - /path/to/custom/plugins  # Directory where the custom plugins are located 
                                     # (enables importing multiple plugins at once)
        files:
          - /path/to/custom/plugins/custom_plugin.py  # Or the file name where the custom plugin is defined
      community: # Or use community plugins (if available)
        - plugin_name1
        - plugin_name2
    
    pipelines:
      pipeline1:
        type: ... # Step 2. Define your pipeline type (ETL, ELT or ETLT)
        phases:
          extract:
            steps:
              - plugin:  # Step 3. Define your extract phase
          transform:
            steps:
              - plugin: # Step 4. Define your transform phase (if ETL or ETLT defined
          load:
            steps:
              - plugin: # Step 5. Define your load phase
          transform_at_load:
            steps:
              - plugin: # Step 6. Define your transform at load phase (if ETLT defined)


Next Steps
-------------
- Explore the full documentation to learn more about the pipeline configuration and advanced features.
- Check out the :ref:`Core Concepts <core_concepts>` to understand the core concepts behind ``pipeline-flow``.
- Learn more about :ref:`Building Custom Plugins <plugin_development>`.

Happy orchestrating! 🚀