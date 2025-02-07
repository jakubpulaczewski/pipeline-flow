.. _core_concepts_pipeline_orchestration:

Pipeline Orchestration
----------------------
Pipeline orchestration refers to how different phases of the pipeline are structured and executed. 
``pipeline-flow`` uses a pipeline configuration file to define the sequence of steps and dependencies in a pipeline.


Pipeline phases
~~~~~~~~~~~~~~~~
A pipeline consists of multiple phases, each representing a stage in the data processing workflow.

The typical phases in a pipeline are:
    - Extract: Fetch data from various sources (e.g., databases, APIs).
    - Transform: Process and clean the data.
    - Load: Write the processed data to a target destination (e.g., database, file).
    - Transform at Load: Perform additional transformations before loading the data.

Execution Rules
~~~~~~~~~~~~~~~~
The pipeline execution follows these rules:
    - Each phase is executed sequentially.
    - The output of one phase is passed as input to the next phase.
    - A phase can have multiple steps, each representing a specific task or operation.
    - Steps within a phase can be executed in parallel if they are can be run asynchronously.

.. list-table:: Data Execution Rules
   :widths: 25 25 25
   :header-rows: 1

   * - Pipeline Phase
     - Type
     - Execution Mode
   * - **Extract**
     - Async
     - Runs conncurrently. Plugins must be asynchronous.
   * - **Transform**
     - Sync
     - Runs sequentially. Plugins must be synchronous.
   * - **Load**
     - Async
     - Runs concurrently. Plugins must be asynchronous.
   * - **Transform at Load**
     - Sync
     - Runs sequentially. Plugins must be synchronous.


**Why are some phases asynchronous?**
  - Extract and Load involve I/O operations (database queries, API calls, etc.)
  - Using async execution prevents blocking the pipeline.

**Why are some phases synchronous?**
  - Each transformation depends on the previous step.
  - Running them out of order would lead to incorrect results.

Next Steps
~~~~~~~~~~~~~~~~
# TODO