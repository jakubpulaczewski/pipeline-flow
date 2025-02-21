.. _core_concepts_pipeline_orchestration:

Pipeline Orchestration
================================
Orchestrating data pipelines can be complex but with ``pipeline-flow``, its made easy. By using a simple configuration file, 
you define the sequence of steps and their dependencies, allowing you to focus on building reliable, efficient workflows.


Pipeline phases: From Raw Data to Insights
---------------------------------------------
Every data pipeline is a journey, moving data through various stages of processing.
In ``pipeline-flow``, each phase represents a key step in this process, ensuring that data flows smoothly from source to destination.


A pipeline consists of multiple phases, each representing a stage in the data processing workflow.

The typical phases in a pipeline are:

- Extract: Retrieve data from various sources (e.g., databases, APIs, files).
- Transform: Process, clean, and enrich the extracted data.
- Load: Deliver the processed data to its target destination (e.g., data warehouses, files).
- Transform at Load: Perform additional transformations on the external system for further processing (Type 2 SCD, etc.).

Execution Flow
---------------------------------------------
``pipeline-flow`` ensures a balanced approach between speed and data integrity through well-defined execution rules.

**Phases execution in a defined sequence**

- Each phase runs in order, ensuring the output of one phase seamlessly feeds into the next.
- This preserves data integrity and ensures that all dependencies between phases are respected.

**Pipelines can run concurrently or sequnetially**

- Multiple pipelines can execute asynchronously for faster processing, or sequentially if there are dependencies between them that require ordered execution.

**Phases contain multiple steps**

- A single phase can include multiple steps, with each step representing a distinct task or operation in the workflow.

**Execution mode depends on the phase type**

- Asychronous execution is used where the speed is critical (e.g., Extract and Load phases).
- Sychronous execution ensures acccuracy and consistency (e.g., Transform and Transform at Load phases).


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
-----------------
- Explore the User Guide to learn more about the :ref:`Plugin Development <plugin_development>` process.