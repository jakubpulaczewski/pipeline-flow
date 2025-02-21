.. _etl_example:

ETL Pipeline
==============
Below is an example of an ETL pipeline with example plugins for illustration purposes.

.. code:: yaml

    pipelines:
      test_pipeline:
        type: ETL
        phases:
          extract:
            steps:
              - plugin: EXTRACT_PLUGIN
                args:
                  ARG1: VALUE1
                  ARG2: VALUE2
          transform:
            steps:
              - plugin: TRANSFORM_PLUGIN
                args:
                  ARG1: VALUE1
                  ARG2: VALUE2
          load:
            steps:
              - plugin: LOAD_PLUGIN
                args:
                  ARG1: VALUE1
                  ARG2: VALUE2
