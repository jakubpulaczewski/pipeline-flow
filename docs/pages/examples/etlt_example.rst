.. _etlt_example:

ETLT Pipeline
===============
Below is an example of an ETLT pipeline with example plugins for illustration purposes.

.. code:: yaml

    pipelines:
      test_pipeline:
        type: ETLT
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
          transform_at_load:
            steps:
              - plugin: TRANSFORM_PLUGIN
                args:
                  ARG1: VALUE1
                  ARG2: VALUE2
