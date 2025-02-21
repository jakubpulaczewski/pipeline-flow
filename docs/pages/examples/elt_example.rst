.. _elt_example:

ELT Pipeline
==============
Below is an example of an ELT pipeline with example plugins for illustration purposes.

.. code:: yaml

    pipelines:
      test_elt_pipeline:
        type: ELT
        phases:
          extract:
            steps:
              - plugin: EXTRACT_PLUGIN
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
