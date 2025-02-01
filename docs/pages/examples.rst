.. _examples:

Examples
===========
This section provides examples of common data pipeline use cases and how to implement them using ``pipeline-orchestrator``.




ETL Pipeline
-----------------
Below is an example of a simple ETL pipeline that:

1. Extracts data from a CSV file
2. Transforms it by filtering rows based on a condition.
3. Loads the result into another CSV file

.. code:: yaml

    plugins:
      custom:
        files:
         - path/to/file/with/custom/plugins.py

    pipelines:
      simple-etl-pipeline:
        type: ETL
        description: A basic ETL pipeline example
        phases:
          extract:
            steps:
              - id: extract data from csv file
                plugin: csv_extractor
                params:
                  file_path: data/raw_data.csv
          transform:
            steps:
              - id: transform data
                plugin: data_transformer
                params:
                  filter: "column1 > 10"
          load:
            steps:
              - id: load data to csv file
                plugin: csv_loader
                params:
                  file_path: data/final_output.csv


ELT Pipeline
-----------------



ETLT Pipeline
-----------------
