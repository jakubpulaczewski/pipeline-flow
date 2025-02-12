# Standard Imports
import sqlite3

# Third Party Imports
import pytest

# Project Imports
from pipeline_flow.entrypoint import start_workflow


@pytest.mark.asyncio
async def test_e2e_etl_pipeline(populate_source_e2e_tables, db_connection: sqlite3.Connection, db_uri: str) -> None:
    yaml_config = f"""
    plugins:
      custom:
        files:
          - tests/resources/e2e_plugins.py
    pipelines:
      customer_order_pipeline:
        type: ETL
        phases:
          extract:
            steps:
              - id: extracting data from source_customers table
                plugin: sqlite_extractor
                params:
                  db_uri: {db_uri}
                  table_name: source_customers
                  columns:
                    - customer_id
                    - name
                    - email
              - id: extract data from source_orders table
                plugin: sqlite_extractor
                params:
                  db_uri: {db_uri}
                  table_name: source_orders
                  columns:
                    - order_id
                    - customer_id
                    - amount
                    - order_date
            merge:
              id: merge the customer and order data
              plugin: sqlite_inner_merger
              params:
                on: customer_id
          transform:
            steps:
              - id: drop unneeded columns
                plugin: column_dropper
                params:
                  columns:
                    - email
                    - order_date
              - id: get the summary of customer orders
                plugin: customer_order_summarizer
          load:
            steps:
              - id: load_customer_orders
                plugin: sqlite_loader
                params:
                  db_uri: {db_uri}
                  table: customer_orders
    """

    result = await start_workflow(yaml_text=yaml_config)

    assert result is True
    sqlite3_cursor = db_connection.cursor()
    sqlite3_cursor.execute("SELECT * FROM customer_orders")
    results = sqlite3_cursor.fetchall()

    assert results == [(1, "John Doe", 2, 250), (2, "Jane Smith", 1, 200)]


@pytest.mark.asyncio
async def test_e2e_elt_pipeline(populate_source_e2e_tables, db_connection: sqlite3.Connection, db_uri: str) -> None:
    yaml_config = f"""
    plugins:
      custom:
        files:
          - tests/resources/e2e_plugins.py
    pipelines:
      customer_order_pipeline:
        type: ELT
        phases:
          extract:
            steps:
              - id: extracting data from source_customers table
                plugin: sqlite_extractor
                params:
                  db_uri: {db_uri}
                  table_name: source_customers
                  columns:
                    - customer_id
                    - name
                    - email
              - id: extract data from source_orders table
                plugin: sqlite_extractor
                params:
                  db_uri: {db_uri}
                  table_name: source_orders
                  columns:
                    - order_id
                    - customer_id
                    - amount
                    - order_date
            merge:
              id: merge the customer and order data
              plugin: sqlite_inner_merger
              params:
                on: customer_id
          load:
            steps:
              - id: load_customer_orders
                plugin: sqlite_loader
                params:
                  db_uri: {db_uri}
                  table: customer_orders
          transform_at_load:
            steps:
              - id: get the summary of customer orders
                plugin: sqlite_transform_loader
                params:
                  db_uri: {db_uri}
                  query: >
                      CREATE TABLE customer_orders_summary AS
                      SELECT
                        customer_id,
                        name,
                        COUNT(order_id) AS total_orders,
                        SUM(amount) AS total_amount
                      FROM customer_orders
                      GROUP BY customer_id, name;
    """

    result = await start_workflow(yaml_text=yaml_config)

    assert result is True
    sqlite3_cursor = db_connection.cursor()
    sqlite3_cursor.execute("SELECT * FROM customer_orders_summary")
    results = sqlite3_cursor.fetchall()

    assert results == [(1, "John Doe", 2, 250), (2, "Jane Smith", 1, 200)]


@pytest.mark.asyncio
async def test_e2e_etlt_pipeline(populate_source_e2e_tables, db_connection: sqlite3.Connection, db_uri: str) -> None:
    yaml_config = f"""
    plugins:
      custom:
        files:
          - tests/resources/e2e_plugins.py
    pipelines:
      customer_order_pipeline:
        type: ETLT
        phases:
          extract:
            steps:
              - id: extracting data from source_customers table
                plugin: sqlite_extractor
                params:
                  db_uri: {db_uri}
                  table_name: source_customers
                  columns:
                    - customer_id
                    - name
                    - email
              - id: extract data from source_orders table
                plugin: sqlite_extractor
                params:
                  db_uri: {db_uri}
                  table_name: source_orders
                  columns:
                    - order_id
                    - customer_id
                    - amount
                    - order_date
            merge:
              id: merge the customer and order data
              plugin: sqlite_inner_merger
              params:
                on: customer_id
          transform:
            steps:
              - id: drop unneeded columns
                plugin: column_dropper
                params:
                  columns:
                    - email
                    - order_date
              - id: get the summary of customer orders
                plugin: customer_order_summarizer
          load:
            steps:
              - id: load_customer_orders
                plugin: sqlite_loader
                params:
                  db_uri: {db_uri}
                  table: customer_orders
          transform_at_load:
            steps:
              - id: get the summary of customer orders
                plugin: sqlite_transform_loader
                params:
                  db_uri: {db_uri}
                  query: >
                    UPDATE customer_orders
                    SET name = UPPER(name);
    """

    result = await start_workflow(yaml_text=yaml_config)
    assert result is True
    sqlite3_cursor = db_connection.cursor()
    sqlite3_cursor.execute("SELECT * FROM customer_orders")
    results = sqlite3_cursor.fetchall()

    assert results == [(1, "JOHN DOE", 2, 250), (2, "JANE SMITH", 1, 200)]
