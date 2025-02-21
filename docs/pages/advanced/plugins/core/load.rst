.. _core_load_plugins:

Load Core Plugins
========================
Load plugins are responsible for loading data into the target destination. 
The plugins in this phase are designed to handle various data loading scenarios and can be easily integrated into your pipeline.


.. warning::
    Please refer to the :ref:`best practices <plugins_best_practices>` section to learn more about 
    how to use secrets in your pipeline configuration.


Below is a list of core load plugins, their functionality, and configurable arguments.


Database Connectors
------------------------------------
|br|

.. _sqlalchemy_query_loader:

This plugin loads data into a database using SQLAlchemy, from a pandas DataFrame.
It splits the data into batches and loads them into the target database table.

It uses the `SQLAlchemy <https://www.sqlalchemy.org/>`_ library to interact with the database, and 
`pandas <https://pandas.pydata.org/>`_ to handle the data.

It uses the asynchronous capabilities of SQLAlchemy to load data concurrently, which can significantly 
improve the performance of the load process.

**Arguments:**

.. list-table::
   :widths: 22 15 55 16  
   :header-rows: 1  

   * - **Argument**
     - **Data Type**
     - **Description**  
     - **Required** 
   * - `id`
     - str
     - Unique identifier for the plugin. It is used for logging and debugging purposes. If not provided, a random ID will be generated.
     - Optional
   * - `db_user`
     - str
     - Database user name.
     - Required 
   * - `db_password`  
     - str
     - Database user password. Should be fetched from a secret store using the :ref:`secrets <secrets>` plugin.
     - Required
   * - `db_host`
     - str
     - Database host address.
     - Required
   * - `db_name`
     - str
     - Database name.
     - Required
   * - `query`
     - str
     - SQL query to execute. It should be a valid SQL query that can be executed by the database of your choice.
     - Required
   * - `concurrency_limit`
     - str
     - Maximum number of concurrent connections to the database. Default is 5.
     - Optional
   * - `batch_size`
     - int
     - Number of rows to load in each batch. Default is 100000.
     - Optional
   * - `driver`
     - str
     - Asyncio database driver to use. Please refer to the `SQLAlchemy documentation <https://docs.sqlalchemy.org/en/20/dialects/mysql.html>`_ 
       for a list of supported drivers by SQLAlchemy. Default is ``mysql+asyncmy``.
     - Optional


**Example Configuration:**  

.. code-block:: yaml

    load:
      steps:
        - plugin: sqlalchemy_query_loader
          args:
            db_user: myuser # or even better use secrets!!
            db_password: mypassword # or even better use secrets!!
            db_host: localhost 
            db_port: 3306
            db_name: mydatabase
            query: SELECT 1



.. |br| raw:: html

      <br>