.. _yaml_parsing_variables:

YAML Parsing & Variable Management
==================================
YAML have its advantanges and disadvantages. It is human readable and easy to write, but it can get messy as you scale 
and add more configurations. ``pipeline-flow`` provides a way to manage variables, environment variables and secrets in your YAML configurations.

.. note::
    When using variables and secrets, make sure that you defined them in a separate yaml document, and the rest
    of your yaml configurations in another document. This is required for the parser to first parse the variables and secrets,
    and then substitute them in the main yaml configuration.

    In a YAML file, a document starts with three dashes (``---``) and ends with three dots (``...``).


Variables 
---------
Variables give you a covenient way to store and reuse value across your YAML configurations.


They help you:

- Avoid repeating the same values in multiple places.
- Easily manage configuration changes in one central location.
- Keep your pipeline YAMLs clean and maintainable.

You define variables in the ``variables`` section of your YAML configuration file, and then reference them elsewhere
using the ``${{ variables.YOUR_VARIABLE_NAME }}`` syntax.

.. warning::
    Variables can be used to store different type of values like strings, integers, boolean, lists and dicts. 
    
    However, if you define a single variable in inline, it will preserve its type. However, if there are multiple variables, or
    if it is used with other strings, it will be converted to a string.

**Why use variables?**

Without variables:

.. code:: yaml

    file_path: "/data/input.csv"
    database_name: "my_database"
    database_port: 5432

With variables:

.. code:: yaml

    file_path: ${{ variables.FILE_PATH }}
    database_name: ${{ variables.DB_NAME }}
    database_port: ${{ variables.DB_PORT }}


**Example Configuration:**

Here is a simple pipeline configuration that uses variables to extract data from a CSV file and load it into a database.
Please note that these plugins are for illustrative purposes only and may not be available in the core plugins.


.. code:: yaml

   
    variables:
      DB_CONNECTION_STRING: "postgresql://user:password@localhost:5432/mydb"
      DB_TABLE: "users"

    ---  # New YAML document for the pipeline configuration

    pipelines:
      test_pipeline:
        type: ELT
        description: A simple ELT pipeline example
        phases:
          extract: ... # No extract step in this example
          transform:: ... # No transformation step in this example
          load:
            steps:
              - id: load data to database
                plugin: db_loader
                args:
                  connection_string: ${{ variables.DB_CONNECTION_STRING }}
                  table: ${{ variables.DB_TABLE }}
                  if_exists: "replace"


|br|
**What's happening in the example?**

We have a single YAML file with two documents. The first document defines the variables, and the second document
uses the loaded variables from memory to configure the pipeline, as shown in the example above.

1. Variable section
    - Store values like `FILE_PATH`, `DB_CONNECTION_STRING`, etc., in the variables section.
    - Why? If you need to change the DB connection string, you can do it in one place, and it will be reflected across all the places where it is used.

2. Pipeline configuration section
    - Use the variables in the pipeline configuration section.
    - Why? It makes the configuration more readable and maintainable.


|br|

**Tips for Using Variables Effectively:**

- Use meaningful names for variables.
- You don't need to use variables, but only for values that are repeated in multiple places.
- Keep variables in a separate document from the rest of the configuration, otherwise the parser will not be able to substitute them.
- Use :ref:`secrets <secrets>` for retrieving sensitive information like passwords, API keys, etc.



Variables give you a covenient way to store and reuse value across your YAML configurations.


Environment Variables
---------------------
Environment variables provide a way to pass configuration values to your pipeline without hardcoding them in the YAML file. 
This is useful when you want to keep more sensitive information out of your configuration files, or when you want
to use environment-specific values.

They help you:

- Keep sensitive information out of your YAML files.
- Make your pipelines portable across different environments (dev, staging, production).
- Easily override values without modifying the YAML file.

You define environment variables in your system environment, and then reference them in your YAML configuration 
using the ``${{ env.ENV_VARIABLE_NAME }}`` syntax.

**Why use environment variables?**

Without environment variables:

.. code:: yaml

    connection_string: "postgresql://user:password@localhost:5432/mydb"  # Hardcoded password!

With environment variables:

.. code:: yaml

    connection_string: ${{ env.DB_CONNECTION_STRING }}  # The database connection will differ based on the environment.


**Example Configuration:**

Below, we show how to use environment variables in conjuction with variables in your pipeline configuration.

.. code:: yaml

    # Reference environment variables directly in your pipeline configuration
    variables:
      FILE_PATH: "/data/input.csv"
      DELIMITER: ","
      DB_TABLE: "users"

    ---  # New document for pipeline definition

    pipelines:
      test_pipeline:
        type: ELT
        description: A simple ELT pipeline example - extracts user dataf rom CSV and loads it into a database.
        phases:
          extract: ... # No extract step in this example
          transform:: ... # No transformation step in this example
          load:
            steps:
              - id: load data to database
                plugin: db_loader
                args:
                  connection_string: ${{ env.DB_CONNECTION_STRING }}  # Uses environment variable for the connection string!
                  table: ${{ variables.DB_TABLE }}
                  if_exists: "replace"


|br|

**What's happening in the example?**

1. Environment Variable Reference:

- The connection string uss ``${{ env.DB_CONNECTION_STRING }}`` to reference the connection string.
- Why? This keeps the database string out of the configuration file, making it more secure and environment-agnostic.

2. Variables and Environment Variables:

- We use variables for values that are repeated in multiple places.
- We use environment variables for sensitive information or environment-specific values.

3. Result

- Use one YAML file across different environments by setting the environment variables accordingly.
- Just set a different ``${{ env.DB_CONNECTION_STRING }}`` in each environment, and the pipeline will use the correct connection string.

Secrets
-------

.. _secrets:

Secrets provide a secure way to manage sensitive information like passwords, API keys, etc., in your pipeline configurations.

Instead of hardcoding sensitive information in your YAML files, you can use existing secret manager plugins, or implement your own secret management plugins to
dynamically fetch secrets from external services (e.g., AWS Secrets Manager, Azure Key Vault, etc.).

- Secrets are fetched only when needed (lazy loading via callback functions).
- No sensitive information is stored in the YAML file.
- Supports multiple secret backends through pluggable architecture.

**Why use secrets instead of variables of environment variables?**

- Variables are stored in the YAML file, which can be a security risk.
- Environment variables are stored in the system environment, and may still be exposed in logs or other places.
- Secrets are fetched only when needed, and are not stored in the YAML file or system environment.

**How Secret Manager Work**

They work, as follows:

- Implement a common interface, ensuring a consistent way for secret retrieval.
- Integrate with various secret management services (AWS Secrets Manager, Azure Key Vault, etc.).
- Return callbacks that fetch the secret only when needed before the task execution.

**Example Configuration:**

- Secret Managers are configured to fetch credentials from AWS Secrets Manager.
- Secrets are referenced in the pipeline using the ``${{ secrets.SECRET_NAME }}`` syntax.
- They have to be defined in a separate YAML document, and then referenced in the main configuration.


|br|

.. code:: yaml

    secrets:
      example_api_key:
        plugin: aws_secrets_manager
        args:
          secret_name: SUPER_SECRET_API_KEY
          region: us-east-1

    ---  # New document for pipeline definition
    pipelines:
      test_pipeline:
        type: ELT
        phases:
          extract:
            steps:
              - plugin: api_extractor
                args:
                  base_url: "https://api.example.com/v1/"
                  endpoint: "/users"
                  headers:
                    Authorization: "Bearer ${{ secrets.example_api_key }}"  # Securely fetched from AWS Secrets Manager
          transform:: ... # No transformation step in this example
          load: ... # No load step in this example

.. |br| raw:: html

      <br>