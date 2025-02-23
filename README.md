# pipeline-flow

``pipeline-flow`` is a lightweight, scalable and extensible platform designed for managing ELT, ETL and ETLT data pipelines.
Being platform agnostic, it can run anywhere with a Python environment, offering a simple and yet flexible
solution for managing data workflows.



Ideal for small to medium-size data workflows without the overhead of full-scale orchestration tools, 
``pipeline-flow`` makes building data pipelines simple and accessible.

With its YAML-based configuration and plugin-based architecture, you can easily define and run your data pipelines with minimal effort and maximum flexibility, being able to extend its functionality to various engines as needed. Whether you are using Spark, Polaris or any other data processing engine, you can easily integrate it with ``pipeline-flow``.

We recommend you to visit our [documentation](https://pipeline-flow.readthedocs.io/en/latest/index.html)


## Features

- YAML config based
- Plugin architecture based
- Supports ETL, ELT and ETL data pipelines.
- YAML suports env variables, local variables and secrets for re-usability and consistency.


## Installation
pipeline-flow is available on PyPI and can be installed using pip or poetry. To install using pip, run:


```bash
pip install pipeline-flow  # or better use poetry
```

Make sure you know how to get started, [check out our docs](https://pipeline-flow.readthedocs.io/en/latest/pages/intro/quick_start.html)

## Contributing
See [Contributing Guide](CONTRIBUTING.md) for more details.