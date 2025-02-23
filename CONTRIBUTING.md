# How to contribute

## Tutorials

If you want to start working on this project, you should familiarize with the [main concepts](https://pipeline-flow.readthedocs.io/en/latest/pages/intro/core_concepts.html).

## Dependencies

We use [`VS Code Dev Containers`](https://code.visualstudio.com/docs/devcontainers/containers) to setup the development environment, with [`poetry`](https://github.com/python-poetry/poetry) to manage
all the dependencies.

To get started, execute the following command from the project root:

```bash
make setup
```
This command sets up the working environment for you, including creating the virtual environment, installing all depedencies, and configuring pre-commit hooks.


## Linters
We use [`Ruff`](https://docs.astral.sh/ruff/) as the primary Python linter and code formatter.

To ensure your code adheres to best practices, run:

```bash
make format # Formats the files (removes extra spaces, fixes indentation, etc.)
make build  # Runs lint checks
```

## Tests
We use `pytest` as the main framework for testing our code.

To run the tests, simply execude:

```bash
make test
```


## Submitting your code

Before submitting your code please do the following steps:

1. Run `make precommit` to ensure everything is working before making changes.
2. Add your changes.
3. Write tests for any new functionality.
4. Update documentation if your changes are significant.
5. Update `CHANELOG.md` with a brief summary of your changes.
6. Run `make precommit` to ensure all checks and tests have passed. 

