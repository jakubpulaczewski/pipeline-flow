.PHONY: clean format precommit setup test build

src_dir := src
tests_dir := tests

default: help

help:
	@grep -E '^[a-zA-Z0-9 -]+:.*#'  Makefile | sort | while read -r l; do printf "\033[1;32m$$(echo  $$l | cut -f 1 -d':')\033[00m:$$(echo $$l | cut -f 2- -d'#')\n"; done

build: ## Lint and compile code
	ruff check ${src_dir} ${tests_dir}
	poetry run pyright ${src_dir} ${tests_dir}
	@echo "Build succeeded"

clean: ## Remove build outputs, test outputs and cached files.
	@rm -rf .pytest_cache .coverage
	@echo "Clean succeeded"

format: ## Reformat source code
	@ruff format ${src_dir} ${tests_dir} -v
	
precommit: format build test ## Running Precommit checks.
	@echo "Pre-commit checks completed successfully."

poetry-publish:
	@echo "Publishing to PyPI..."
	poetry publish --build
	@echo "Publishing complete!"

setup: ## Setup or update local env
	@echo "Setting up or updating local environment..."
	rm -rf .venv/
	poetry config virtualenvs.in-project true
	poetry install
	pre-commit install
	@echo "Environment setup complete!"

test:
	poetry run pytest
