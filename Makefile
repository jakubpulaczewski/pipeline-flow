.PHONY: clean format precommit setup test build

src_dir := src
tests_dir := tests

default: help

help:
	@grep -E '^[a-zA-Z0-9 -]+:.*#'  Makefile | sort | while read -r l; do printf "\033[1;32m$$(echo  $$l | cut -f 1 -d':')\033[00m:$$(echo $$l | cut -f 2- -d'#')\n"; done

build: ## Lint and compile code - #TODO: Need change to use ruff and pyright for checking.
	ruff check ${src_dir} ${tests_dir}
	poetry run pyright ${src_dir} ${tests_dir}
	@echo "Build succeeded"

clean: ## Remove build outputs, test outputs and cached files.
	@rm -rf .pytest_cache .coverage
	@echo "Clean succeeded"

format: ## Reformat source code
	@ruff format ${src_dir} ${tests_dir} -v
	
precommit: ## Running Precommit checks.
	format
	build
	test
	@poetry export --dev --format requirements.txt | poetry run safety check --stdin

setup: ## Setup or update local env
	@echo "Setting up or updating local environment..."
	rm -rf .venv/
	poetry config virtualenvs.in-project true
	poetry install
	pre-commit install
	@echo "Environment setup complete!"

test:
	poetry run pytest
