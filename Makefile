src_dir := src
tests_dir := tests

build: ## Lint and compile code
	poetry run flake ${src_dir} ${tests_dir}
	poetry run pylint ${src_dir} ${tests_dir}
	poetry run mypy ${src_dir} ${tests_dir}
	@echo "Build succeeded"

clean: ## Remove build outputs, test outputs and cached files.
	@rm -rf .mypy_cache .pytest_cache .coverage
	@echo "Clean succeeded"

format: #Reformat source code
	@poetry run isort ${src_dir} ${tests_dir}
	@poetry run black ${src_dir} ${tests_dir}

precommit: clean format build test
	@poetry export --dev --format requirements.txt | poetry run safety check --stdin

setup: ## Setup or update local env
	@echo "Setting up or updating local environment..."
	rm -rf .venv/
	poetry config virtualenvs.in-project true
	poetry install
	pre-commit install
	@echo "Environment setup complete!"
	
test:
	@poetry run pytest