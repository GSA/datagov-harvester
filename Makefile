pypi-upload: build-dist  ## Uploads new package to PyPi after clean, build
	poetry publish

deps-update: ## Updates requirements.txt and requirements_dev.txt from pyproject.toml
	poetry export --without-hashes --without=dev --format=requirements.txt > requirements.txt
	poetry export --without-hashes --only=dev --format=requirements.txt > requirements-dev.txt 

# pypi-upload-test: build-dist  ## Uploads new package to TEST PyPi after clean, build
# 	twine upload -r testpypi dist/*	

build-dist: clean-dist  ## Builds new package dist
	poetry build --verbose
	
build:  ## build Flask app
	docker compose build app

build-dev:  ## build Flask app w/ dev dependencies
	docker compose build app --build-arg DEV=True

clean-dist:  ## Cleans dist dir
	rm -rf dist/*

test: up ## Runs poetry tests, ignores ckan load
	poetry run pytest --ignore=./tests/integration  --ignore=./scripts/load_test.py

up: ## Sets up local docker environment
	docker compose up -d

up-debug: ## Sets up local docker environment
	docker compose -f docker-compose_debug.yml up -d

down: ## Shuts down local docker instance
	docker compose down

clean: ## Cleans docker images
	docker compose down -v --remove-orphans

lint:  ## Lints wtih ruff, isort, black
	ruff .
	isort .
	black .

# Output documentation for top-level targets
# Thanks to https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
.PHONY: help
help: ## This help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-10s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
