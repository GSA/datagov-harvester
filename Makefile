SHELL=/bin/bash -o pipefail

all: help

pypi-upload: build-dist  ## Uploads new package to PyPi after clean, build
	poetry publish

update-dependencies: ## Updates requirements.txt and requirements_dev.txt from pyproject.toml
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

install-static: ## Installs static assets
	cd app/static; \
	npm install; \
	npm run build

test-unit: ## Runs unit tests. Compatible with dev environment / `make up`
	poetry run pytest  --local-badge-output-dir tests/badges/unit/ --cov-report term-missing --junitxml=pytest-unit.xml --cov=harvester ./tests/unit | tee pytest-coverage-unit.txt

test-integration: ## Runs integration tests. Compatible with dev environment / `make up`
	poetry run pytest --local-badge-output-dir tests/badges/integration/ --cov-report term-missing --junitxml=pytest-integration.xml --cov=harvester ./tests/integration | tee pytest-coverage-integration.txt

test-functional: ## Runs functional tests. Compatible with dev environment / `make up`
	poetry run pytest --local-badge-output-dir tests/badges/functional/ --noconftest --cov-report term-missing --junitxml=pytest-functional.xml --cov=harvester ./tests/functional | tee pytest-coverage-functional.txt

test-playwright: ## Runs playwright tests. Compatible with dev environment / `make up`
	poetry run pytest --local-badge-output-dir tests/badges/playwright/ --cov-report term-missing --junitxml=pytest-playwright.xml --cov=app ./tests/playwright | tee pytest-coverage-playwright.txt

test: up test-unit test-integration ## Runs all tests. Compatible with dev environment / `make up`

test-e2e: ## Runs all e2e tests. NOT compatible with dev environment / `make up`
	docker compose up -d
	make test-playwright
	make test-functional
	docker compose down

test-ci: ## Runs all simulated tests using only db and required test resources. NOT compatible with dev environment / `make up`
	docker compose up -d db nginx-harvest-source transformer
	make test-unit
	make test-integration
	docker compose down

up: ## Sets up local flask and harvest runner docker environments. harvest runner gets DATABASE_PORT from .env
	DATABASE_PORT=5433 docker compose up -d
	docker compose -p harvest-app up db -d

up-unified: ## For testing when you want a shared db between flask and harvester
	docker compose up -d

down: ## Tears down the flask and harvester containers
	docker compose down
	docker compose -p harvest-app down

up-debug: ## Sets up local docker environment with VSCODE debug support enabled
	docker compose -f docker-compose.yml -f docker-compose_debug.yml up -d

up-prod: ## Sets up local flask env running gunicorn instead of standard dev server
	docker compose -f docker-compose.yml -f docker-compose_prod.yml up -d

clean: ## Cleans docker images
	docker compose down -v --remove-orphans
	docker compose -p harvest-app down -v --remove-orphans

lint:  ## Lints wtih ruff, isort, black
	ruff check .
	isort .
	black .

# Output documentation for top-level targets
# Thanks to https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
.PHONY: help

help: ## This help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-10s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
