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

test-unit:
	HARVEST_SOURCE_URL=http://localhost:81 DATABASE_URI=postgresql://myuser:mypassword@localhost:5433/mydb poetry run pytest --junitxml=pytest.xml --cov=harvester ./tests/unit

test-integration:
	HARVEST_SOURCE_URL=http://localhost:81 DATABASE_URI=postgresql://myuser:mypassword@localhost:5433/mydb poetry run pytest --junitxml=pytest.xml --cov=harvester ./tests/integration

test: test-services
	HARVEST_SOURCE_URL=http://localhost:81 DATABASE_URI=postgresql://myuser:mypassword@localhost:5433/mydb poetry run pytest
	make clean-test-services

test-services:
	DATABASE_PORT=5433 HARVEST_SOURCE_PORT=81 docker compose -p integration-test-services up nginx-harvest-source db -d

clean-test-services:
	docker compose -p integration-test-services down

up: ## Sets up local flask and harvest runner docker environments. harvest runner gets DATABASE_PORT from .env
	DATABASE_PORT=5433 docker compose up -d
	docker compose -p harvest-app up db -d

down: ## Tears down the flask and harvester containers
	docker compose down
	docker compose -p harvest-app down

up-debug: ## Sets up local docker environment
	docker compose -f docker-compose.yml -f docker-compose_debug.yml up -d
	
clean: down ## Cleans docker images
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
