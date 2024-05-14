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

up-flask: ## Sets up local flask app docker environment
	DATABASE_PORT=5432 docker compose -p flask-app up app db -d

up-harvester: ## Sets up local harvester app docker environment. DATABASE_PORT derives from the local .env
	docker compose -p harvest-app up nginx-harvest-source db -d 

up-debug: ## Sets up local docker environment
	docker compose -f docker-compose_debug.yml up -d

down-flask: ## Shuts down flask app container
	docker compose -p flask-app down

down-harvester:	### Shuts down harvester app container
	docker compose -p harvest-app down

down-all:
	docker compose -p flask-app down; docker compose -p harvest-app down
	
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
