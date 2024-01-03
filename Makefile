pypi-upload: build-dist  ## Uploads new package to PyPi after clean, build
	poetry publish

# pypi-upload-test: build-dist  ## Uploads new package to TEST PyPi after clean, build
# 	twine upload -r testpypi dist/*	

build-dist: clean-dist  ## Builds new package dist
	poetry build --verbose
	
clean-dist:  ## Cleans dist dir
	rm -rf dist/*

test: up ## Runs poetry tests, ignores ckan load
	poetry run pytest --ignore=./tests/integration

up: ## Sets up local docker environment
	docker compose up -d

down: ## Shuts down local docker instance
	docker-compose down

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
