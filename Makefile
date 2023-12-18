pypi-upload: build-dist  ## Uploads new package to PyPi after clean, build
	twine upload dist/*	

pypi-upload-test: build-dist  ## Uploads new package to TEST PyPi after clean, build
	twine upload -r testpypi dist/*	

build-dist: clean-dist  ## Builds new package dist
	poetry build --verbose
	
clean-dist:  ## Cleans dist dir
	rm -rf dist/*

test: up ## Runs poetry tests, ignores ckan load
	poetry run pytest --ignore=./tests/load/ckan

up: ## Sets up local docker environment
	docker compose up -d

lint:  ## Lints wtih ruff
	ruff .

# Output documentation for top-level targets
# Thanks to https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
.PHONY: help
help: ## This help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-10s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
