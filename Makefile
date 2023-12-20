.DEFAULT_GOAL := help

up: ## Brings up docker airflow instance using CeleryExecutor
	docker-compose up -d

down: ## Shuts down local docker airflow instance using CeleryExecutor
	docker-compose down

clean: ## Cleans docker images
	docker compose down -v --remove-orphans

build: ## Forces clean build from Dockerfile
	docker compose build --no-cache --pull
	
scale-up: ## Scales up CF airflow test deploy in current CF space
	cf scale airflow-test-scheduler -i 1
	cf scale airflow-test-webserver -i 1
	cf scale airflow-test-worker -i 1

scale-down: ## Scales down CF airflow test deploy in current CF space
	cf scale airflow-test-webserver -i 0
	cf scale airflow-test-scheduler -i 0
	cf scale airflow-test-worker -i 0

push:
	cf push --vars-file vars.<space>.yml

# Output documentation for top-level targets
# Thanks to https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
.PHONY: help
help: ## This help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-10s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
