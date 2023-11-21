.DEFAULT_GOAL := help

up: ## Brings up docker airflow instance using LocalExecutor
	docker-compose up -d

down: ## Shuts down local docker airflow instance using LocalExecutor
	docker-compose down

up-celery: ## Brings up docker airflow instance using CeleryExecutor
	docker-compose -f docker-compose_celery.yml up -d 

down-celery: ## Shuts down local docker airflow instance using CeleryExecutor
	docker-compose -f docker-compose_celery.yml down
	
scale-up: ## Scales up CF airflow test deploy in current CF space
	cf scale airflow-test-scheduler -i 2
	cf scale airflow-test-webserver -i 1

scale-down: ## Scales down CF airflow test deploy in current CF space
	cf scale airflow-test-webserver -i 0
	cf scale airflow-test-scheduler -i 0

# Output documentation for top-level targets
# Thanks to https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
.PHONY: help
help: ## This help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-10s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
