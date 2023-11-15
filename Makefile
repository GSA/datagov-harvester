# default docker image tag
tag = development
	
.PHONY: build-docker-development

# build new docker image
# default tag is `development`
# supply an argument (ex. `make build-docker tag=0.0.1`) if you want to build something other than default
build-docker:
	@if [ -z "${AIRFLOW_POSTGRES_URI_STRING}" ] ; then echo "ERROR >> POSTGRES_URI_STRING must be set in your shell" ; false ; fi
	@echo "### building docker image and tagging as $(tag) ###"
	@echo "### binding to Postgres DB at this URI: $(AIRFLOW_POSTGRES_URI_STRING) ###"
	docker build . -f Dockerfile --pull --tag airflow-test:$(tag) --build-arg AIRFLOW_POSTGRES_URI_STRING=$(AIRFLOW_POSTGRES_URI_STRING)
	docker tag airflow-test:$(tag) ghcr.io/gsa/airflow-test:$(tag)
	docker push ghcr.io/gsa/airflow-test:$(tag)

scale-up:
	cf scale airflow-test-scheduler -i 2
	cf scale airflow-test-webserver -i 1

scale-down:
	cf scale airflow-test-webserver -i 0
	cf scale airflow-test-scheduler -i 0
