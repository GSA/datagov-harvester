# datagov-harvester

This repository holds the source code the Data.gov Harvester 2.0, which consiste of two applications:
- [datagov-harvest-admin](#datagov-harvest-admin)
- [datagov-harvest-runner](#datagov-harvest-runner)

## Setup

This project is using `poetry` to manage this project. Install [here](https://python-poetry.org/docs/#installation).

Once installed, `poetry install` installs dependencies into a local virtual environment.

We use [Ruff](https://github.com/astral-sh/ruff) to format and lint our Python files. If you use VS Code, you can install the formatter [here](https://marketplace.visualstudio.com/items?itemName=charliermarsh.ruff).

### Local development

Set these environment variables in your shell:

- CF_SERVICE_USER
- CF_SERVICE_AUTH
- CKAN_API_TOKEN

CF_SERVICE_* variables can be extracted from from service-keys by running `cf service-key ci-deployer dhl-deployer` in the appropriate space.

CKAN_API_TOKEN should be extracted from `cf env datagov-harvest-runner` in the `user-provided` service `datagov-harvest-secrets` with the same key name.

#### Flask Debugging
*NOTE: To use the VS-Code debugger, you will first need to sacrifice the reloading support for flask*

1. Build new containers with development requirements by running `make build-dev`

2. Launch containers by running `make up-debug`

3. In VS-Code, launch debug process `Python: Remote Attach`

4. Set breakpoints

5. Visit the site at `http://localhost:8080` and invoke the route which contains the code you've set the breakpoint on.

## Testing

### CKAN load testing

- CKAN load testing doesn't require the services provided in the `docker-compose.yml`.
- [catalog-dev](https://catalog-dev.data.gov/) is used for ckan load testing.
- Create an api-key by signing into catalog-dev.
- Create a `credentials.py` file at the root of the project containing the variable `ckan_catalog_dev_api_key` assigned to the api-key.
- Run tests with the command `poetry run pytest ./tests/load/ckan`

### Harvester testing

- These tests are found in `extract`, and `validate`. Some of them rely on services in the `docker-compose.yml`. Run using docker `docker compose up -d` and with the command `poetry run pytest --ignore=./tests/load/ckan`.

If you followed the instructions for `CKAN load testing` and `Harvester testing` you can simply run `poetry run pytest` to run all tests.

### Integration testing
- to run integration tests locally add the following env variables to your .env file in addition to their appropriate values
  - CF_SERVICE_USER = "put username here"
  - CF_SERVICE_AUTH = "put password here"

## Apps
### datagov-harvest-admin
This is a Flask app which manages the configuration of harvest sources, organizations, and the creation of harvest jobs.


### datagov-harvest-runner
This is a python application, chiefy comprised of files in the `harvester` directory.

#### Features

- Extract
  - General purpose fetching and downloading of web resources.
  - Catered extraction to the following data formats:
    - DCAT-US
- Validation
  - DCAT-US
    - `jsonschema` validation using draft 2020-12.
- Load
  - DCAT-US
    - Conversion of dcat-us catalog into ckan dataset schema
    - Create, delete, update, and patch of ckan package/dataset

## Deployment to cloud.gov

### Services

#### Database

A database service is required for use on cloud.gov.

In a given Cloud Foundry `space`, a db can be created with
`cf create-service <service offering> <plan> <service instance>`.

In dev, for example, the db was created with
`cf create-service aws-rds micro-psql datagov-harvest-db`.

Creating databases for the other spaces should follow the same pattern, though the size may need to be adjusted (see available AWS RDS service offerings with `cf marketplace -e aws-rds`).

Any created service needs to be bound to an app with `cf bind-service <app> <service>`. With the above example, the db can be bound with
`cf bind-service harvesting-logic datagov-harvest-db`.

Alternately, you can just push the app up and it will bind with the services so long as they are named following the expected pattern in `manifest.yml`.

#### User provided

A user provided service by the name of `datagov-harvest-secrets` is also expected to be in place and populated with the following secrets:

- CF_SERVICE_AUTH
- CF_SERVICE_USER
- CKAN_API_TOKEN
- FLASK_APP_SECRET_KEY
- OPENID_PRIVATE_KEY

### Commands

#### database migrations

When there are database DDL changes, use following to do the database update:

    ```bash
    cf run-task harvesting-logic --command "flask db upgrade" --name database-upgrade
    ```

#### user management

`cf run-task datagov-harvest-admin --name "add new user" --command "flask user add xxx@gsa.gov --name xxx"`

#### add organizations

`cf run-task datagov-harvest-admin --name "add new org" --command "flask org add 'Name of Org' --log https://some-url.png --id 1234"`

### Manually Deploying the Flask Application to development

1. Ensure you have a `manifest.yml` and `vars.development.yml` file configured for your Flask application. The vars file may include variables:

    ```bash
    app_name: harvesting-logic
    database_name: harvesting-logic-db
    route_external: harvester-dev-datagov.app.cloud.gov
    ```

2. Deploy the application using Cloud Foundry's `cf push` command with the variable file:

   ```bash
   poetry export -f requirements.txt --output requirements.txt --without-hashes
   cf push --vars-file vars.development.yml
   ```
