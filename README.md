# datagov-harvesting-logic

This is a library that will be utilized for metadata extraction, validation,
transformation, and loading into the data.gov catalog.

## Features

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

## Requirements

This project is using `poetry` to manage this project. Install [here](https://python-poetry.org/docs/#installation).

Once installed, `poetry install` installs dependencies into a local virtual environment.

We use [Ruff](https://github.com/astral-sh/ruff) to format and lint our Python files. If you use VS Code, you can install the formatter [here](https://marketplace.visualstudio.com/items?itemName=charliermarsh.ruff).

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

## Comparison

- `./tests/harvest_sources/ckan_datasets_resp.json`
  - Represents what ckan would respond with after querying for the harvest source name
- `./tests/harvest_sources/dcatus_compare.json`
  - Represents a changed harvest source
  - Created:
    - datasets[0]

        ```diff
        + "identifier" = "cftc-dc10"
        ```

  - Deleted:
    - datasets[0]

        ```diff
        - "identifier" = "cftc-dc1"
        ```

  - Updated:
    - datasets[1]

        ```diff
        - "modified": "R/P1M"
        + "modified": "R/P1M Update"
        ```

    - datasets[2]

        ```diff
        - "keyword": ["cotton on call", "cotton on-call"]
        + "keyword": ["cotton on call", "cotton on-call", "update keyword"]
        ```

    - datasets[3]

        ```diff
        "publisher": {
          "name": "U.S. Commodity Futures Trading Commission",
          "subOrganizationOf": {
        -   "name": "U.S. Government"
        +   "name": "Changed Value"
          }
        }
        ```

- `./test/harvest_sources/dcatus.json`
  - Represents an original harvest source prior to change occuring.


## Flask App

### Local development 

1. set your local configurations in `.env` file.

2. Use the Makefile to set up local Docker containers, including a PostgreSQL database and the Flask application:

   ```bash
   make build 
   make up
   make test
   make clean
   ```

   This will start the necessary services and execute the test.

3. when there are database DDL changes, use following steps to generate migration scripts and update database:

    ```bash
    docker compose up -d db
    docker compose run app flask db migrate -m "migration description"
    docker compose run app flask db upgrade
    ```
### Debugging
*NOTE: To use the VS-Code debugger, you will first need to sacrifice the reloading support for flask*

1. Build new containers with development requirements by running `make build-dev`

2. Launch containers by running `make up-debug`

3. In VS-Code, launch debug process `Python: Remote Attach`

4. Set breakpoints

5. Visit the site at `http://localhost:8080` and invoke the route which contains the code you've set the breakpoint on.

### Deployment to cloud.gov

#### Database Service Setup

A database service is required for use on cloud.gov.

In a given Cloud Foundry `space`, a db can be created with 
`cf create-service <service offering> <plan> <service instance>`. 

In dev, for example, the db was created with 
`cf create-service aws-rds micro-psql harvesting-logic-db`. 

Creating databases for the other spaces should follow the same pattern, though the size may need to be adjusted (see available AWS RDS service offerings with `cf marketplace -e aws-rds`).

Any created service needs to be bound to an app with `cf bind-service <app> <service>`. With the above example, the db can be bound with 
`cf bind-service harvesting-logic harvesting-logic-db`.

Accessing the service can be done with service keys. They can be created with `cf create-service-keys`, listed with `cf service-keys`, and shown with 

`cf service-key <service-key-name>`.

#### Manually Deploying the Flask Application to development

1. Ensure you have a `manifest.yml` and `vars.development.yml` file configured for your Flask application. The vars file may include variables: 

    ```bash
    app_name: harvesting-logic
    database_name: harvesting-logic-db
    route-external: harvester-dev-datagov.app.cloud.gov
    ```

2. Deploy the application using Cloud Foundry's `cf push` command with the variable file:

   ```bash
   poetry export -f requirements.txt --output requirements.txt --without-hashes
   cf push --vars-file vars.development.yml
   ```

3. when there are database DDL changes, use following to do the database update:

    ```bash
    cf run-task harvesting-logic --command "flask db upgrade" --name database-upgrade
    ```