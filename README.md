# datagov-harvester

| Test Suite | Count | Coverage |
| --- | --- | --- |
| Unit | [![Unit Test Count](./tests/badges/unit/tests.svg)](./tests/unit)| [![Unit Test Coverage](./tests/badges/unit/coverage.svg)](./tests/unit)|
| Integration | [![Unit Test Count](./tests/badges/integration/tests.svg)](./tests/integration)| [![Unit Test Coverage](./tests/badges/integration/coverage.svg)](./tests/integration)|
| Functional | [![Functional Test Count](tests/badges/functional/tests.svg)](./tests/functional)| [![Unit Test Coverage](./tests/badges/functional/coverage.svg)](./tests/functional/)|

This repository holds the source code the Data.gov Harvester 2.0, which consists of two applications:

- [datagov-harvest-admin](#datagov-harvest-admin)
- [datagov-harvest-runner](#datagov-harvest-runner)

## Documentation

Current sequence diagrams are availale in the the `/docs/diagrams/dest` folder.

Documentation for the Harvseter 2.0 project is found in Google Drive: [Harvester 2.0 folder](https://drive.google.com/drive/folders/11mhCBb9vWxrTHV_s_S4pZhhI2fGHmxrq)

Historic documentation can be found in the `Archive` [folder](https://drive.google.com/drive/folders/1DG1oxSCoeru2-bbmSfcBnIiCZUg-N1Az), along with historic diagrams [here](https://drive.google.com/drive/folders/1GJZYVVMubGG54d5yCZY9zoea3xjZ9HNI)

Application specific documentation [below](#applications).

## Setup

This project is using `poetry` to manage this project. Install poetry [here](https://python-poetry.org/docs/#installation).

Once installed, `poetry install` installs dependencies into a local virtual environment.

We use [Ruff](https://github.com/astral-sh/ruff) to format and lint our Python files. If you use VS Code, you can install the formatter [here](https://marketplace.visualstudio.com/items?itemName=charliermarsh.ruff).

This repo contains pre-commit actions. Learn how to configure your IDE to run those [here](https://pre-commit.com/)

## Local development

Set these environment variables in your shell:

- CF_SERVICE_USER
- CF_SERVICE_AUTH
- CKAN_API_TOKEN

CF_SERVICE_* variables can be extracted from from service-keys by running `cf service-key ci-deployer dhl-deployer` in the appropriate space.

CKAN_API_TOKEN should be extracted from `cf env datagov-harvest-runner` in the `user-provided` service `datagov-harvest-secrets` with the same key name.

### Flask Debugging
If absolutely need to hit a breakpoint in your Flask app, you can setup local Flask debugging in your IDE.

*NOTE: To use the VS-Code debugger, you will first need to sacrifice the reloading support for flask*

1. Build new containers with development requirements by running `make build-dev`

2. Launch containers by running `make up-debug`

3. In VS-Code, launch debug process `Python: Remote Attach`

4. Set breakpoints

5. Visit the site at `http://localhost:8080` and invoke the route which contains the code you've set the breakpoint on.

### Local Flask to CKAN workflow
You can configure a local instance of the Flask application to push datasets to a local CKAN instance. This dramatically cuts development time and is a huge increase to efficiency.

!NOTE: Currenly this process is not user-friendly but is expected to be improved in the future.

To setup:

  1. Launch a local instance of the datagov-harvest-admin app,
    a. Create an organization, and grab the org id
    b. Create a harvest source with a valid harvest url
    c. This [test source](https://raw.githubusercontent.com/GSA/catalog.data.gov/refs/heads/main/tests/harvest-sources/data.json) is a good one to use to test integration
  2. Launch a local instance of catalog.data.gov, aka CKAN
  3. Login to CKAN as admin and generate an API token. Copy that token.
  4. Install ckanapi via pip so that it's available in your local shell, `pip install python`
  5. use ckanapi to create an organization with same id as the organization in flask app, ex.
    `ckanapi action organization_create -r http://localhost:5000 name=test-org id=5cf8d5a1-12be-4300-a249-20ca584681cf`
  6. Get internal IP for your machine.
    a. On Mac's this can be derived by running `ifconfig | grep inet` and grabbing the first `192.168.xxx.xxx` address you find.
  7. Add two new values to the local `.env` file:
    ```
      CKAN_API_URL=`ip-address-from-step-4a`:5000
      CKAN_API_TOKEN=`api-token-from-step-3`:5000
    ```
  8. Restart datagov-harvest-admin, `make up`
  9. GoTo your previously configured harvest source
  10. Click "Harvest" button. This will fail.
  11. `docker exec` into local datagov-harvest-admin and run same job command from Step 8.
    a. this is usually `python harvester/harvest.py {job_id} {job_type}`
  12. You should now see successful harvest of your datasets 🎉

## Testing

### Harvester testing

- These tests are found in `extract`, and `validate`. Some of them rely on services in the `docker-compose.yml`. Run using docker `docker compose up -d` and with the command `poetry run pytest --ignore=./tests/load/ckan`.

If you followed the instructions for `CKAN load testing` and `Harvester testing` you can simply run `poetry run pytest` to run all tests.

### Integration testing
- to run integration tests locally add the following env variables to your .env file in addition to their appropriate values
  - CF_SERVICE_USER = "put username here"
  - CF_SERVICE_AUTH = "put password here"

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

## Applications

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
- Report
  - Update harvest job db record with job results
  - Email results using SMTP client

### Flask Commands

#### database migrations

When altering the db during development, you first want to stamp the db before making any changes to the model.

```bash
make clean up
docker compose exec app bash
```

Once inside the container, you run:

```bash
flask db stamp head
```

Apply your changes to the model file, then run:

```bash
flask db migrate -m "your migration message here"
```

Then, finally, to apply your changes in place to the local db, run:

```bash
flask db upgrade
```

#### user management

`cf run-task datagov-harvest-admin --name "add new user" --command "flask user add xxx@gsa.gov --name xxx"`

#### add organizations

`cf run-task datagov-harvest-admin --name "add new org" --command "flask org add 'Name of Org' --log https://some-url.png --id 1234"`
