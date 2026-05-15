# Developer quickstart

We use docker containers to run the application locally.

## Running the app

Build the static assets (requires `npm`):

```
% make install-static
```

Build and bring up docker containers:
```
% make build
% make up
```

Refer to the [`Makefile`](/Makefile) for additional commands.

Note that you do not need to set the `CF_SERVICE_USER` and `CF_SERVICE_AUTH` variables. The app will emit a warning about these; they are needed only in the Cloud.gov environment.

### Using the app

Point your web browser to https://localhost:8080

You will need to be able to log in! We use login.gov, and for local development you must have an account at the login.gov sandbox `https://idp.int.identitysandbox.gov`. (Click "Create an account" if you don't already have one.)


Add your user account to the local app, using an email address that matches your login.gov sandbox account (see also "user management" below):
```
% docker compose exec app flask user add your.i.name@gsa.gov --name yourName
User added successfully!
```

Now you should be able to log in (at https://localhost:8080/login ), add an organization, and add a feed to it.

## Linting and IDE setup

This is primarily a python project.

We use [Ruff](https://github.com/astral-sh/ruff) to format and lint our Python files. If you use VS Code, you can install the formatter [here](https://marketplace.visualstudio.com/items?itemName=charliermarsh.ruff).

## Git setup and conventions

 - This repo contains pre-commit actions. Learn how to configure your IDE to run those [here](https://pre-commit.com/).
 - Create a branch from `main`. We prefer short descriptive branch names.
 - To test changes in the `development` space in Cloud.gov, merge changes into the `develop` branch. Coordinate with other developers by announcing your plans in #datagov-devsecops.


## Flask Debugging

If you absolutely need to hit a breakpoint in your Flask app, you can setup local Flask debugging in your IDE.

*NOTE: To use the VS-Code debugger, you will first need to sacrifice the reloading support for flask*

1. Build new containers with development requirements by running `make build-dev`

2. Launch containers by running `make up-debug`

3. In VS-Code, launch debug process `Python: Remote Attach`

4. Set breakpoints

5. Visit the site at `http://localhost:8080` and invoke the route which contains the code you've set the breakpoint on.


## Testing

### Install poetry

We use `poetry` to manage this project, and to run the tests. Install poetry [here](https://python-poetry.org/docs/#installation). (Poetry is also installed and run automatically within the app container, which is why you didn't need it to get the app up and running.)

Currently, this project is pinned to python version 3.12.12 (check `pyproject.toml` to verify). To install and use this specific version:

```
% poetry python install 3.12.12
% poetry env use 3.12.12
```

Once poetry is installed, `poetry install` installs dependencies into a local virtual environment.

To update poetry itself locally (matching CI, which will always use the latest version), run `poetry self update` (or `make poetry-update`).


### Harvester testing

- These tests are found in `extract`, and `validate`. Some of them rely on services in the `docker-compose.yml`. Run using docker `docker compose up -d` and with the command `poetry run pytest`.

For tests to pass, you may have to pull the latest MDTranslator. Use `docker compose pull` to get the latest versions of the docker images.


### Integration testing

TODO: does this still apply? TODO

- to run integration tests locally add the following env variables to your .env file in addition to their appropriate values
  - CF_SERVICE_USER = "put username here"
  - CF_SERVICE_AUTH = "put password here"


### Database migrations

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

## Deployment to Cloud.gov

Github workflows automatically deploy:
 - to the `development` space when the `develop` branch is updated
 - to `staging` and `prod` when the `main` branch is updated

Data.gov team members can deploy to `development` from the command line. The remainder of this document provides background on the Cloud.gov configuration.

*Warning: this documentation has not been tested recently!*

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

The harvester also expects an OpenSearch service named `datagov-catalog-opensearch`. The provisioning script creates it with Cloud.gov's `aws-elasticsearch` broker and requests `OpenSearch_2.11`, using `es-medium` in development and `es-medium-ha` in staging and `es-large` in production.

#### User provided

A user provided service by the name of `datagov-harvest-secrets` is also expected to be in place and populated with the following secrets:

- CF_SERVICE_AUTH
- CF_SERVICE_USER
- FLASK_APP_SECRET_KEY
- OPENID_PRIVATE_KEY

### Manually Deploying the Flask Application to development

1. Ensure you have a `manifest.yml` and `vars.development.yml` file configured for your Flask application. The vars file may include variables:

    ```bash
    app_name: datagov-harvest
    database_name: datagov-harvest-db
    route_external: harvest-dev.data.gov
    route_internal: datagov-harvest-dev.apps.internal
    proxy_instances: 1
    basic_auth_enabled: on
    ```

2. Deploy the application using Cloud Foundry's `cf push` command with the variable file:

   ```bash
   poetry export -f requirements.txt --output requirements.txt --without-hashes
   cf push --vars-file vars.development.yml
   ```

## Applications

### datagov-harvest-proxy
This is an nginx app which owns the public route and proxies traffic to the internal Flask app route.

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


#### user management

`cf run-task datagov-harvest-admin --name "add new user" --command "flask user add xxx@gsa.gov --name xxx"`

Or, if doing for local development:

`docker compose exec app flask user add your.i.name@gsa.gov --name yourName`

#### add organizations

`cf run-task datagov-harvest-admin --name "add new org" --command "flask org add 'Name of Org' --log https://some-url.png --id 1234"`
