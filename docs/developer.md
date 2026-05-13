# Developer quickstart

## Local development

We use docker containers to run the application locally.

### Running the app

Build the static assets (requires `npm`):

```
% make install-static
```

Build and bring up docker containers:
```
% make build-dev
% make up
```

Refer to the [`Makefile`](/Makefile) for additional commands.

Note that you do not need to set the `CF_SERVICE_USER` and `CF_SERVICE_AUTH` variables. The app will emit a warning about these; they are needed only in the Cloud.gov environment.

#### Using the app

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

This repo contains pre-commit actions. Learn how to configure your IDE to run those [here](https://pre-commit.com/).


## Additional tools

We use `poetry` to manage this project. It is installed and run automatically within the app container. You might also find it helpful to install it locally. Install poetry [here](https://python-poetry.org/docs/#installation).


Currently, this project is pinned to python version 3.12.12 (check `pyproject.toml` to verify). To install and use this specific version:

```
% poetry python install 3.12.12
% poetry env use 3.12.12
```

Once installed, `poetry install` installs dependencies into a local virtual environment.

To update poetry itself locally (matching CI, which will always use the latest version), run `poetry self update` (or `make poetry-update`).


## Local development

Set these environment variables in your shell:

??? Do we need these to map to cloud.gov for local dev?

- CF_SERVICE_USER
- CF_SERVICE_AUTH

?? poetry install as above? or not?





CF_SERVICE_* variables can be extracted from from service-keys by running `cf service-key ci-deployer dhl-deployer` in the appropriate space.

<--
CKAN_API_TOKEN should be extracted from `cf env datagov-harvest` in the `user-provided` service `datagov-harvest-secrets` with the same key name.
-->

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
      CKAN_URL=`ip-address-from-step-4a`:5000
      CKAN_API_URL=`ip-address-from-step-4a`:5000
      CKAN_API_TOKEN=`api-token-from-step-3`
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

For tests to pass, you may have to pull the latest MDTranslator. Use `docker compose pull` to get the latest versions of the docker images.

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

The harvester also expects an OpenSearch service named `datagov-catalog-opensearch`. The provisioning script creates it with Cloud.gov's `aws-elasticsearch` broker and requests `OpenSearch_2.11`, using `es-medium` in development and `es-medium-ha` in staging and `es-large` in production.

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

Or, if doing for local development:

`docker compose exec app flask user add your.i.name@gsa.gov --name yourName`

#### add organizations

`cf run-task datagov-harvest-admin --name "add new org" --command "flask org add 'Name of Org' --log https://some-url.png --id 1234"`
