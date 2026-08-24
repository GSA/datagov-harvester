# Developer quickstart

We use docker containers to run the application locally.

## Running the app

Make sure the DCAT-US 3.0 schema submodule is checked out first — see
[DCAT-US 3.0 schemas](#dcat-us-30-schemas). If you cloned with
`git clone --recurse-submodules`, you already have it.

Build the static assets (requires `npm`):

```
% make install-static
```

Build and bring up docker containers:
```
% make build
% make up
% make load-test-data   # optional: load fixture orgs, sources, and jobs
```

`make up` starts one Compose stack with a single database. The Flask app,
OpenSearch, transformer, and harvest source nginx all share that network, so
the app can reach the database at the `db` hostname and harvest jobs started
via `LocalTaskHandler` use the same database.

This is the default local workflow: run `make up`, use the app at
http://localhost:8080, and trigger harvests from the UI. You do not need a
separate harvest-runner container or a second database.

To reset to a clean database with fixtures loaded:
```
% make re-up
```

Refer to the [`Makefile`](/Makefile) for additional commands.

Note that you do not need to set the `CF_SERVICE_USER` and `CF_SERVICE_AUTH` variables. They are needed only in the Cloud.gov environment.

### How harvest jobs run locally

In deployed (Cloud.gov) environments, harvest jobs run as Cloud Foundry tasks via `CFHandler`. Locally there is no CF task API, so the app falls back to `LocalTaskHandler`, which runs the same `python harvester/harvest.py <job_id> <job_type>` command as a child subprocess of the running app. The handler is selected automatically by `create_task_handler()` (`harvester/lib/task_handler.py`): it uses `CFHandler` only when running on Cloud Foundry or when all three `CF_*` credentials are configured, and otherwise uses `LocalTaskHandler`. This means you can register a harvest source and trigger a harvest locally without any Cloud Foundry credentials.

#### Optional: running the harvest runner on the host

The normal path is to trigger harvests from the app (UI or API) and let
`LocalTaskHandler` run `harvester/harvest.py` inside the app container.

For debugging harvester code directly — for example to use a debugger in your
IDE on harvester modules — you can still run the harvest runner on the host
after `make up`:

```bash
poetry install
poetry run python harvester/harvest.py <job_id> <job_type>
```

Host-side runs use `DATABASE_URI` from `.env` (`localhost:5432`), which is the
same database exposed by `make up`. No second database or separate `make`
target is required.

### Using the app

Point your web browser to http://localhost:8080

#### Local login (no Login.gov account)

To use local login, set `ENABLE_LOCAL_DEV_LOGIN=true` in your `.env`, then visit http://localhost:8080/login and sign in with:

- **Username:** `admin`
- **Password:** `admin`

This bypasses Login.gov and the `harvest_user` allow list for local development only. It is disabled by default and remains disabled in deployed environments. Login.gov sandbox remains available via the link on the login page.

#### Login.gov sandbox

Alternatively, you can log in with Login.gov. For local development you must have an account at the login.gov sandbox `https://idp.int.identitysandbox.gov`. (Click "Create an account" if you don't already have one.)

Add your user account to the local app, using an email address that matches your login.gov sandbox account (see also "user management" below):
```
% docker compose exec app flask user add your.i.name@gsa.gov --name yourName
User added successfully!
```

Now you should be able to log in at http://localhost:8080/login, add an organization, and add a feed to it.

## Linting and IDE setup

This is primarily a python project.

We use [Ruff](https://github.com/astral-sh/ruff) to format and lint our Python files. If you use VS Code, you can install the formatter [here](https://marketplace.visualstudio.com/items?itemName=charliermarsh.ruff).

## Git setup and conventions

 - This repo contains pre-commit actions. Learn how to configure your IDE to run those [here](https://pre-commit.com/).
 - Create a branch from `main`. We prefer short descriptive branch names.
 - To test changes in the `development` space in Cloud.gov, merge changes into the `develop` branch. Coordinate with other developers by announcing your plans in #datagov-devsecops.

## DCAT-US 3.0 schemas

The DCAT-US 3.0 JSON Schemas are **not in this repo**. They come from
[GSA/dcat-us](https://github.com/GSA/dcat-us), tracked as a git submodule at
`_external/dcat-us`. Paths are defined once, in
[`harvester/utils/schema_paths.py`](../harvester/utils/schema_paths.py) — import
from there rather than rebuilding paths from `__file__`.

Don't edit anything under `_external/dcat-us`. It belongs to `GSA/dcat-us`; open
a PR against that repo instead. A commit here records only which `dcat-us`
commit to check out, never file contents.

DCAT-US **1.1** is different — it has no `GSA/dcat-us` equivalent and stays
vendored in this repo under `schemas/dcatus1.1/`.

### Getting the schemas

Cloning fresh:

```
% git clone --recurse-submodules https://github.com/GSA/datagov-harvester.git
```

If you already cloned without that flag:

```
% git submodule update --init _external/dcat-us
```

To stop having to remember the flag on every clone and pull:

```
% git config --global submodule.recurse true
```

### If the submodule is missing

An uninitialized submodule leaves `_external/dcat-us` as an **empty directory**,
not a missing one, so the failure is easy to misread. Two things to know:

- **`make build` will not fix it.** `docker-compose.yml` bind-mounts `.:/app`, so
  the container sees your host working tree, empty directory included. Rebuilding
  the image accomplishes nothing.
- **The symptom** is a `FileNotFoundError` from `build_dcatus3_validator` naming
  the directory and the fix. Anything touching DCAT-US 3.0 validation raises it,
  including at test-collection time.

Confirm with `git submodule status`. A leading `-` means uninitialized:

```
-24f6f1e...  _external/dcat-us        # not initialized — run the update above
 24f6f1e...  _external/dcat-us (...)  # good (note the leading space)
+abc1234...  _external/dcat-us (...)  # checked out at a different commit than pinned
```

Don't use sparse-checkout inside the submodule. Sparse patterns live in
`.git/modules/_external/dcat-us/info/sparse-checkout`, which **cannot be
committed**, so CI and fresh clones get the full tree regardless. All it does is
make your machine disagree with CI about which schema files exist — in the one
code path whose entire job is schema validation.

### Upgrading to a newer dcat-us

The submodule is pinned to a specific `dcat-us` commit on purpose, so upgrades
are deliberate. Dependabot opens a monthly PR that bumps it; you can also do it
by hand.

Note that `--remote` follows the branch named in `.gitmodules`, which is
**`dcat-us`'s `main`** — not this repo's `main`.

```
% git -C _external/dcat-us rev-parse HEAD          # record the old SHA first
% git submodule update --remote _external/dcat-us
```

Review what actually moved, then test:

```
% git diff --submodule=log                          # commit range
% git -C _external/dcat-us diff "$old" HEAD -- jsonschema/definitions
% poetry run pytest tests/unit
```

Commit **only** the gitlink (`_external/dcat-us`) — there are no file changes to
stage.

A schema change can legitimately alter which harvest records validate, so
**failing tests here are the point of pinning, not an obstacle.** Fix the code or
reject the bump. Never skip the tests to land the SHA.

## Local development

Local configuration should be stored in `.env`, which is ignored by git.
Use `.env.sample` as the template for required local variables.

Do not commit real credentials, environment-specific secrets, or generated `.env` files.
Production and deployed environment variables are provided by the deployment platform.


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

Once poetry is installed, `poetry install` installs dependencies into a local virtual environment.

To update poetry itself locally (matching CI, which will always use the latest version), run `poetry self update` (or `make poetry-update`).


### Running tests

A number of "test" and "test-*" targets are defined in the `Makefile`.

For tests to pass, you may have to pull the latest MDTranslator. Use `docker compose pull` to get the latest versions of the docker images.

`make test` and `make test-integration` run against the database in `.env`
(`localhost:5432`). Integration tests reset schema/data during the run, so use
`make re-up` or `make load-test-data` afterward if you need fixture data back in
the dev app.


### Exporting requirements.txt

If you've added, updated, or removed any python dependencies, be sure to export requirements.txt:

   ```bash
   poetry export -f requirements.txt --output requirements.txt --without-hashes
   ```

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
- HARVEST_API_TOKEN
- OPENID_PRIVATE_KEY

CF_SERVICE_* variables can be extracted from from service-keys by running `cf service-key ci-deployer datagov-harvest-deployer` in the appropriate space.

### Manually Deploying the Flask Application to development

Note: we prefer that you deploy to the development environment by pushing to the `develop` branch, which triggers deployment. That approach provides better team visibility. However, there are circumstances where deploying from the command line is necessary; for example if a failing action is preventing deployment.

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

### datagov-harvest
This is a Flask app which manages the configuration of harvest sources, organizations, and the creation of harvest jobs.

#### User management

The Data.gov team are the only intended users of the harvester admin app.

`cf run-task datagov-harvest --name "add new user" --command "flask user add xxx@gsa.gov --name xxx"`

Or, if doing for local development:

`docker compose exec app flask user add your.i.name@gsa.gov --name yourName`

#### Add organizations

You can add organizations using the harvester UI. Alternatively, you can run this command:

`cf run-task datagov-harvest-admin --name "add new org" --command "flask org add 'Name of Org' --log https://some-url.png --id 1234"`
