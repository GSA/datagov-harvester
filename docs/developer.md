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

## Alert banners

Every `flash()` call passes an explicit category, and
`app/templates/components/alert.j2` is the only place that maps a category to
a USWDS alert class:

| Category  | Color  | Use for                                                    |
|-----------|--------|------------------------------------------------------------|
| `success` | green  | the requested action completed                             |
| `warning` | yellow | partial success, a non-fatal problem, or nothing changed   |
| `error`   | red    | the action failed, or the input or link was invalid        |
| `info`    | blue   | neutral information that reports no action outcome         |

`LoadManager` methods that report to the UI (`trigger_manual_job`,
`start_job`, `stop_job`) return a `(message, category)` tuple, so callers can
flash the message without parsing it.

## DCAT-US 3.0 schemas

The DCAT-US 3.0 JSON Schemas are **not in this repo**. They come from
[GSA/dcat-us](https://github.com/GSA/dcat-us), tracked as a git submodule at
`_external/dcat-us`. Paths are defined once, in
[`dcatus_validation/schema_paths.py`](../dcatus_validation/schema_paths.py) — import
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

It may also hold the optional `HARVEST_API_CLIENTS`; see [API client credentials](#api-client-credentials).

#### API client credentials

`HARVEST_API_TOKEN` is one key shared by every API caller, so its requests are recorded as `user=<api_token>`. A caller (a script, a job, a team) can instead get its own key in `HARVEST_API_CLIENTS`. Requests made with that key are recorded as:

- the authentication line: `event=user_login method=api_client user=api_client:<client-id> client_id=<client-id> ip=... path=...`
- each existing `Audit <action> <entity> ...` mutation line: `auth_type=api_client user=api_client:<client-id>`
- `harvest_source.archived_by` when the client archives a source: `api_client:<client-id>`

Shared-token requests keep logging `method=api_token user=<api_token>` and `auth_type=api_token`, so the two are distinguishable.

**What this does not change**

- *Authorization.* A named key is accepted everywhere the shared token is: every route protected by `login_required`, browser pages included. Keys are not scoped to particular routes, organizations or actions; a named key only changes how requests are attributed.
- *Audit coverage.* Attribution reaches only the places that already write an `Audit` line: organization create/edit/delete, harvest source create/edit/lock/unlock/archive/unarchive, harvest job create/update/delete, harvest record create, and dataset slug edits. Triggering a harvest, force-harvest, clear or validate job (`/api/v1/harvest_source/harvest/<id>/<type>`, and the matching buttons in the UI) and cancelling a job (`/api/v1/harvest_job/cancel/<id>`) write no `Audit` line. For those, the `event=user_login` line records which client called which path, but not the outcome.

**Configuration format**

In the `datagov-harvest-secrets` service, `HARVEST_API_CLIENTS` must be a JSON-encoded *string* whose content is a JSON object mapping each client ID to the SHA-256 digest of its key, or to a list of digests:

```json
{
  "HARVEST_API_CLIENTS": "{\"ckan-migration\": \"sha256:609e4f553f5d9e79cbad75f1425f73879a77fa4d8699aa9523b9492994fdb605\"}"
}
```

`.profile` stops the app from starting if the value is a native JSON object, array, number or boolean. Converting a native object back to text would silently drop a repeated client ID, so only strings are accepted, and the app can check them itself. These all mean "no named clients": the key is absent; its value is JSON `null`; or it's a string that is empty, only whitespace, or `null`, `none`, `nil` or `undefined` in any letter case. The same values count as unset when `HARVEST_API_CLIENTS` is set directly in the environment, as in `.env`.

The app also refuses to start if the string isn't a JSON object, repeats a client ID, has an invalid client ID or a malformed digest, gives the same key to two entries, or reuses the `HARVEST_API_TOKEN` key. Startup errors name the problem and, for a valid client ID, the ID. They are written to leave out keys and digests, and the tests check this for each kind of rejection.

- Client IDs are 1–64 lowercase letters, digits, `.`, `_` or `-`, starting and ending with a letter or digit. They are logged, so use a descriptive name, not an email address or anything secret.
- Callers keep sending the key in `X-API-Key`. The app takes the caller's identity only from which configured key matched, not from any other header.
- Locally, `.env` takes the same JSON object as a plain value; see `.env.sample`.

**Key requirements**

Keys must be generated by a cryptographically secure random generator with at least 256 bits of entropy, as the command below does. Don't use a human-chosen password or phrase. Digests are unsalted SHA-256. That is acceptable only for keys like these, because guessing a 256-bit random key from its digest is computationally infeasible with known methods. A human-chosen key could be found by hashing likely guesses. Treat the digests as sensitive configuration anyway. Keep them in the secrets service, and don't put them in tickets, chat, logs or the repository.

**Provision**

Generate each key and its digest into a private file, not the terminal:

```bash
umask 077
python3 -c 'import hashlib, secrets, sys
k = secrets.token_urlsafe(32)
open(sys.argv[1], "x").write(f"key: {k}\ndigest: sha256:{hashlib.sha256(k.encode()).hexdigest()}\n")' ./new-api-client.txt
```

Give the key to the client owner over an approved secret-sharing channel, and delete the file once the change is complete. An authorized administrator then adds the digest to `HARVEST_API_CLIENTS` in `datagov-harvest-secrets`, following GSA's approved procedure for changing that service. `cf update-user-provided-service` replaces all of a service's credentials, so every existing credential must be carried over unchanged, and the result must keep `HARVEST_API_CLIENTS` as a JSON-encoded string. Don't add a client ID that already exists.

**Apply**

Each app instance reads `HARVEST_API_CLIENTS` once, when it starts. Nothing changes until the instances restart: `cf restart datagov-harvest --strategy rolling`. During a rolling restart, instances still on the old configuration accept the old keys until they are replaced. Revocation takes effect only once every instance has restarted, not when the service is updated.

**Rotate:** list the new digest alongside the old one (`"<id>": ["sha256:<new>", "sha256:<old>"]` in the decoded object), restart, move the client to the new key, then remove the old digest and restart again. Both keys work in between.

**Revoke:** delete the client's entry, update the service, and restart. If a key may be compromised, use a regular restart rather than a rolling one so no old instance keeps serving it, and accept the brief outage.

The shared `HARVEST_API_TOKEN` is still required at startup. Once no caller uses it, retire it by replacing it with a new random value that nobody holds.

CF_SERVICE_* variables can be extracted from from service-keys by running `cf service-key ci-deployer datagov-harvest-deployer` in the appropriate space.

`datagov-harvest-validator` binds only its own `datagov-harvest-validator-secrets`, never `datagov-harvest-secrets`, so the public validator (it takes server-side URL-fetch requests) holds none of the admin app's secrets (GSA/data.gov#6293). It needs just one value; `create_cloudgov_services.sh` creates the service empty if missing, then populate it once per space:

```bash
cf uups datagov-harvest-validator-secrets -p '{"NEW_RELIC_LICENSE_KEY": "<same value as datagov-harvest-secrets>"}'
cf restage datagov-harvest-validator
```

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
This is an nginx app which owns the public route and proxies traffic to the internal Flask app route. It also splits off validator traffic: `/api/validate`, `/api/v1/validate`, the validator's docs at `/validator/docs` and `/validator/openapi.json`, and its health check at `/validator/health` are proxied to `datagov-harvest-validator` instead of `datagov-harvest` (see `proxy/nginx-common.conf` and `proxy/nginx-maps.conf`). Everything else, including the `/validate/` page, goes to `datagov-harvest`.

### datagov-harvest
This is a Flask app which manages the configuration of harvest sources, organizations, and the creation of harvest jobs.

### datagov-harvest-validator
The public DCAT-US validation API, deployed as its own Cloud Foundry app so validation load and its server-side URL fetching can't consume the admin app's capacity, and can be scaled or restarted on their own (GSA/data.gov#6293).

- **Minimal on purpose.** It runs `validator_api/` (`validator-start.sh`), not the admin app: it imports only `dcatus_validation/`, APIFlask and marshmallow, so it needs no database, no admin secrets and no CF credentials. `tests/unit/test_validator_import_clean.py` fails if it ever imports `app`, `harvester` or `database`.
- **One copy of the validation code.** `dcatus_validation/` holds the error-message formatting, the schema paths, `validate_records` and the URL fetcher. The harvest runner imports the same module, so the validator and a harvest job describe a bad record the same way.
- **The `/validate/` page doesn't call it server-side.** `datagov-harvest` only renders the page (GET only); the browser `fetch()`es `/api/v1/validate` same-origin through the proxy (`app/static/js/view_validators.js`), so the admin app's workers are never involved.
- **URL fetches** get one 12s budget, with each connect attempt capped at 4s and retried up to 3 times on a fresh connection (`dcatus_validation/fetch.py`). Private and internal addresses are refused unless `ALLOW_PRIVATE_ADDRESSES=true` (local `docker compose` only).
- Its docs are at `/validator/docs`, linked from the admin app's `/openapi/docs`.
- **Health check:** `/health` on the internal route (Cloud Foundry's health check) and `/validator/health` through the proxy (uptime monitoring). Every schema validator is built at startup, so an instance with missing schemas fails to start rather than reporting healthy.
- **Security:** the SSRF protections, resource limits and rules for changing this code are in [validator-security.md](validator-security.md).
- Has its own `validator_instances`/`validator_memory_quota`/`validator_disk_quota` vars per space, separate from `admin_*`. gunicorn runs 3 workers × 2 threads per instance.

Locally, `docker compose` runs it as the `validator` service, plus a `proxy` service (`nginx-local-proxy.conf`, port 8082) that reproduces the cloud.gov routing and body limits, since the Flask dev server has no nginx in front of it. `/validate/` only works end to end through port 8082.

#### Validator egress proxy

The validator fetches through its own [cg-egress-proxy](https://github.com/GSA-TTS/cg-egress-proxy), separate from the admin app's. It allows every domain (the admin app's proxy uses the per-space allow list) and HTTPS (port 443) only; the egress space's security groups keep it off private networks. Like the harvester, the validator picks the proxy up from `https_proxy`/`http_proxy`, which `.profile` sets from `proxy_url`; with it unset, fetches go direct.

Its lists live in GSA/data.gov (`egress/acl/datagov-harvest-validator.<space>.allow.acl` and `.deny.acl`, explained in that repo's `egress/README.md`). It's deployed per space by that repo's "enable egress proxy" workflow: choose `datagov-harvest-validator` and the space. The workflow runs `cf-deployproxy`, which:
- pushes `proxy-gsa-datagov-<space>-datagov-harvest-validator` to `<space>-egress`;
- adds the network policy from the validator to it;
- sets `proxy_url` on the validator and restarts it.

Re-running it rotates the proxy credentials. "disable egress proxy" undoes it, after which the validator fetches directly.

To check it: the validator logs `egress=proxy` for each fetch, and the proxy app's logs show each request it allowed or refused (`cf logs proxy-gsa-datagov-<space>-datagov-harvest-validator` in `<space>-egress`).

#### User management

The Data.gov team are the only intended users of the harvester admin app. Since users are rarely added or deleted, no user management UI has been built and adding/deleting users must be done via the command line.

To add a user:

`cf run-task datagov-harvest --name "add new user" --command "flask user add xxx@gsa.gov --name xxx"`

Or, if doing for local development:

`docker compose exec app flask user add your.i.name@gsa.gov --name yourName`

To delete a user:

`cf run-task datagov-harvest --command "flask user remove xxx@gsa.gov"`

#### Add organizations

You can add organizations using the harvester UI. Alternatively, you can run this command:

`cf run-task datagov-harvest-admin --name "add new org" --command "flask org add 'Name of Org' --log https://some-url.png --id 1234"`
