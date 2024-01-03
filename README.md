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
