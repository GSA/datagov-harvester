# datagov-harvesting-logic

This is a library that will be utilized for metadata extraction, validation,
transformation, and loading into the data.gov catalog.

## Features

The datagov-harvesting-logic offers the following features:

- Extract
  - general purpose fetching and downloading of web resources.
  - catered extraction to the following data formats:
    - DCAT-US
- Validation
  - DCAT-US
    - jsonschema validation using draft 2020-12.
- Load
  - DCAT-US
    - conversion of dcatu-us catalog into ckan dataset schema
    - create, delete, update, and patch of ckan package/dataset

## Requirements

This project is using poetry to manage this project. Install [here](https://python-poetry.org/docs/#installation).

Once installed, `poetry install` installs dependencies into a local virtual environment.

## Testing
### CKAN load testing
- CKAN load testing doesn't require the services provided in the `docker-compose.yml`.
- [catalog-dev](https://catalog-dev.data.gov/) is used for ckan load testing.
- Create an api-key by signing into catalog-dev. 
- Create a `credentials.py` file at the root of the project containing the variable `ckan_catalog_dev_api_key` assigned to the api-key.
- run tests with the command `poetry run pytest ./tests/load/ckan`
### Harvester testing
- These tests are found in `extract`, and `validate`. Some of them rely on services in the `docker-compose.yml`. run using docker `docker compose up -d` and with the command `poetry run pytest --ignore=./tests/load/ckan`. 

If you followed the instructions for `CKAN load testing` and `Harvester testing` you can simply run `poetry run pytest` to run all tests.


## Comparison 
- ./tests/harvest_sources/ckan_datasets_resp.json
    - represents what ckan would respond with after querying for the harvest source name
- ./tests/harvest_sources/dcatus_compare.json
    - represents a changed harvest source
    - what has been created?
        - datasets[0] 
            - "identifier" = "cftc-dc10"
    - what has been deleted? 
        - datasets[0]
            - "identifier" = "cftc-dc1"
    - what has been updated? 
        - datasets[1]
            - from "modified": "R/P1M" to "modified": "R/P1M Update"
        - datasets[2] 
            - from "keyword": ["cotton on call", "cotton on-call"]
            - to "keyword": ["cotton on call", "cotton on-call", "update keyword"]
        - datasets[3] 
            - from "publisher": {
                "name": "U.S. Commodity Futures Trading Commission",
                "subOrganizationOf": {
                    "name": "U.S. Government"
                }
            }
            - to "publisher": {
                "name": "U.S. Commodity Futures Trading Commission",
                "subOrganizationOf": {
                    "name": "Changed Value"
                }
            }
- ./test/harvest_sources/dcatus.json 
    - represents an original harvest source prior to change occuring.