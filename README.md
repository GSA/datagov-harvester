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

## Requirements

This project is using poetry to manage this project. Install [here](https://python-poetry.org/docs/#installation).

Once installed, `poetry install` installs dependencies into a local virtual environment.

Tests are run using docker `docker compose up -d` and with the command
`poetry run pytest`.
