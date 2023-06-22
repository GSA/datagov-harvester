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

Install poetry

```bash
curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/install-poetry.py | python -

poetry new <project_name>

poetry add xxx

poetry install

```
