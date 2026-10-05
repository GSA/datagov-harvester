"""DCAT-US validation shared by the harvest runner and the validator API.

Import-clean on purpose: only the stdlib, jsonschema, referencing and requests,
never `app`, `harvester` or `database`, so `validator_api` can run with no
database, secrets or admin app (enforced by tests/unit/test_validator_import_clean.py).
"""
