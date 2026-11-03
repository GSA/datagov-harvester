# Largest catalog document accepted, however it arrives: an upload or form
# field on the admin app, or a URL fetch or `json_text` on the validator API.
MAX_UPLOAD_MB = 10
MAX_UPLOAD_BYTES = MAX_UPLOAD_MB * 1024 * 1024

# A small document containing many invalid datasets can expand into millions of
# validation errors. The public API stops before that amplification can exhaust
# a worker; direct/offline callers of validate_records remain uncapped.
MAX_VALIDATION_ERRORS = 1000
MAX_RESULT_IDENTIFIER_CHARS = 1000
MAX_RESULT_MESSAGE_CHARS = 4000

# How deeply a document may nest objects and arrays. Validation recurses: Python
# jsonschema spends several stack frames on every level a schema recurses
# through (3.0's nested catalogs, 1.1's subOrganizationOf chains), so with
# Python's default recursion limit of 1000 it fails with a RecursionError at
# about 250 levels (1.1) or 330 (3.0). 100 keeps attacker-controlled documents
# comfortably below that, independent of the parser and interpreter patch
# version in use, and real catalogs nest far less (8 at most across
# example_data and the GSA/dcat-us examples). orjson's own limit is 1024: it
# parses up to that depth without recursing, and validate.py's post-parse walk
# holds every document to this limit before anything else walks it
# (GSA/data.gov#6460).
MAX_DOCUMENT_NESTING_DEPTH = 100

# Largest validator API request body. Pasted catalogs arrive as a JSON string,
# and encoding one escapes every quote, backslash and control character, so a
# document at the limit can double in size. Leave room for the surrounding API
# fields as well. The document itself is still held to MAX_UPLOAD_BYTES; this
# only keeps that check reachable.
MAX_REQUEST_OVERHEAD_BYTES = 1024
MAX_REQUEST_BYTES = 2 * MAX_UPLOAD_BYTES + MAX_REQUEST_OVERHEAD_BYTES
