# Largest catalog document the validator accepts, however it arrives: a URL
# fetch or `json_text` (which is also how the /validate/ page sends an uploaded
# file). 150MB per GSA/data.gov#6460: big enough for nearly every agency
# catalog without sizing the service for the largest outlier (a ~600MB Census
# file). It sets the validator's memory need: a 150MB catalog peaks at roughly
# 1GB in the worker validating it (validator_memory_quota in vars.*.yml). Keep
# client_max_body_size in proxy/nginx-common.conf (and nginx-local-proxy.conf)
# above MAX_REQUEST_BYTES when changing it.
MAX_UPLOAD_MB = 150
MAX_UPLOAD_BYTES = MAX_UPLOAD_MB * 1024 * 1024

# A small document containing many invalid datasets can expand into millions of
# validation errors. The public API stops before that amplification can exhaust
# a worker; direct/offline callers of validate_records remain uncapped.
MAX_VALIDATION_ERRORS = 1000
MAX_RESULT_IDENTIFIER_CHARS = 1000
MAX_RESULT_MESSAGE_CHARS = 4000

# Keep attacker-controlled documents comfortably below Python's recursion
# limit, independent of the parser and interpreter patch version in use.
MAX_DOCUMENT_NESTING_DEPTH = 100

# Largest validator API request body. Pasted catalogs arrive as a JSON string,
# and encoding one escapes every quote, backslash and control character, so a
# document at the limit can double in size. Leave room for the surrounding API
# fields as well. The document itself is still held to MAX_UPLOAD_BYTES; this
# only keeps that check reachable.
MAX_REQUEST_OVERHEAD_BYTES = 1024
MAX_REQUEST_BYTES = 2 * MAX_UPLOAD_BYTES + MAX_REQUEST_OVERHEAD_BYTES
