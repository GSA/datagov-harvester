# Largest catalog document the validator accepts, however it arrives: a URL
# fetch, `json_text`, or a /validate/document body (GSA/data.gov#6460). A
# 150MB catalog peaks near 1GB in the worker validating it (see
# validator_memory_quota in vars.*.yml). Keep client_max_body_size in
# proxy/nginx-common.conf (and nginx-local-proxy.conf) above MAX_REQUEST_BYTES.
MAX_UPLOAD_MB = 150
MAX_UPLOAD_BYTES = MAX_UPLOAD_MB * 1024 * 1024

# Documents bigger than this are parsed and validated one at a time per
# instance: a 150MB one peaks near 1GB, so two at once could exceed
# validator_memory_quota and get the instance restarted. Another one gets a
# 503 while one runs (validator_api.api).
LARGE_DOCUMENT_BYTES = 50 * 1024 * 1024

# A small document containing many invalid datasets can expand into millions of
# validation errors. The public API stops before that amplification can exhaust
# a worker; direct/offline callers of validate_records remain uncapped.
MAX_VALIDATION_ERRORS = 1000
MAX_RESULT_IDENTIFIER_CHARS = 1000
MAX_RESULT_MESSAGE_CHARS = 4000

# Keep attacker-controlled documents comfortably below Python's recursion
# limit, independent of the parser and interpreter patch version in use.
# Checked after parsing, by validate.py's walk; json.loads raises RecursionError,
# reported the same way, for a document too deep to parse (GSA/data.gov#6460).
MAX_DOCUMENT_NESTING_DEPTH = 100

# Largest validator API request body. Pasted catalogs arrive as a JSON string,
# and encoding one escapes every quote, backslash and control character, so the
# body is bigger than the document: 1.01-1.23x for real catalogs. 1.5x covers
# that and the surrounding API fields. The document itself is still held to
# MAX_UPLOAD_BYTES; this only keeps that check reachable.
MAX_REQUEST_OVERHEAD_BYTES = 1024
MAX_REQUEST_BYTES = MAX_UPLOAD_BYTES * 3 // 2 + MAX_REQUEST_OVERHEAD_BYTES
