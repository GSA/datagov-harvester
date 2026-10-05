# Largest catalog document accepted, however it arrives: an upload or form
# field on the admin app, or a URL fetch or `json_text` on the validator API.
MAX_UPLOAD_MB = 10
MAX_UPLOAD_BYTES = MAX_UPLOAD_MB * 1024 * 1024

# Largest validator API request body. Pasted catalogs arrive as a JSON string,
# and encoding one escapes every quote, backslash and control character, so a
# document at the limit makes a body well over it. The document itself is still
# held to MAX_UPLOAD_BYTES; this only keeps that check reachable.
MAX_REQUEST_BYTES = 2 * MAX_UPLOAD_BYTES
