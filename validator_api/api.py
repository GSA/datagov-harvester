import contextlib
import fcntl
import json
import logging
import os
import tempfile

from apiflask import APIBlueprint, Schema, validators
from apiflask.fields import String
from flask import jsonify, make_response, request

from dcatus_validation.catalog_issues import find_catalog_issues, summarize_issues
from dcatus_validation.deadline import ValidationTimedOut, validation_deadline
from dcatus_validation.errors import (
    NESTING_TOO_DEEP_MESSAGE,
    VALIDATION_TIMED_OUT_MESSAGE,
    CatalogTooDeeplyNested,
)
from dcatus_validation.fetch import (
    INVALID_JSON_ENCODING_MESSAGE,
    PAYLOAD_TOO_LARGE_MESSAGE,
    InvalidCatalogSource,
    fetch_json_from_url,
    invalid_json_message,
    parse_pasted_json_document,
)
from dcatus_validation.limits import (
    LARGE_DOCUMENT_BYTES,
    MAX_RESULT_IDENTIFIER_CHARS,
    MAX_RESULT_MESSAGE_CHARS,
    MAX_UPLOAD_BYTES,
    MAX_VALIDATION_ERRORS,
    MAX_VALIDATION_WARNINGS,
    VALIDATION_DEADLINE_SECONDS,
)
from dcatus_validation.places import snapshot_place_lookup
from dcatus_validation.validate import SCHEMA_NAMES, validate_records_limited
from validator_api.schemas import (
    ValidationErrorResponseSchema,
    ValidationResultSchema,
    ValidatorInfo,
)

logger = logging.getLogger("validator_api")

api = APIBlueprint("api", __name__, url_prefix="/api", tag="Validate")

# One file per instance: flock on it is shared by every gunicorn worker and
# thread there, so only one document over LARGE_DOCUMENT_BYTES is parsed and
# validated at a time (GSA/data.gov#6460).
LARGE_DOCUMENT_LOCK_PATH = os.path.join(
    tempfile.gettempdir(), "datagov-validator-large-document.lock"
)
VALIDATOR_BUSY_MESSAGE = (
    "The validator is busy with another large catalog. Please try again in a " "minute."
)


class _ValidatorBusy(Exception):
    pass


def _hold_large_document_slot(stack: contextlib.ExitStack, size: int) -> None:
    """For a document over LARGE_DOCUMENT_BYTES, take this instance's one slot
    until `stack` closes, or raise _ValidatorBusy if another request has it.
    Closing the lock file releases the lock."""
    if size <= LARGE_DOCUMENT_BYTES:
        return
    lock = stack.enter_context(open(LARGE_DOCUMENT_LOCK_PATH, "a"))
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise _ValidatorBusy from None


def _truncate_result_text(value: str, max_chars: int) -> str:
    if len(value) <= max_chars:
        return value
    suffix = "... [truncated]"
    if max_chars <= len(suffix):
        return suffix[:max_chars]
    return f"{value[: max_chars - len(suffix)]}{suffix}"


def _bound_identifier(identifier):
    if isinstance(identifier, str):
        return _truncate_result_text(identifier, MAX_RESULT_IDENTIFIER_CHARS)
    return identifier


def _bound_validation_results(errors: list) -> list:
    return [
        [
            _bound_identifier(identifier),
            _truncate_result_text(message, MAX_RESULT_MESSAGE_CHARS),
        ]
        for identifier, message in errors
    ]


def _bound_warnings(warnings: list) -> list:
    return [
        {
            "identifier": _bound_identifier(w.identifier),
            "record_type": w.record_type,
            "warning_type": w.warning_type,
            "message": _truncate_result_text(w.message, MAX_RESULT_MESSAGE_CHARS),
            "title": (
                _truncate_result_text(w.title, MAX_RESULT_IDENTIFIER_CHARS)
                if w.title is not None
                else None
            ),
        }
        for w in warnings
    ]


def _catalog_issues(data, schema_name: str) -> tuple[list, list, bool]:
    """Duplicate-identifier errors and content warnings, or none (flagged
    incomplete) when the catalog is too malformed to split into records; its
    schema errors already say why."""
    if not isinstance(data, dict):
        return [], [], False
    try:
        return find_catalog_issues(
            data,
            schema_name,
            place_lookup=snapshot_place_lookup,
            max_warnings=MAX_VALIDATION_WARNINGS,
        )
    # What malformed records raise when walked; anything else is a real bug.
    except (AttributeError, TypeError, ValueError, KeyError):
        logger.info("API validator could not split the catalog for warning checks")
        return [], [], True


def _take_pasted_document(json_data) -> bytes:
    """The pasted catalog as UTF-8, with werkzeug's cached copies of the
    request released, so only one copy is alive while it's parsed
    (GSA/data.gov#6460)."""
    document = json_data.pop("json_text").encode("utf-8")
    cache = vars(request._get_current_object())
    cache.pop("_cached_json", None)
    cache.pop("_cached_data", None)
    return document


_RESPONSES = {
    400: {
        "description": (
            "Submission refused: the URL could not be retrieved (bad scheme, "
            "blocked domain, internal address, timeout, too many redirects), "
            "or the catalog "
            "was oversized, not JSON, or unparseable. The `error` field says "
            "which."
        ),
        "content": {"application/json": {"schema": ValidationErrorResponseSchema}},
    },
    413: {
        "description": "Request body too large",
        "content": {"application/json": {"schema": ValidationErrorResponseSchema}},
    },
    422: {
        "description": (
            "Catalog cannot be checked: nested too deeply, or took too long "
            "to check (`error`); or the request itself is invalid "
            "(`message`/`detail`)"
        ),
        "content": {"application/json": {"schema": ValidationErrorResponseSchema}},
    },
    503: {
        "description": (
            "Busy validating another large catalog on this instance; try again"
        ),
        "content": {"application/json": {"schema": ValidationErrorResponseSchema}},
    },
    500: {
        "description": "Failed to download or process the catalog",
        "content": {"application/json": {"schema": ValidationErrorResponseSchema}},
    },
}


def _validation_response(fetch_method: str, schema: str, load_catalog):
    """Load the catalog with load_catalog(hold), validate it, and answer: the
    same for both endpoints. load_catalog calls hold(size) before parsing, so a
    large document holds the instance's slot until the answer is ready."""
    errors = []
    warnings = []
    issue_summary = []
    warnings_incomplete = False

    try:
        with contextlib.ExitStack() as stack:
            stack.enter_context(validation_deadline(VALIDATION_DEADLINE_SECONDS))
            data = load_catalog(lambda size: _hold_large_document_slot(stack, size))
            errors, validation_incomplete = validate_records_limited(
                data,
                schema,
                max_errors=MAX_VALIDATION_ERRORS,
            )
            duplicate_errors, warnings, warnings_incomplete = _catalog_issues(
                data, schema
            )
            del data
        # Duplicates are errors at harvest time (the record is dropped), so
        # they share the error list and its cap.
        schema_error_count = len(errors)
        errors += duplicate_errors
        if len(errors) > MAX_VALIDATION_ERRORS:
            errors = errors[:MAX_VALIDATION_ERRORS]
            validation_incomplete = True
        issue_summary = summarize_issues(
            schema_error_count, len(errors) - schema_error_count, warnings
        )
        errors = _bound_validation_results(errors)
        warnings = _bound_warnings(warnings)
        logger.info(
            "API validator completed fetch_method=%s schema=%s "
            "validation_errors=%s validation_incomplete=%s "
            "validation_warnings=%s warnings_incomplete=%s",
            fetch_method,
            schema,
            len(errors),
            validation_incomplete,
            len(warnings),
            warnings_incomplete,
        )
    except _ValidatorBusy:
        logger.warning(
            "API validator busy with another large document fetch_method=%s",
            fetch_method,
        )
        response = make_response(jsonify({"error": VALIDATOR_BUSY_MESSAGE}), 503)
        response.headers["Retry-After"] = "60"
        return response
    except ValidationTimedOut:
        logger.warning(
            "API validator ran out of time fetch_method=%s schema=%s",
            fetch_method,
            schema,
        )
        return make_response(jsonify({"error": VALIDATION_TIMED_OUT_MESSAGE}), 422)
    except CatalogTooDeeplyNested:
        # Use a constant so exception data cannot reach the client.
        logger.warning("API Validator could not walk the document")
        return make_response(jsonify({"error": NESTING_TOO_DEEP_MESSAGE}), 422)
    except RecursionError:
        # Parsing can exhaust the stack before schema validation.
        logger.warning("API Validator could not parse a deeply nested document")
        return make_response(jsonify({"error": NESTING_TOO_DEEP_MESSAGE}), 422)
    except json.JSONDecodeError as e:
        # Decoder exceptions retain the submitted document; expose position only.
        logger.info(
            "API validator got unparseable pasted JSON line=%s column=%s",
            e.lineno,
            e.colno,
        )
        return make_response(jsonify({"error": invalid_json_message(e)}), 400)
    except InvalidCatalogSource as e:
        # public_message contains only safe literals and positions.
        logger.info(
            "API validator refused submission fetch_method=%s reason=%s",
            fetch_method,
            e.public_message,
        )
        return make_response(jsonify({"error": e.public_message}), 400)
    except Exception as e:
        logger.error("API Validator error error_type=%s", type(e).__name__)
        return make_response(
            jsonify(
                {"error": "API Validator error: failed to validate dcatus catalog"}
            ),
            500,
        )

    result = {
        "validation_errors": errors,
        "validation_warnings": warnings,
        "issue_summary": issue_summary,
    }
    if validation_incomplete:
        result["validation_incomplete"] = True
    if warnings_incomplete:
        result["warnings_incomplete"] = True
    return make_response(jsonify(result), 200)


@api.route("/validate", methods=["POST"])
@api.input(ValidatorInfo)
@api.output(ValidationResultSchema, status_code=200)
@api.doc(
    summary="Validate a DCAT catalog against a v1.1 or v3.0 schema",
    description=(
        "Downloads or parses a DCATUS catalog and validates each dataset. Also "
        "reports what a harvest of the catalog would: duplicate identifiers (as "
        "errors, since the duplicate is skipped) and content-quality warnings, "
        "which never make a catalog invalid."
    ),
    responses=_RESPONSES,
)
def validator(json_data):
    """API route for validating v1.1 or v3.0 dcatus catalogs."""
    if json_data["fetch_method"] == "url":

        def load_catalog(hold):
            return fetch_json_from_url(json_data["url"], before_parse=hold)

    else:

        def load_catalog(hold):
            return _parse_document(_take_pasted_document(json_data), hold)

    return _validation_response(
        json_data["fetch_method"], json_data["schema"], load_catalog
    )


def _parse_document(document, hold):
    """Parse a pasted or uploaded catalog's UTF-8 bytes, held to the same
    document limit a URL submission gets."""
    if len(document) > MAX_UPLOAD_BYTES:
        raise InvalidCatalogSource(PAYLOAD_TOO_LARGE_MESSAGE)
    hold(len(document))
    return parse_pasted_json_document(document)


def _uploaded_file_as_utf8(raw: bytes) -> bytes | memoryview:
    """An uploaded file's text as UTF-8, decoded as the /validate/ page used
    to decode it in the browser: UTF-16 by BOM or NUL pattern, otherwise
    UTF-8, minus one leading BOM."""
    first, second = raw[:1], raw[1:2]
    if raw[:2] == b"\xfe\xff" or (first == b"\0" and second not in (b"", b"\0")):
        encoding = "utf-16-be"
    elif raw[:2] == b"\xff\xfe" or (first not in (b"", b"\0") and second == b"\0"):
        encoding = "utf-16-le"
    else:
        if raw.startswith(b"\xef\xbb\xbf"):
            return memoryview(raw)[3:]
        return raw
    try:
        text = raw.decode(encoding)
    except UnicodeDecodeError:
        raise InvalidCatalogSource(INVALID_JSON_ENCODING_MESSAGE) from None
    return text.removeprefix("\ufeff").encode("utf-8")


class DocumentQuery(Schema):
    schema = String(required=True, validate=validators.OneOf(SCHEMA_NAMES))
    fetch_method = String(
        load_default="upload", validate=validators.OneOf(["paste", "upload"])
    )


@api.route("/validate/document", methods=["POST"])
@api.input(DocumentQuery, location="query")
@api.output(ValidationResultSchema, status_code=200)
@api.doc(
    summary="Validate a DCAT catalog sent as the request body",
    description=(
        "The request body is the catalog; `schema` and `fetch_method` are "
        "query parameters. `paste` reads the body as UTF-8 text, `upload` as "
        "a UTF-8 or UTF-16 file. Same results as `/validate` with `json_text`."
    ),
    responses=_RESPONSES,
)
def validate_document(query_data):
    """The catalog as the raw request body (GSA/data.gov#6460)."""
    # Read outside _validation_response so an oversized body gets the JSON 413,
    # not a 500; a list so load_catalog holds the only reference.
    body = [request.get_data(cache=False)]
    fetch_method = query_data["fetch_method"]

    def load_catalog(hold):
        raw = body.pop()
        if len(raw) > MAX_UPLOAD_BYTES:
            raise InvalidCatalogSource(PAYLOAD_TOO_LARGE_MESSAGE)
        if fetch_method == "upload":
            raw = _uploaded_file_as_utf8(raw)
        return _parse_document(raw, hold)

    return _validation_response(fetch_method, query_data["schema"], load_catalog)
