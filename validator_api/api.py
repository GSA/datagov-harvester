import json
import logging

from apiflask import APIBlueprint
from flask import jsonify, make_response, request

from dcatus_validation.errors import (
    NESTING_TOO_DEEP_MESSAGE,
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
    MAX_RESULT_IDENTIFIER_CHARS,
    MAX_RESULT_MESSAGE_CHARS,
    MAX_UPLOAD_BYTES,
    MAX_VALIDATION_ERRORS,
)
from dcatus_validation.validate import validate_records_limited
from validator_api.schemas import (
    DocumentQuery,
    ValidationErrorResponseSchema,
    ValidationResultSchema,
    ValidatorInfo,
)

logger = logging.getLogger("validator_api")

api = APIBlueprint("api", __name__, url_prefix="/api", tag="Validate")


def _truncate_result_text(value: str, max_chars: int) -> str:
    if len(value) <= max_chars:
        return value
    suffix = "... [truncated]"
    if max_chars <= len(suffix):
        return suffix[:max_chars]
    return f"{value[: max_chars - len(suffix)]}{suffix}"


def _bound_validation_results(errors: list) -> list:
    bounded = []
    for identifier, message in errors:
        if isinstance(identifier, str):
            identifier = _truncate_result_text(
                identifier,
                MAX_RESULT_IDENTIFIER_CHARS,
            )
        bounded.append(
            [
                identifier,
                _truncate_result_text(message, MAX_RESULT_MESSAGE_CHARS),
            ]
        )
    return bounded


def _take_pasted_document(json_data) -> bytes:
    """The pasted catalog as UTF-8, with every other copy of it released: the
    decoded text (in json_data and werkzeug's parsed-JSON cache) and the raw
    request body werkzeug caches. Otherwise a 150MB paste is alive three or
    four times while it's parsed, ~300-600MB more at the peak
    (GSA/data.gov#6460). Nothing reads the request body again after this.

    Encoding raises UnicodeEncodeError for a lone surrogate, as the size
    check always has.
    """
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
            "Catalog cannot be walked, e.g. nested too deeply (`error`), or "
            "the request itself is invalid (`message`/`detail`)"
        ),
        "content": {"application/json": {"schema": ValidationErrorResponseSchema}},
    },
    500: {
        "description": "Failed to download or process the catalog",
        "content": {"application/json": {"schema": ValidationErrorResponseSchema}},
    },
}


def _validation_response(fetch_method: str, schema: str, load_catalog):
    """Load the catalog with load_catalog(), validate it, and answer. Both
    request formats share this, so they give the same results, refusals and
    statuses."""
    errors = []

    try:
        data = load_catalog()
        errors, validation_incomplete = validate_records_limited(
            data,
            schema,
            max_errors=MAX_VALIDATION_ERRORS,
        )
        errors = _bound_validation_results(errors)
        logger.info(
            "API validator completed fetch_method=%s schema=%s "
            "validation_errors=%s validation_incomplete=%s",
            fetch_method,
            schema,
            len(errors),
            validation_incomplete,
        )
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

    result = {"validation_errors": errors}
    if validation_incomplete:
        result["validation_incomplete"] = True
    return make_response(jsonify(result), 200)


@api.route("/validate", methods=["POST"])
@api.input(ValidatorInfo)
@api.output(ValidationResultSchema, status_code=200)
@api.doc(
    summary="Validate a DCAT catalog against a v1.1 or v3.0 schema",
    description="Downloads or parses a DCATUS catalog and validates each dataset.",
    responses=_RESPONSES,
)
def validator(json_data):
    """API route for validating v1.1 or v3.0 dcatus catalogs."""
    if json_data["fetch_method"] == "url":

        def load_catalog():
            return fetch_json_from_url(json_data["url"])

    else:

        def load_catalog():
            return _parse_document(_take_pasted_document(json_data))

    return _validation_response(
        json_data["fetch_method"], json_data["schema"], load_catalog
    )


def _parse_document(document):
    """Parse a pasted or uploaded catalog's UTF-8 bytes. MAX_CONTENT_LENGTH
    caps the request body, which has room for JSON escaping (see
    app.constants); this caps the document itself, the same limit a URL
    submission gets."""
    if len(document) > MAX_UPLOAD_BYTES:
        raise InvalidCatalogSource(PAYLOAD_TOO_LARGE_MESSAGE)
    return parse_pasted_json_document(document)


def _uploaded_file_as_utf8(raw: bytes) -> bytes | memoryview:
    """An uploaded file's text as UTF-8, decoded the way the /validate/ page
    decoded it in the browser before it sent files as the body itself: UTF-16
    when the file starts with a UTF-16 BOM or has a NUL in exactly one of its
    first two bytes, otherwise UTF-8, without one leading BOM. A file that
    doesn't decode is refused, as the page refused it. UTF-8 files, nearly
    all of them, aren't copied."""
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


@api.route("/validate/document", methods=["POST"])
@api.input(DocumentQuery, location="query")
@api.output(ValidationResultSchema, status_code=200)
@api.doc(
    summary="Validate a DCAT catalog sent as the request body",
    description=(
        "The catalog itself is the request body; `schema` and `fetch_method` "
        "are query parameters. `fetch_method=paste` takes the body as pasted "
        "text in UTF-8 (a leading byte order mark is refused, as in a paste). "
        "`fetch_method=upload` decodes it as a file: UTF-16 by its byte order "
        "mark or NULs, otherwise UTF-8. This is how the /validate/ page sends "
        "catalogs: nothing is JSON-escaped, so neither the browser nor the "
        "service holds an extra copy. Same results and refusals as "
        "`/validate` with `json_text`."
    ),
    responses=_RESPONSES,
)
def validate_document(query_data):
    """The catalog as the raw request body (GSA/data.gov#6460)."""
    # Read here, outside _validation_response, so an oversized body gets the
    # app's JSON 413 rather than a 500. Held in a list so load_catalog can
    # take the only reference and the body is freed once parsed.
    body = [request.get_data(cache=False)]
    fetch_method = query_data["fetch_method"]

    def load_catalog():
        raw = body.pop()
        if len(raw) > MAX_UPLOAD_BYTES:
            raise InvalidCatalogSource(PAYLOAD_TOO_LARGE_MESSAGE)
        if fetch_method == "upload":
            raw = _uploaded_file_as_utf8(raw)
        return _parse_document(raw)

    return _validation_response(fetch_method, query_data["schema"], load_catalog)
