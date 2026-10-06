import json
import logging

from apiflask import APIBlueprint
from flask import jsonify, make_response

from dcatus_validation.errors import (
    NESTING_TOO_DEEP_MESSAGE,
    CatalogTooDeeplyNested,
)
from dcatus_validation.fetch import (
    PAYLOAD_TOO_LARGE_MESSAGE,
    InvalidCatalogSource,
    fetch_json_from_url,
    invalid_json_message,
    parse_json_document,
)
from dcatus_validation.limits import (
    MAX_RESULT_IDENTIFIER_CHARS,
    MAX_RESULT_MESSAGE_CHARS,
    MAX_UPLOAD_BYTES,
    MAX_VALIDATION_ERRORS,
)
from dcatus_validation.validate import validate_records_limited
from validator_api.schemas import (
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


@api.route("/validate", methods=["POST"])
@api.input(ValidatorInfo)
@api.output(ValidationResultSchema, status_code=200)
@api.doc(
    summary="Validate a DCAT catalog against a v1.1 or v3.0 schema",
    description="Downloads or parses a DCATUS catalog and validates each dataset.",
    responses={
        400: {
            "description": (
                "Submission refused: the URL could not be retrieved (bad scheme, "
                "internal address, timeout, too many redirects), or the catalog "
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
    },
)
def validator(json_data):
    """API route for validating v1.1 or v3.0 dcatus catalogs."""
    errors = []

    try:
        if json_data["fetch_method"] == "url":
            data = fetch_json_from_url(json_data["url"])
        else:
            json_text = json_data["json_text"]
            # MAX_CONTENT_LENGTH caps the request body, which has room for JSON
            # escaping (see app.constants); this caps the document itself, the
            # same limit a URL submission gets.
            if len(json_text.encode("utf-8")) > MAX_UPLOAD_BYTES:
                raise InvalidCatalogSource(PAYLOAD_TOO_LARGE_MESSAGE)
            data = parse_json_document(json_text)

        errors, validation_incomplete = validate_records_limited(
            data,
            json_data["schema"],
            max_errors=MAX_VALIDATION_ERRORS,
        )
        errors = _bound_validation_results(errors)
        logger.info(
            "API validator completed fetch_method=%s schema=%s "
            "validation_errors=%s validation_incomplete=%s",
            json_data["fetch_method"],
            json_data["schema"],
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
            json_data["fetch_method"],
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
