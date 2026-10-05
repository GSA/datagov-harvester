import json
import logging

from apiflask import APIBlueprint
from flask import jsonify, make_response

from dcatus_validation.fetch import (
    PAYLOAD_TOO_LARGE_MESSAGE,
    InvalidCatalogSource,
    fetch_json_from_url,
    invalid_json_message,
)
from dcatus_validation.limits import MAX_UPLOAD_BYTES
from dcatus_validation.validate import (
    NESTING_TOO_DEEP_MESSAGE,
    CatalogTooDeeplyNested,
    validate_records,
)
from validator_api.schemas import (
    ValidationErrorResponseSchema,
    ValidationResultSchema,
    ValidatorInfo,
)

logger = logging.getLogger("validator_api")

api = APIBlueprint("api", __name__, url_prefix="/api", tag="Validate")


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
            data = json.loads(json_text)

        errors = validate_records(data, json_data["schema"])
        logger.info(
            "API validator completed fetch_method=%s schema=%s validation_errors=%s",
            json_data["fetch_method"],
            json_data["schema"],
            len(errors),
        )
    except CatalogTooDeeplyNested as e:
        # the submitter can act on this one, so say what it was. Respond with the
        # constant rather than `str(e)` so nothing from the exception reaches the
        # client (CodeQL py/stack-trace-exposure).
        logger.warning(f"API Validator could not walk the document :: {repr(e)}")
        return make_response(jsonify({"error": NESTING_TOO_DEEP_MESSAGE}), 422)
    except json.JSONDecodeError as e:
        # Pasted text that won't parse. Reported by position only - never
        # str(e), which would put the decoder's own text (and reachable from
        # the same object, the whole document) into the response. See
        # invalid_json_message.
        logger.info("API validator got unparseable pasted JSON :: %s", repr(e))
        return make_response(jsonify({"error": invalid_json_message(e)}), 400)
    except InvalidCatalogSource as e:
        # Bad submission, not a server fault - 400, and say which reason so
        # callers can show it to the submitter. Safe to echo: these messages
        # are literals and numbers by construction, which is the invariant
        # InvalidCatalogSource exists to carry.
        logger.info(
            "API validator refused submission fetch_method=%s reason=%s",
            json_data["fetch_method"],
            e,
        )
        return make_response(jsonify({"error": str(e)}), 400)
    except Exception as e:
        logger.error(f"API Validator error :: {repr(e)}")
        return make_response(
            jsonify(
                {"error": "API Validator error: failed to validate dcatus catalog"}
            ),
            500,
        )

    return make_response(
        jsonify({"validation_errors": errors}),
        200,
    )
