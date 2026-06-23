import json

from flask import jsonify, make_response, request

from app.api_schemas import (
    ValidationErrorResponseSchema,
    ValidationResultSchema,
    ValidatorInfo,
)
from app.deps import logger
from app.util import fetch_json_from_url, validate_records

from . import api


@api.route("/api/validate", methods=["POST"])
@api.input(ValidatorInfo)
@api.output(ValidationResultSchema, status_code=200)
@api.doc(
    summary="Validate a DCAT catalog against a v1.1 or v3.0 schema",
    description="Downloads or parses a DCATUS catalog and validates each dataset.",
    responses={
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
        elif json_data["fetch_method"] == "paste":
            data = json.loads(json_data["json_text"])
        else:
            data = []

        errors = validate_records(data, json_data["schema"])
        logger.info(
            "API validator completed fetch_method=%s schema=%s validation_errors=%s",
            json_data["fetch_method"],
            json_data["schema"],
            len(errors),
        )
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
