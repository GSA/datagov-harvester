import pytest

from harvester.utils.general_utils import (
    group_record_error_fields,
    parse_validation_message,
)


class TestParseValidationMessage:
    @pytest.mark.parametrize(
        "message,expected_field,expected_rule",
        [
            (
                "<ValidationError: \"$.license, 'center' does not match any of "
                "the acceptable formats: 'uri', 'null', "
                "'^(\\[\\[REDACTED).*?(\\]\\])$'\">",
                "license",
                "'uri', 'null', '^(\\[\\[REDACTED).*?(\\]\\])$'",
            ),
            (
                '<ValidationError: "$.distribution[0].accessURL, '
                "'//////not-a-url.example.com/' does not match any of the "
                "acceptable formats: 'uri', 'null', "
                "'^(\\[\\[REDACTED).*?(\\]\\])$'\">",
                "distribution[].accessURL",
                "'uri', 'null', '^(\\[\\[REDACTED).*?(\\]\\])$'",
            ),
            (
                '<ValidationError: "$.modified, None does not match any of '
                "the acceptable formats: 'string'\">",
                "modified",
                "'string'",
            ),
            (
                "<ValidationError: \"$, 'contactPoint' is a required property\">",
                "(root)",
                "required property",
            ),
        ],
    )
    def test_validation_error_messages(self, message, expected_field, expected_rule):
        field, rule = parse_validation_message(message)
        assert field == expected_field
        assert rule == expected_rule

    def test_single_quoted_outer_wrapper(self):
        # ruff: noqa: E501
        message = (
            "<ValidationError: '$.contactPoint.hasEmail, "
            "\\'ocagoadmin@oakgov.com\\' does not match any of the acceptable "
            "formats: \"^mailto:[\\w\\_\\~\\!\\$\\&\\'\\(\\)\\*\\+\\,\\;\\=\\:.-]+@"
            "[\\w.-]+\\.[\\w.-]+?$\", \\'^(\\[\\[REDACTED).*?(\\]\\])$\\''>"
        )
        field, rule = parse_validation_message(message)
        assert field == "contactPoint.hasEmail"
        assert "mailto" in rule

    def test_non_validation_message_returns_none_field(self):
        field, rule = parse_validation_message("some transformation failure")
        assert field is None
        assert rule == "some transformation failure"

    def test_validation_exception_wrapper(self):
        message = "<ValidationException: \"$.title, 'foo' does not match any of the acceptable formats: 'string'\">"
        field, rule = parse_validation_message(message)
        assert field == "title"
        assert rule == "'string'"


class TestGroupRecordErrorFields:
    def test_groups_validation_errors_by_field(self):
        rows = [
            (
                "error",
                "ValidationError",
                "<ValidationError: \"$.license, 'center' does not match any of "
                "the acceptable formats: 'uri'\">",
                3,
            ),
            (
                "error",
                "ValidationError",
                "<ValidationError: \"$.license, 'other' does not match any of "
                "the acceptable formats: 'uri'\">",
                2,
            ),
        ]
        summary = group_record_error_fields(rows)
        assert len(summary) == 1
        assert summary[0]["field"] == "license"
        assert summary[0]["count"] == 5
        assert len(summary[0]["examples"]) == 2

    def test_non_validation_errors_group_by_type(self):
        rows = [("warning", "SomeDcatWarning", "plain warning text", 4)]
        summary = group_record_error_fields(rows)
        assert summary[0]["field"] is None
        assert summary[0]["rule"] == "SomeDcatWarning"
        assert summary[0]["severity"] == "warning"

    def test_sorted_by_severity_then_count(self):
        rows = [
            ("warning", "TypeA", "warning message", 100),
            ("error", "TypeB", "error message", 1),
        ]
        summary = group_record_error_fields(rows)
        assert summary[0]["severity"] == "error"
        assert summary[1]["severity"] == "warning"
