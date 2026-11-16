from unittest.mock import Mock

import pytest

from harvester.utils.general_utils import (
    build_report_email_section,
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


class TestBuildReportEmailSection:
    def test_no_errors_or_datasets(self):
        section = build_report_email_section([], [], "")
        assert "No record errors or warnings found." in section
        assert "No datasets have been harvested from this source yet." in section

    def test_lists_errors_and_datasets_with_catalog_link(self):
        error_field_summary = [
            {"severity": "error", "field": "license", "rule": "'uri'", "count": 3},
            {"severity": "warning", "field": None, "rule": "SomeWarning", "count": 1},
        ]
        dataset = Mock(slug="fixture-dataset-1")

        section = build_report_email_section(
            error_field_summary, [dataset], "https://catalog.data.gov"
        )

        assert "- error: license - 'uri' (3)" in section
        assert "- warning: (root) - SomeWarning (1)" in section
        assert (
            "- fixture-dataset-1: https://catalog.data.gov/dataset/fixture-dataset-1"
            in section
        )

    def test_lists_dataset_slug_without_catalog_link_when_not_configured(self):
        dataset = Mock(slug="fixture-dataset-1")
        section = build_report_email_section([], [dataset], "")
        assert "- fixture-dataset-1" in section
        assert "http" not in section


class TestReportRecipientsDisplay:
    def test_renders_single_recipient(self):
        from harvester.utils.harvest_job_email import render_harvest_job_email_html

        html = render_harvest_job_email_html(
            title="Test Report",
            introduction="Test intro",
            source_name="Test Source",
            job_id="test-job-123",
            job_url="http://example.com/job/123",
            refresh_url="http://example.com/refresh",
            notification_emails=["admin@example.com"],
        )

        assert "Report recipients" in html
        assert "admin@example.com" in html

    def test_renders_multiple_recipients(self):
        from harvester.utils.harvest_job_email import render_harvest_job_email_html

        html = render_harvest_job_email_html(
            title="Test Report",
            introduction="Test intro",
            source_name="Test Source",
            job_id="test-job-123",
            job_url="http://example.com/job/123",
            refresh_url="http://example.com/refresh",
            notification_emails=["admin@example.com", "data@example.com"],
        )

        assert "Report recipients" in html
        assert "admin@example.com, data@example.com" in html

    def test_renders_no_recipients_as_none(self):
        from harvester.utils.harvest_job_email import render_harvest_job_email_html

        html = render_harvest_job_email_html(
            title="Test Report",
            introduction="Test intro",
            source_name="Test Source",
            job_id="test-job-123",
            job_url="http://example.com/job/123",
            refresh_url="http://example.com/refresh",
            notification_emails=[],
        )

        assert "Report recipients" in html
        assert "None" in html

    def test_renders_null_recipients_as_none(self):
        from harvester.utils.harvest_job_email import render_harvest_job_email_html

        html = render_harvest_job_email_html(
            title="Test Report",
            introduction="Test intro",
            source_name="Test Source",
            job_id="test-job-123",
            job_url="http://example.com/job/123",
            refresh_url="http://example.com/refresh",
            notification_emails=None,
        )

        assert "Report recipients" in html
        assert "None" in html

    def test_plain_text_section_includes_recipients(self):
        from harvester.utils.general_utils import format_recipient_list

        recipients = ["admin@example.com", "data@example.com"]
        formatted = format_recipient_list(recipients)

        assert formatted == "admin@example.com, data@example.com"

    def test_plain_text_section_handles_empty_list(self):
        from harvester.utils.general_utils import format_recipient_list

        formatted = format_recipient_list([])

        assert formatted == "None"

    def test_plain_text_section_handles_none(self):
        from harvester.utils.general_utils import format_recipient_list

        formatted = format_recipient_list(None)

        assert formatted == "None"

    def test_plain_text_section_filters_empty_strings(self):
        from harvester.utils.general_utils import format_recipient_list

        recipients = ["admin@example.com", "", "data@example.com", None]
        formatted = format_recipient_list(recipients)

        assert formatted == "admin@example.com, data@example.com"

    def test_plain_text_section_handles_all_empty_strings(self):
        from harvester.utils.general_utils import format_recipient_list

        recipients = ["", "", None]
        formatted = format_recipient_list(recipients)

        assert formatted == "None"
