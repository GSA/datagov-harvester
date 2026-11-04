from app.deps import format_validation_messages


class TestFormatValidationMessages:
    """The API's validation errors are flattened into one "error" string."""

    def test_single_field(self):
        assert (
            format_validation_messages({"slug": ["Slug must be unique."]})
            == "slug: Slug must be unique."
        )

    def test_several_fields_and_messages(self):
        assert (
            format_validation_messages(
                {"slug": ["First.", "Second."], "code_repo_url": ["Third."]}
            )
            == "slug: First.; slug: Second.; code_repo_url: Third."
        )

    def test_schema_level_errors_have_no_prefix(self):
        assert (
            format_validation_messages({"_schema": ["Invalid input type."]})
            == "Invalid input type."
        )

    def test_nested_errors_use_a_dotted_path(self):
        assert (
            format_validation_messages({"aliases": {0: ["Not a valid string."]}})
            == "aliases.0: Not a valid string."
        )
        assert (
            format_validation_messages({"nested": {"_schema": ["Bad."]}})
            == "nested: Bad."
        )
