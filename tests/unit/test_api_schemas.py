import pytest
from marshmallow import ValidationError
from sqlalchemy.dialects import postgresql

from app.api_schemas import OrgCreate, OrgUpdate
from app.constants import SLUG_ERROR_MESSAGE
from database.models import Organization


def bind_organization_type(value):
    """Run a value through the column type the API writes it to."""
    column_type = Organization.__table__.c.organization_type.type
    return column_type.bind_processor(postgresql.dialect())(value)


class TestOrgCreate:
    """Tests for the organization create schema."""

    def test_organization_type_loads_as_a_plain_string(self):
        loaded = OrgCreate().load(
            {
                "name": "OpenTopography",
                "slug": "opentopography",
                "organization_type": "Non-Profit",
            }
        )

        assert loaded["organization_type"] == "Non-Profit"
        assert isinstance(loaded["organization_type"], str)

    def test_loaded_organization_type_is_accepted_by_the_column(self):
        # the Enum field deserializes to the member, and the column is
        # declared with the values, so the member reaches the insert as
        # "OrganizationType.Non-Profit" and Postgres rejects it
        loaded = OrgCreate().load(
            {
                "name": "OpenTopography",
                "slug": "opentopography",
                "organization_type": "Non-Profit",
            }
        )

        assert bind_organization_type(loaded["organization_type"]) == "Non-Profit"

    def test_organization_type_is_left_out_when_not_supplied(self):
        loaded = OrgCreate().load({"name": "OpenTopography", "slug": "opentopography"})

        assert "organization_type" not in loaded

    def test_unknown_organization_type_is_still_rejected(self):
        with pytest.raises(ValidationError):
            OrgCreate().load(
                {
                    "name": "OpenTopography",
                    "slug": "opentopography",
                    "organization_type": "Not A Real Type",
                }
            )


class TestOrgUpdate:
    """Tests for the organization edit schema."""

    def test_every_field_is_optional(self):
        assert OrgUpdate().load({}) == {}
        assert OrgUpdate().load({"description": "x"}) == {"description": "x"}

    def test_slug_follows_the_create_rules(self):
        assert OrgUpdate().load({"slug": "gsa-2"}) == {"slug": "gsa-2"}
        for slug in ["GSA", "g_s_a", "", "gsa\n", "a" * 101]:
            with pytest.raises(ValidationError) as update_error:
                OrgUpdate().load({"slug": slug})
            with pytest.raises(ValidationError) as create_error:
                OrgCreate().load({"name": "GSA", "slug": slug})
            assert update_error.value.messages == create_error.value.messages

    def test_slug_uses_the_shared_error_message(self):
        with pytest.raises(ValidationError) as excinfo:
            OrgUpdate().load({"slug": "GSA"})
        assert excinfo.value.messages == {"slug": [SLUG_ERROR_MESSAGE]}

    def test_id_and_unknown_fields_are_rejected(self):
        with pytest.raises(ValidationError) as excinfo:
            OrgUpdate().load({"id": "x", "bogus": 1})
        assert excinfo.value.messages == {
            "id": ["Unknown field."],
            "bogus": ["Unknown field."],
        }

    def test_code_repo_url_may_be_null(self):
        assert OrgUpdate().load({"code_repo_url": None}) == {"code_repo_url": None}

    def test_code_repo_url_protocol_is_still_checked(self):
        with pytest.raises(ValidationError) as excinfo:
            OrgUpdate().load({"code_repo_url": "ftp://example.com"})
        assert "code_repo_url" in excinfo.value.messages

    def test_organization_type_loads_as_a_plain_string(self):
        loaded = OrgUpdate().load({"organization_type": "Non-Profit"})
        assert loaded["organization_type"] == "Non-Profit"
        assert isinstance(loaded["organization_type"], str)


@pytest.mark.parametrize("schema", [OrgCreate, OrgUpdate])
def test_slug_with_trailing_newline_is_rejected(schema):
    # $ would match just before the newline and let "gsa\n" through
    with pytest.raises(ValidationError) as excinfo:
        schema().load({"name": "GSA", "slug": "gsa\n"})
    assert excinfo.value.messages == {"slug": [SLUG_ERROR_MESSAGE]}
