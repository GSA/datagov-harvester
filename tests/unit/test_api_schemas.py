import pytest
from marshmallow import ValidationError
from sqlalchemy.dialects import postgresql

from app.api_schemas import OrgCreate
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
