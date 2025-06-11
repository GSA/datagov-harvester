"""Add organization_type column with enumerated values

Revision ID: 1800d355e5b9
Revises: cf577d46fccd
Create Date: 2025-06-11 13:01:12.614966

"""

from alembic import op
import sqlalchemy as sa

from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "1800d355e5b9"
down_revision = "cf577d46fccd"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("organization", schema=None) as batch_op:
        organization_type_enum = postgresql.ENUM(
            "Federal Government",
            "City Government",
            "State Government",
            "County Government",
            "University",
            "Tribal",
            "Non-Profit",
            name="organization_type_enum",
        )
        organization_type_enum.create(batch_op.get_bind())

        batch_op.add_column(
            sa.Column(
                "organization_type",
                organization_type_enum,
                nullable=True,
            ),
        )


def downgrade():
    with op.batch_alter_table("organization", schema=None) as batch_op:
        batch_op.drop_column("organization_type")

    organization_type_enum = postgresql.ENUM(name="organization_type_enum")
    organization_type_enum.drop(op.get_bind())
