"""harvest_record_current_issue.id FKs to harvest_record_error.id

Revision ID: af98cfc82f70
Revises: 9e7c3a4b6d21
Create Date: 2026-10-23 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "af98cfc82f70"
down_revision = "9e7c3a4b6d21"
branch_labels = None
depends_on = None

severity_enum = postgresql.ENUM(
    "error",
    "warning",
    name="error_severity",
    create_type=False,
)


def upgrade():
    # A current-issue row's id becomes the id of the harvest_record_error
    # row it mirrors, instead of an independently generated UUID, so the
    # two tables always agree on id/date_created/type/message/severity for
    # a given issue (there is exactly one row of data; this table just
    # indexes it by identifier instead of by job). Existing rows are
    # dropped and rebuilt on the next harvest rather than migrated in
    # place, since they're a point-in-time cache, not a source of truth.
    op.execute("TRUNCATE TABLE harvest_record_current_issue")
    op.drop_column("harvest_record_current_issue", "date_created")
    op.drop_column("harvest_record_current_issue", "type")
    op.drop_column("harvest_record_current_issue", "message")
    op.drop_column("harvest_record_current_issue", "severity")
    op.create_foreign_key(
        "fk_harvest_record_current_issue_id_harvest_record_error",
        "harvest_record_current_issue",
        "harvest_record_error",
        ["id"],
        ["id"],
        ondelete="CASCADE",
    )


def downgrade():
    op.execute("TRUNCATE TABLE harvest_record_current_issue")
    op.drop_constraint(
        "fk_harvest_record_current_issue_id_harvest_record_error",
        "harvest_record_current_issue",
        type_="foreignkey",
    )
    op.add_column(
        "harvest_record_current_issue",
        sa.Column("severity", severity_enum, nullable=False, server_default="error"),
    )
    op.add_column(
        "harvest_record_current_issue", sa.Column("message", sa.String(), nullable=True)
    )
    op.add_column(
        "harvest_record_current_issue", sa.Column("type", sa.String(), nullable=True)
    )
    op.add_column(
        "harvest_record_current_issue",
        sa.Column("date_created", sa.DateTime(), nullable=True),
    )
