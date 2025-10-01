"""create dataset materialized view

Revision ID: 4c8c5f116577
Revises: 416319f5e5bb
Create Date: 2025-09-29 18:49:29.263628

"""
from alembic import op
from alembic_utils.pg_materialized_view import PGMaterializedView

# revision identifiers, used by Alembic.
revision = '4c8c5f116577'
down_revision = '416319f5e5bb'
branch_labels = None
depends_on = None


def upgrade():
    public_dataset_mv = PGMaterializedView(
                schema="public",
                signature="dataset_mv",
                definition="""SELECT * FROM (
                SELECT DISTINCT ON (identifier, harvest_source_id) *
                FROM harvest_record 
                WHERE status = 'success'
                ORDER BY identifier, harvest_source_id, date_created DESC ) sq
                WHERE sq.action != 'delete';""",
                with_data=True
            )

    op.create_entity(public_dataset_mv)

def downgrade():
    public_dataset_mv = PGMaterializedView(
                schema="public",
                signature="dataset_mv",
                definition="""SELECT * FROM (
                    SELECT DISTINCT ON (identifier, harvest_source_id) *
                    FROM harvest_record 
                    WHERE status = 'success'
                    ORDER BY identifier, harvest_source_id, date_created DESC ) sq
                    WHERE sq.action != 'delete';""",
                with_data=True
            )

    op.drop_entity(public_dataset_mv)
