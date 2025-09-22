"""Add collection_parent_url and waf-collection type

Revision ID: 3b56c7e46974
Revises: 1800d355e5b9
Create Date: 2025-09-19 18:18:14.376089

"""
from alembic import op
import sqlalchemy as sa
from alembic_postgresql_enum import TableReference

from migrations.utils import get_terminate_processes_sql_cmd

# revision identifiers, used by Alembic.
revision = '3b56c7e46974'
down_revision = 'a6aa1afd27b7'
branch_labels = None
depends_on = None


def upgrade():
    # enum change uses a restrictive table lock, so get everything out of the
    # way so we can succeed
    op.execute(get_terminate_processes_sql_cmd())

    with op.batch_alter_table('harvest_source', schema=None) as batch_op:
        batch_op.add_column(sa.Column('collection_parent_url', sa.String(), nullable=True))

    op.sync_enum_values(
        enum_schema='public',
        enum_name='source_type',
        new_values=['document', 'waf', 'waf-collection'],
        affected_columns=[TableReference(table_schema='public', table_name='harvest_source', column_name='source_type')],
        enum_values_to_rename=[],
    )
    # ### end Alembic commands ###


def downgrade():
    # enum change uses a restrictive table lock, so get everything out of the
    # way so we can succeed
    op.execute(get_terminate_processes_sql_cmd())

    op.sync_enum_values(
        enum_schema='public',
        enum_name='source_type',
        new_values=['document', 'waf'],
        affected_columns=[TableReference(table_schema='public', table_name='harvest_source', column_name='source_type')],
        enum_values_to_rename=[],
    )
    with op.batch_alter_table('harvest_source', schema=None) as batch_op:
        batch_op.drop_column('collection_parent_url')
