"""SCRATCH: deliberate migration failure for #6145 validation

Revision ID: 6145scratch01
Revises: d4f6a8c0e2b3
Create Date: 2026-08-21 00:00:00.000000

THROWAWAY migration used only to empirically validate that a failed
`flask db upgrade` now fails container boot and the CF rolling deploy,
per GSA/data.gov#6145. Will be reverted immediately after the
development-space validation run.
"""

from alembic import op

revision = "6145scratch01"
down_revision = "d4f6a8c0e2b3"
branch_labels = None
depends_on = None


def upgrade():
    raise RuntimeError("6145 scratch: deliberate migration failure for validation")


def downgrade():
    pass
