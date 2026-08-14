"""add record_type to harvest_record

Revision ID: a1c2e3f4b5d6
Revises: 428e3ffa02ea
Create Date: 2026-08-10 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a1c2e3f4b5d6"
down_revision = "428e3ffa02ea"
branch_labels = None
depends_on = None

record_type_enum = sa.Enum("dataset", "data_service", name="record_type")


def _validate_record_type_enum(bind):
    labels = set(
        bind.execute(
            sa.text("""
                SELECT enumlabel
                FROM pg_enum
                JOIN pg_type ON pg_type.oid = pg_enum.enumtypid
                WHERE pg_type.typname = :type_name
                  AND pg_type_is_visible(pg_type.oid)
                """),
            {"type_name": record_type_enum.name},
        )
        .scalars()
        .all()
    )
    required_labels = {"dataset", "data_service"}
    if not required_labels.issubset(labels):
        raise RuntimeError(
            "Existing record_type enum is missing required labels: "
            f"{sorted(required_labels - labels)}"
        )


def _ensure_record_type_column(bind):
    inspector = sa.inspect(bind)
    existing_column = next(
        (
            column
            for column in inspector.get_columns("harvest_record")
            if column["name"] == "record_type"
        ),
        None,
    )

    if existing_column is None:
        op.add_column(
            "harvest_record",
            sa.Column(
                "record_type",
                record_type_enum,
                nullable=False,
                server_default="dataset",
            ),
        )
    else:
        type_name = getattr(existing_column["type"], "name", None)
        server_default = str(existing_column.get("default") or "")
        if (
            type_name != record_type_enum.name
            or existing_column["nullable"]
            or "dataset" not in server_default
        ):
            raise RuntimeError(
                "Existing harvest_record.record_type column does not match "
                "the partially applied migration"
            )

    existing_index = next(
        (
            index
            for index in sa.inspect(bind).get_indexes("harvest_record")
            if index["name"] == "ix_harvest_record_record_type"
        ),
        None,
    )
    if existing_index is None:
        op.create_index(
            "ix_harvest_record_record_type",
            "harvest_record",
            ["record_type"],
        )
    elif existing_index.get("column_names") != ["record_type"] or existing_index.get(
        "unique", False
    ):
        raise RuntimeError(
            "Existing ix_harvest_record_record_type index does not match "
            "the partially applied migration"
        )


def upgrade():
    bind = op.get_bind()
    record_type_enum.create(bind, checkfirst=True)
    _validate_record_type_enum(bind)
    _ensure_record_type_column(bind)

    with op.get_context().autocommit_block():
        op.execute("""
            DROP INDEX CONCURRENTLY IF EXISTS
            ix_harvest_record_source_identifier_created_success
            """)
        # A failed CREATE INDEX CONCURRENTLY can leave an invalid index behind.
        op.execute("""
            DROP INDEX CONCURRENTLY IF EXISTS
            ix_harvest_record_source_type_identifier_created_success
            """)
        op.execute("""
            CREATE INDEX CONCURRENTLY
            ix_harvest_record_source_type_identifier_created_success
            ON harvest_record (
                harvest_source_id,
                record_type,
                identifier,
                date_created DESC
            )
            INCLUDE (action)
            WHERE status = 'success'
            """)


def downgrade():
    with op.get_context().autocommit_block():
        op.execute("""
            DROP INDEX CONCURRENTLY IF EXISTS
            ix_harvest_record_source_type_identifier_created_success
            """)
        op.execute("""
            DROP INDEX CONCURRENTLY IF EXISTS
            ix_harvest_record_source_identifier_created_success
            """)
        op.execute("""
            CREATE INDEX CONCURRENTLY
            ix_harvest_record_source_identifier_created_success
            ON harvest_record (harvest_source_id, identifier, date_created DESC)
            INCLUDE (action)
            WHERE status = 'success'
            """)

    op.drop_index("ix_harvest_record_record_type", table_name="harvest_record")
    op.drop_column("harvest_record", "record_type")
    record_type_enum.drop(op.get_bind(), checkfirst=True)
