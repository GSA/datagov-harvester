from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from migrations.versions import (
    a1c2e3f4b5d6_add_record_type_to_harvest_record as migration,
)
from migrations.versions import (
    b2d4f6a8c0e2_add_catalog_record_to_record_type as catalog_record_migration,
)
from migrations.versions import (
    c3e5a7b9d1f3_add_data_series_to_record_type as data_series_migration,
)


def _run_upgrade(columns, indexes):
    bind = MagicMock()
    bind.execute.return_value.scalars.return_value.all.return_value = [
        "dataset",
        "data_service",
    ]
    inspector = MagicMock()
    inspector.get_columns.return_value = columns
    inspector.get_indexes.return_value = indexes
    context = MagicMock()
    context.autocommit_block.return_value = nullcontext()

    with (
        patch.object(migration.op, "get_bind", return_value=bind),
        patch.object(migration.sa, "inspect", return_value=inspector),
        patch.object(migration.record_type_enum, "create") as create_enum,
        patch.object(migration.op, "add_column") as add_column,
        patch.object(migration.op, "create_index") as create_index,
        patch.object(migration.op, "get_context", return_value=context),
        patch.object(migration.op, "execute") as execute,
    ):
        migration.upgrade()

    return create_enum, add_column, create_index, execute


def test_upgrade_resumes_after_column_and_index_were_committed():
    column = {
        "name": "record_type",
        "type": SimpleNamespace(name="record_type"),
        "nullable": False,
        "default": "'dataset'::record_type",
    }
    index = {
        "name": "ix_harvest_record_record_type",
        "column_names": ["record_type"],
        "unique": False,
    }

    create_enum, add_column, create_index, execute = _run_upgrade(
        [column],
        [index],
    )

    create_enum.assert_called_once()
    add_column.assert_not_called()
    create_index.assert_not_called()
    statements = [" ".join(call.args[0].split()) for call in execute.call_args_list]
    assert statements == [
        (
            "DROP INDEX CONCURRENTLY IF EXISTS "
            "ix_harvest_record_source_identifier_created_success"
        ),
        (
            "DROP INDEX CONCURRENTLY IF EXISTS "
            "ix_harvest_record_source_type_identifier_created_success"
        ),
        (
            "CREATE INDEX CONCURRENTLY "
            "ix_harvest_record_source_type_identifier_created_success "
            "ON harvest_record ( harvest_source_id, record_type, identifier, "
            "date_created DESC ) INCLUDE (action) WHERE status = 'success'"
        ),
    ]


def test_upgrade_creates_column_and_simple_index_when_missing():
    _, add_column, create_index, _ = _run_upgrade([], [])

    add_column.assert_called_once()
    create_index.assert_called_once_with(
        "ix_harvest_record_record_type",
        "harvest_record",
        ["record_type"],
    )


def test_upgrade_rejects_an_unexpected_existing_column():
    column = {
        "name": "record_type",
        "type": SimpleNamespace(name="record_type"),
        "nullable": True,
        "default": "'dataset'::record_type",
    }

    with pytest.raises(
        RuntimeError,
        match="record_type column does not match",
    ):
        _run_upgrade([column], [])


@pytest.mark.parametrize(
    ("record_type_migration", "value"),
    [
        (catalog_record_migration, "catalog_record"),
        (data_series_migration, "data_series"),
    ],
)
def test_enum_upgrade_adds_value_without_rewriting_table(
    record_type_migration,
    value,
):
    with patch.object(record_type_migration.op, "execute") as execute:
        record_type_migration.upgrade()

    execute.assert_called_once_with(
        f"ALTER TYPE record_type ADD VALUE IF NOT EXISTS '{value}'"
    )
