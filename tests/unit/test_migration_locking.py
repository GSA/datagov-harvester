from unittest.mock import MagicMock

import pytest

from migrations.locking import (
    LOCK_APPLICATION_NAME,
    migration_lock,
    terminate_database_connections,
)


def _engine_and_connection():
    engine = MagicMock()
    autocommit_engine = engine.execution_options.return_value
    connection = autocommit_engine.connect.return_value.__enter__.return_value
    return engine, connection


def test_migration_lock_is_released_after_success():
    engine, connection = _engine_and_connection()

    with migration_lock(engine):
        pass

    statements = [str(call.args[0]) for call in connection.execute.call_args_list]
    assert "set_config" in statements[0]
    assert "pg_advisory_lock" in statements[1]
    assert "pg_advisory_unlock" in statements[2]
    assert "set_config" in statements[3]


def test_migration_lock_is_released_after_failure():
    engine, connection = _engine_and_connection()

    with pytest.raises(RuntimeError, match="migration failed"):
        with migration_lock(engine):
            raise RuntimeError("migration failed")

    statements = [str(call.args[0]) for call in connection.execute.call_args_list]
    assert "pg_advisory_unlock" in statements[-2]
    assert "set_config" in statements[-1]


def test_connection_termination_is_scoped_and_preserves_lock_waiters():
    engine, connection = _engine_and_connection()

    terminate_database_connections(engine)

    statement, parameters = connection.execute.call_args.args
    sql = str(statement)
    assert "datname = current_database()" in sql
    assert "state IN ('active', 'idle in transaction')" in sql
    assert "application_name <> :lock_application_name" in sql
    assert parameters == {"lock_application_name": LOCK_APPLICATION_NAME}
