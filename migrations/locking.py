from contextlib import contextmanager

from sqlalchemy import text

LOCK_APPLICATION_NAME = "datagov-harvester-alembic-lock"
LOCK_NAME = "datagov-harvester-alembic"


@contextmanager
def migration_lock(connectable):
    autocommit_engine = connectable.execution_options(isolation_level="AUTOCOMMIT")

    with autocommit_engine.connect() as connection:
        connection.execute(
            text("SELECT set_config('application_name', :application_name, false)"),
            {"application_name": LOCK_APPLICATION_NAME},
        )
        connection.execute(
            text(
                "SELECT pg_advisory_lock("
                "hashtext(current_database()), hashtext(:lock_name))"
            ),
            {"lock_name": LOCK_NAME},
        )
        try:
            yield
        finally:
            try:
                connection.execute(
                    text(
                        "SELECT pg_advisory_unlock("
                        "hashtext(current_database()), hashtext(:lock_name))"
                    ),
                    {"lock_name": LOCK_NAME},
                )
            finally:
                connection.execute(
                    text("SELECT set_config('application_name', '', false)")
                )


def terminate_database_connections(connectable) -> None:
    autocommit_engine = connectable.execution_options(isolation_level="AUTOCOMMIT")
    terminate_sql = text(
        "SELECT pg_terminate_backend(pid) "
        "FROM pg_stat_activity "
        "WHERE pid <> pg_backend_pid() "
        "AND datname = current_database() "
        "AND state IN ('active', 'idle in transaction') "
        "AND application_name <> :lock_application_name"
    )

    with autocommit_engine.connect() as connection:
        connection.execute(
            terminate_sql,
            {"lock_application_name": LOCK_APPLICATION_NAME},
        )
