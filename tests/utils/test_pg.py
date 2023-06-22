import logging

import pytest
from pytest_postgresql import factories
from pytest_postgresql.janitor import DatabaseJanitor
from sqlalchemy import create_engine, select
from sqlalchemy.orm.session import sessionmaker

from harvester import harvest_jobs_schema
from harvester.utils.pg import PostgresUtility

# TODO is this the logging we want?
logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


@pytest.fixture
def postgres_utility():
    test_db = factories.postgresql("somedb")
    return PostgresUtility(
        test_db.host, test_db.port, test_db.user, test_db.password, test_db.dbname
    )


def test_store_new_entry(postgres_utility):
    job_id = "test_store_new_entry"
    postgres_utility.store_new_entry(job_id)
    postgres_utility.connection.cursor().execute.assert_called_once_with(
        "INSERT INTO job_table (job_id, status) VALUES (%s, 'new')", (job_id,)
    )
    postgres_utility.connection.commit.assert_called_once()


def test_update_entry_status(postgres_utility):
    job_id = "job123"
    new_status = "Extracted"
    postgres_utility.connection = MagicMock()
    postgres_utility.update_entry_status(job_id, new_status)
    postgres_utility.connection.cursor().execute.assert_called_once_with(
        "UPDATE job_table SET status = %s WHERE job_id = %s", (new_status, job_id)
    )
    postgres_utility.connection.commit.assert_called_once()


def test_perform_bulk_updates(postgres_utility):
    new_status = "In Progress"
    preconditions = "<preconditions>"
    postgres_utility.connection = MagicMock()
    postgres_utility.perform_bulk_updates(new_status, preconditions)
    postgres_utility.connection.cursor().execute.assert_called_once_with(
        "UPDATE job_table SET status = %s WHERE <preconditions>", (new_status,)
    )
    postgres_utility.connection.commit.assert_called_once()
