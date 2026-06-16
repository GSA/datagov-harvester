from sqlalchemy.dialects import postgresql
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
import os
import sys

# because this runs outside of the context the flask app
# we need this line to help resolve the python path
# to find the harvester modules
sys.path.insert(1, "/".join(os.path.realpath(__file__).split("/")[0:-2]))

from harvester import HarvesterDBInterface
from harvester.utils.general_utils import translate_spatial_to_geojson

DATABASE_URI = os.getenv("DATABASE_URI")

# create a scopedsession for our harvest runner
engine = create_engine(DATABASE_URI)
session_factory = sessionmaker(bind=engine, autoflush=True)
session = scoped_session(session_factory)

interface: HarvesterDBInterface = HarvesterDBInterface(session=session)


def render_sql(query) -> str:
    return str(
        query.statement.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )


def run_benchmarks():
    # EXPLAIN (ANALYZE, BUFFERS, COSTS, VERBOSE)
    # SELECT * FROM users WHERE email = 'user@example.com';

    res = translate_spatial_to_geojson("-500.5471, 36.1198, -89.5345, 40.596")
    # resp = interface.get_datasets_by_source(
    #     "9996b8ab-2032-4fd4-acfd-2702991ae5ef", paginate=True
    # )


if __name__ == "__main__":
    run_benchmarks()
