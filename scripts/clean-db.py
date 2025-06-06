import argparse
import os
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

sys.path.insert(1, "/".join(os.path.realpath(__file__).split("/")[0:-2]))

from harvester import HarvesterDBInterface  # noqa E402

DATABASE_URI = os.getenv("DATABASE_URI")

# create a scopedsession for our harvest runner
engine = create_engine(DATABASE_URI)
session_factory = sessionmaker(bind=engine, autoflush=True)
session = scoped_session(session_factory)


def main(days):
    interface = HarvesterDBInterface(session=session)
    outdated_records = interface.get_all_outdated_records(days)
    for record in outdated_records:
        interface.delete_harvest_record(ID=record.id)  # this cascades to errors
    interface.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Harvest database cleaner",
        description="cleans outdated harvest records and errors",
    )
    parser.add_argument("days", help="harvest records [days] old")
    args = parser.parse_args(sys.argv[1:])

    main(args.days)
