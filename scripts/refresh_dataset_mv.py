import os
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

sys.path.insert(1, "/".join(os.path.realpath(__file__).split("/")[0:-2]))

from harvester import HarvesterDBInterface  # noqa E402

DATABASE_URI = os.getenv("DATABASE_URI")

engine = create_engine(DATABASE_URI)
session_factory = sessionmaker(bind=engine, autoflush=True)
session = scoped_session(session_factory)


def main():
    interface = HarvesterDBInterface(session=session)
    interface.refresh_dataset_mv()


if __name__ == "__main__":
    main()
