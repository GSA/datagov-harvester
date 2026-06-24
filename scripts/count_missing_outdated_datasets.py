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
    count, sample_record = interface.get_missing_or_outdated_dataset_count_and_sample()

    print(f"number of missing/outdated datasets {count}")

    if count > 0:
        print("10 sample record ids for troubleshooting")

        for record in sample_record:
            print(record[0])

    sys.exit(0 if count == 0 else 1)


if __name__ == "__main__":
    main()
