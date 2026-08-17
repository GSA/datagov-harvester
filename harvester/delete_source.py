"""CF/local task entrypoint: delete a harvest source (CASCADE commit)."""

import logging
import sys

from harvester import db_interface

logger = logging.getLogger("harvest_admin")


def delete_source(source_id: str) -> int:
    try:
        message, status = db_interface.delete_harvest_source(source_id)
        if status == 200:
            logger.info(message)
            return 0
        logger.error(message)
        return 1
    except Exception as e:
        logger.error("Harvest source delete failed :: %s", repr(e))
        return 1
    finally:
        db_interface.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <source_id>", file=sys.stderr)
        sys.exit(2)
    sys.exit(delete_source(sys.argv[1]))
