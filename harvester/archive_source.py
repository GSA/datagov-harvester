"""Background task command to archive a harvest source.

Invoked via:
    python -m harvester.archive_source <source_id> <user_email>
"""

import logging
import sys

from harvester import db_interface

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def archive_source_command(source_id, user_email):
    """Archive a harvest source by ID.

    Args:
        source_id: ID of the harvest source to archive
        user_email: Email of the user who initiated the archive
    """
    logger.info("Archiving harvest source %s (requested by %s)", source_id, user_email)

    message, status = db_interface.archive_harvest_source(source_id, user_email)

    if status == 200:
        logger.info("Successfully archived harvest source %s", source_id)
    else:
        logger.error(
            "Failed to archive harvest source %s: %s (status %d)",
            source_id,
            message,
            status,
        )
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        logger.error(
            "Usage: python -m harvester.archive_source <source_id> <user_email>"
        )
        sys.exit(1)

    source_id = sys.argv[1]
    user_email = sys.argv[2]

    archive_source_command(source_id, user_email)
