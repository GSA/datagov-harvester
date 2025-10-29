import os
import sys
from datetime import datetime, timedelta
import calendar
import json
import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from google.oauth2 import service_account
from googleapiclient.discovery import build

sys.path.insert(1, "/".join(os.path.realpath(__file__).split("/")[0:-2]))

logger = logging.getLogger(__name__)

# database data
from harvester import HarvesterDBInterface  # noqa E402

DATABASE_URI = os.getenv("DATABASE_URI")

engine = create_engine(DATABASE_URI)
session_factory = sessionmaker(bind=engine, autoflush=True)
session = scoped_session(session_factory)

# google analytics data
GA_CREDENTIALS = json.loads(os.getenv("GA_CREDENTIALS", {}))
if not GA_CREDENTIALS:
    logger.error("GA credentials are missing. exiting")
    sys.exit(1)

GA_CREDENTIALS["scopes"] = ["https://www.googleapis.com/auth/analytics.readonly"]
GA4_PROPERTY_ID = "properties/381392243"

credentials = service_account.Credentials.from_service_account_info(GA_CREDENTIALS)
analytics = build("analyticsdata", "v1beta", credentials=credentials)
properties = analytics.properties()


def date_range_last_month():
    last_month = datetime.today().replace(day=1) - timedelta(days=1)
    last_day = calendar.monthrange(last_month.year, last_month.month)[1]
    end_date = datetime(last_month.year, last_month.month, last_day).strftime(
        "%Y-%m-%d"
    )
    start_date = datetime(last_month.year, last_month.month, 1).strftime("%Y-%m-%d")
    return [{"startDate": start_date, "endDate": end_date}]


def get_view_counts_of_datasets() -> list[dict]:
    """
    get last months page count for datasets with view > 0
      [
        {
          "dataset_slug": [dataset_slug],
          "view_count": [view_count]
        }
        ... n
      ]
    """
    output = []

    page_size = 100000
    offset = 0
    report = {
        "dateRanges": date_range_last_month(),
        "dimensions": [{"name": "pagePath"}],
        "dimensionFilter": {
            "filter": {
                "fieldName": "pagePath",
                "stringFilter": {
                    "matchType": "FULL_REGEXP",
                    "value": "\/dataset\/[\w-]+",
                },
            }
        },
        "metrics": [{"name": "screenPageViews"}],
        "limit": page_size,
        "offset": offset,
    }

    while True:
        response = properties.runReport(property=GA4_PROPERTY_ID, body=report).execute()

        if "rows" not in response:
            break

        for row in response["rows"]:
            output.append(
                {
                    # slice off '/dataset/' to get slug
                    "dataset_slug": row["dimensionValues"][0]["value"][9:],
                    "view_count": int(row["metricValues"][0]["value"]),
                }
            )

        offset += page_size
        report["offset"] = offset

    return output


def main():
    interface = HarvesterDBInterface(session=session)
    datasets = get_view_counts_of_datasets()
    interface.upsert_view_counts_of_datasets(datasets)


if __name__ == "__main__":
    main()
