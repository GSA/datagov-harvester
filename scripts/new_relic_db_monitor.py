import logging
import os

from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

PG_ACTIVITY_SUMMARY = text("""
    WITH idle_transactions AS (
        SELECT
            pid,
            application_name,
            query,
            xact_start,
            state_change
        FROM pg_stat_activity
        WHERE state = 'idle in transaction'
          AND xact_start IS NOT NULL
    )
    SELECT
        COUNT(*)::int AS idle_transaction_count,

        COALESCE(
            MAX(EXTRACT(EPOCH FROM (now() - xact_start))),
            0
        )::float AS max_transaction_age_seconds,

        COALESCE(
            MAX(EXTRACT(EPOCH FROM (now() - state_change))),
            0
        )::float AS max_idle_age_seconds,

        COUNT(*) FILTER (
            WHERE now() - xact_start > INTERVAL '60 seconds'
        )::int AS over_60_seconds,

        COUNT(*) FILTER (
            WHERE now() - xact_start > INTERVAL '5 minutes'
        )::int AS over_5_minutes,

        (
            ARRAY_AGG(query ORDER BY xact_start ASC)
        )[1] AS oldest_transaction_query

    FROM idle_transactions
""")


def emit_idle_transaction_event():
    try:
        import newrelic.agent

        newrelic.agent.initialize()
        application = newrelic.agent.register_application(timeout=10)

        if not application.active:
            logger.error("new relic monitoring session isn't active. exiting.")
            return

        db_url = os.getenv("DATABASE_URI")
        if not db_url:
            logger.error(
                "database url isn't set in env. can't proceed further. exiting."
            )
            return

        engine = create_engine(db_url)

        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            activity = conn.execute(PG_ACTIVITY_SUMMARY).mappings().one()

        if not activity:
            logger.error("no activity from the database to emit to new relic. exiting.")
            return

        newrelic.agent.record_custom_event(
            "PostgresActivitySample",
            {
                "idleTransactionCount": activity["idle_transaction_count"],
                "maxTransactionAgeSeconds": activity["max_transaction_age_seconds"],
                "maxIdleAgeSeconds": activity["max_idle_age_seconds"],
                "over60Seconds": activity["over_60_seconds"],
                "over5Minutes": activity["over_5_minutes"],
                "oldestTransactionQuery": activity["oldest_transaction_query"] or "",
                # new relic app name is available via cf manifest.yml
                "appName": os.getenv("NEW_RELIC_APP_NAME"),
            },
            application=application,
        )

        newrelic.agent.shutdown_agent(timeout=5)
    except Exception as e:
        logger.error(f"exception thrown during new relic db monitoring. {str(e)}")
