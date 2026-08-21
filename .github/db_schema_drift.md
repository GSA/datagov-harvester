---
title: DB schema drift detected ({{ env.ENVIRONMENT }})
labels: ["bug", "o&m"]
---

`flask db check` found differences between the SQLAlchemy models, the Alembic
migration history, and the live PostgreSQL schema. This usually means a
manual change was made to the database outside of a migration.

Workflow with Issue: {{ workflow }}
Job Failed: {{ env.GITHUB_JOB }}
Command: {{ env.COMMAND }}
Cloud.gov Environment: {{ env.ENVIRONMENT }}

Last Commit: {{ env.LAST_COMMIT }}
Number of times run: {{ env.GITHUB_ATTEMPTS }}
Last run by: {{ env.LAST_RUN_BY }}
GitHub Action Run: https://github.com/GSA/datagov-harvester/actions/runs/{{ env.RUN_ID }}
