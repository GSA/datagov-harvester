---
title: Missing Datasets in Harvester ({{ env.ENVIRONMENT }})
labels: ["bug", "o&m"]
---

Workflow with Issue: {{ workflow }}
Job Failed: {{ env.GITHUB_JOB }}
Type of Failure: {{ env.COMMAND }}
Cloud.gov Environment: {{ env.ENVIRONMENT }}

Last Commit: {{ env.LAST_COMMIT }}
Last run by: {{ env.LAST_RUN_BY }}
Github Action Run: https://github.com/GSA/datagov-harvester/actions/runs/{{ env.RUN_ID }}
