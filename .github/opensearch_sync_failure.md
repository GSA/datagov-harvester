---
title: OpenSearch Sync Failed ({{ env.ENVIRONMENT }})
labels: ["bug", "o&m"]
---

Workflow with Issue: {{ workflow }}
Job Failed: {{ env.GITHUB_JOB }}
Command: {{ env.COMMAND }}
Cloud.gov Environment: {{ env.ENVIRONMENT }}

Last Commit: {{ env.LAST_COMMIT }}
Number of times run: {{ env.GITHUB_ATTEMPTS }}
Last run by: {{ env.LAST_RUN_BY }}
GitHub Action Run: https://github.com/GSA/datagov-harvester/actions/runs/{{ env.RUN_ID }}
