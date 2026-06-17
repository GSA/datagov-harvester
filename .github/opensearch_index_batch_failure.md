---
title: OpenSearch Index Batch Failures ({{ env.ENVIRONMENT }})
labels: ["bug", "o&m"]
---

The OpenSearch sync task logs included the index failure message
`{{ env.OPENSEARCH_INDEX_BATCH_FAILURE_PATTERN }}`.

Workflow with Issue: {{ workflow }}
Job Failed: {{ env.GITHUB_JOB }}
Command: {{ env.COMMAND }}
Cloud.gov Environment: {{ env.ENVIRONMENT }}

Last observed: {{ date | date('YYYY-MM-DD HH:mm:ss Z') }}
Last Commit: {{ env.LAST_COMMIT }}
Number of times run: {{ env.GITHUB_ATTEMPTS }}
Last run by: {{ env.LAST_RUN_BY }}
GitHub Action Run: https://github.com/GSA/datagov-harvester/actions/runs/{{ env.RUN_ID }}
