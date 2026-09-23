---
title: Scheduled Harvest Failure
labels: bug
---

Scheduled harvest workflow with issue: {{ workflow }}
Job Failed: {{ env.GITHUB_JOB }}
Last Commit: {{ env.LAST_COMMIT }}
Number of times run: {{ env.GITHUB_ATTEMPTS }}
Last run by: {{ env.LAST_RUN_BY }}
GitHub Actions run: https://github.com/{{ env.REPO }}/actions/runs/{{ env.RUN_ID }}
