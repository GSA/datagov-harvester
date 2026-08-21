---
title: OpenSearch migration failure - {{ env.ENVIRONMENT }}
labels: bug
---

The zero-downtime OpenSearch release failed in **{{ env.ENVIRONMENT }}**.
Scheduled harvesting remains disabled and requires operator review.

| Phase | Result |
| --- | --- |
| Disable harvesting | {{ env.DISABLE_RESULT }} |
| Create services | {{ env.CREATE_SERVICES_RESULT }} |
| Provision replacement | {{ env.PROVISION_RESULT }} |
| Push temporary app | {{ env.PUSH_TEMP_RESULT }} |
| Bind replacement | {{ env.BIND_RESULT }} |
| Rebuild task | {{ env.REBUILD_RESULT }} |
| Monitor rebuild | {{ env.MONITOR_RESULT }} |
| Validate replacement | {{ env.VALIDATE_RESULT }} |
| Monitor validation | {{ env.VALIDATE_MONITOR_RESULT }} |
| Delete temporary app | {{ env.DELETE_TEMP_RESULT }} |
| Promote services | {{ env.PROMOTE_RESULT }} |
| Deploy canonical app | {{ env.DEPLOY_RESULT }} |
| Restart catalog | {{ env.CATALOG_RESULT }} |
| Clean up old cluster | {{ env.CLEANUP_RESULT }} |
| Restore harvesting | {{ env.ENABLE_RESULT }} |

- Workflow: {{ workflow }}
- Commit: {{ env.LAST_COMMIT }}
- Run: https://github.com/{{ env.REPO }}/actions/runs/{{ env.RUN_ID }}
- Triggering migration PR(s): {{ env.MIGRATION_PRS }}
