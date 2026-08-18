---
title: Retired OpenSearch cleanup required - {{ env.ENVIRONMENT }}
labels: bug
---

The OpenSearch migration completed in **{{ env.ENVIRONMENT }}**, but
`datagov-catalog-opensearch-old` was retained:

{{ env.CLEANUP_REASON }}

After the listed work finishes, verify that no harvest tasks or catalog deployment
is active, then remove the retired service with:

```sh
bin/cleanup_opensearch_cluster.sh datagov-catalog-opensearch-old
```

- Commit: {{ env.LAST_COMMIT }}
- Run: https://github.com/{{ env.REPO }}/actions/runs/{{ env.RUN_ID }}
