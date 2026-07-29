# Post-mortem: staging OpenSearch rebuild failure, 2026-07-28

The **Rebuild OpenSearch Index** workflow failed against staging at 20:17 UTC
with `resource_already_exists_exception` on the index it had just created itself.
Catalog stayed fully available throughout.

## What happened

```
20:17:24.89  Creating physical index datasets-30394972485-1 with current mapping...
20:18:24.95  WARNING PUT /datasets-30394972485-1 [status:N/A request:60.059s]
20:18:47.26  RequestError(400, 'resource_already_exists_exception',
             'index [datasets-30394972485-1/Fu6Jv-GqSw6ZhKY4U5fmPg] already exists')
20:18:47.54  Exit status 1
```

Attempt 1 ran for **60.059 seconds** and hit the client socket timeout. Attempt 2,
issued 22 seconds later, was rejected because the index already existed.

## Root cause

A client-side timeout on a request that had actually succeeded server-side, retried
non-idempotently.

Three facts combine:

1. **The client socket timeout is 60s.** `OpenSearchClient._create_aws_opensearch_client`
   (in the `datagov_data_access` package) builds the client with `timeout=60`,
   `max_retries=3`, `retry_on_timeout=True`.

2. **`indices.create` does not return until shards are active.** The candidate
   index is created with 5 primaries and 1 replica — 10 shards to allocate across
   4 data nodes. On a busy cluster that allocation can take longer than 60s. The
   mapping itself is not the problem: it is 1.6 KB and 27 fields.

3. **`retry_on_timeout=True` retries a request that is not idempotent.**
   `opensearchpy/transport.py` treats `ConnectionTimeout` as retryable and reissues
   the same `PUT`. But the first request had already reached the cluster and created
   the index — the client just never saw the response. `PUT /<index>` is a create,
   not an upsert, so the retry is guaranteed to fail with
   `resource_already_exists_exception`.

The failure was therefore *caused by* the retry that was supposed to make the
operation more reliable. Nothing was wrong with the cluster, the mapping, or the
workflow inputs.

### Confirming evidence

The orphaned index is still on the cluster with zero documents:

```
datasets                  docs=1,643,649  5.6gb  pri=5 rep=1
datasets-30394972485-1    docs=0          2kb    pri=5 rep=1   <- orphan
```

`datasets-30394972485-1` existing at all proves attempt 1 succeeded server-side.
`docs=0` proves the task died before backfilling. The `datasets` alias was never
touched — it is still a concrete index, exactly as before the run.

## Impact

**None to users.** Monitoring across the failure window recorded:

- 58/58 dashboard checks returned 547,883 datasets — the count never moved
- 123 search checks, none returning zero results
- cluster green throughout, 0 thread-pool rejections

The "reads keep being served from the old index until the alias swap" property
held even though the rebuild failed. That is the design working: the failure
happened before anything user-visible changed.

Residual: one orphaned empty index consuming ~2 KB and 10 shard slots.

## The fix

`app/commands/search.py` — `_create_rebuild_index()`:

1. **Pass an explicit `request_timeout`** (`OPENSEARCH_CREATE_INDEX_TIMEOUT_SECONDS
   = 300`) so shard allocation has room to finish inside one attempt instead of
   being cut off at the client's 60s default.
2. **Treat `resource_already_exists_exception` as success.** The command already
   checks that the target index does not exist before it starts, so an "already
   exists" error at this point can only be this command's own timed-out attempt.
   Swallowing it makes the create idempotent, which is what `retry_on_timeout`
   assumed all along.

Both halves are needed. (1) makes the timeout unlikely; (2) makes it harmless when
it happens anyway. Any other `RequestError` still aborts the rebuild.

Regression tests in `tests/unit/test_search_commands.py`:

- `test_rebuild_index_creates_with_extended_request_timeout`
- `test_rebuild_index_survives_already_exists_after_timed_out_create` — reproduces
  this exact sequence and asserts the rebuild proceeds to backfill and alias swap
- `test_rebuild_index_still_aborts_on_other_create_errors`

## Before re-running

Delete the orphaned index, or the pre-flight existence check will reject a rerun
that reuses the name. The workflow derives the name from
`datasets-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}`, so a *new* run gets a fresh name
and is not blocked — but a re-*attempt* of the same run would collide.

```bash
cf run-task datagov-harvest --name delete-orphan-index -k 2G -m 1G \
  --command "flask search delete-index --index-name datasets-30394972485-1"
```

`delete-index` refuses to remove anything still attached to an alias or the
logical alias name itself, so it cannot touch live traffic.

## Notes for the next attempt

- This is still the **first** alias conversion in staging: `datasets` remains a
  concrete index, so the run needs `--allow-legacy-index-removal`
  (**Allow the one-time replacement of a concrete datasets index**). Take an
  OpenSearch snapshot first, per the operator runbook
  (`docs/ops/rebuild-opensearch-index.md` on the
  `5885-zero-downtime-opensearch` branch; not yet merged to `develop`).
- Re-run the monitoring alongside it —
  [monitor-catalog-during-rebuild.md](monitor-catalog-during-rebuild.md). The
  useful comparison is against the stored baseline, not against intuition: staging
  search already has a heavy tail at idle (8% of samples over 5s, max 49s), so
  isolated slow searches during a rebuild are not by themselves evidence of impact.
- Watch **JVM heap**, not CPU. Heap idles near 70% on the busiest node against a
  1.9 GiB max; CPU idles under 10%. Heap is the metric with no headroom.
