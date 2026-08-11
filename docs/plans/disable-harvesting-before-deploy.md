# Disable harvesting *before* the deploy, not inside the reindex

Status: not started. Applies to `.github/workflows/release-space.yml`, and therefore to
both the `develop` → development path (`commit.yml`) and the `main` → staging + prod path
(`deploy.yml`).

## Where the code lives right now (as of 2026-08-11)

| ref | sha | state |
| --- | --- | --- |
| `origin/develop` | `9a1ac0e2` | **has the pipeline.** PR #832 merged; deployed to the development space successfully |
| `origin/main` | `ba174866` | **none of this work.** No `release-space.yml`, no `detect-reindex.yml`, no `detect-reindex-label.sh` |
| `origin/5885-zero-downtime-opensearch-timeouts` | `a4e7e718` | the long-running feature branch — **23 commits behind `develop` and missing every pipeline file** |
| `origin/scratch/migrate-opensearch-staging-test` | `98cbfcc4` | the branch #832 came from; now merged into `develop`. Was used to test unmerged workflows via a `push:` trigger |
| `origin/test-labelled-reindex-development` | `17f6ee52` | PR #833, open against `develop`, carrying `force re-index recommended` — the labelled end-to-end test |

**`5885-zero-downtime-opensearch-timeouts` needs catching up.** It is the long-lived
working branch but has none of the pipeline; its single unique commit (`a4e7e718`) is just
a stale merge-from-`main` with no work at risk. Merge `origin/develop` into it before
doing anything else there, or two divergent copies of these workflows will develop.

### What is actually deployed to `development`

Run `31518201599` (`9a1ac0e2`, all green) — the **unlabelled** verification. It proves the
deploy path and that detection works:

```
Detect reindex label (develop)        success   ← the gh api -X GET fix working live
Preflight (development)               skipped   ← correct, no label
development / create services         success
development / deploy                  success
development / Add network-policies     success
development / Await rollout            skipped   ← correct, no reindex
development / Reindex (development)    skipped   ← correct, no reindex
```

So the development space is running the pipeline code, but **no migration has ever run
through the automated path**. PR #833 is the test that would do that — and it is worth
landing this plan's change *first*, because hoisting the disable alters exactly what #833
exercises.

### Manual triggering

Both callers are `workflow_dispatch`-registered and can be dispatched **at any ref** —
registration is by file path, so GitHub runs the branch's YAML even though `main`'s copy
of `commit.yml` has no `workflow_dispatch` at all (verified 2026-08-11). Inputs on both:
`reindex` (`auto` | `force` | `skip`) and `dry_run`.

Caveat: `commit.yml`'s release jobs are gated `if: github.ref == 'refs/heads/develop'`, so
dispatching it at a feature branch runs lint/test only and skips the release. **The
release half can only be exercised from `develop` itself.** Historically the workaround
for testing unmerged workflows was a `push:`-triggered scratch runner
(`zz_run_migrate_staging.yml`, now deleted) — see
`memory/github-workflow-dispatch-branch-only.md`.

## Context

`release-space.yml` runs the per-space release and is called once by `commit.yml`
(development) and twice by `deploy.yml` (staging, then prod). When a merged PR carries
`force re-index recommended`, it deploys the new code and then rebuilds the OpenSearch
index.

Harvesting is currently paused **inside** the rebuild, which is three jobs after the
deploy. So new code runs against the old index while harvest jobs are still writing to
it. For an additive mapping change that is harmless. For a **breaking** one — a field
retyped or renamed — every write in that window goes into an index whose mapping does not
match the document being written, so it is rejected or silently coerced.

The standalone workflows got this right: `migrate_opensearch_cluster.yml` and
`rebuild_opensearch_index.yml` both open with `disable-harvester`, because they were
dispatched *after* a deploy had already happened. Wrapping a deploy in front of the
migrate call moved that disable from "first in its own workflow" to "third in the
pipeline" without changing it.

## The current (wrong) order

```
release-space.yml:
  create-services → deploy → await-rollout → reindex
                                              └─ migrate_opensearch_cluster.yml:
                                                   plan
                                                   → disable-harvester      ← FIRST PAUSE, far too late
                                                   → build (drain, provision, rebuild, verify)
                                                   → promote → decommission
                                                   → enable-harvester
```

The exposure is not a brief gap. `wait_for_harvest_tasks.sh datagov-harvest 7200`
(`migrate_opensearch_cluster.yml:353`) waits up to **2 hours** for in-flight jobs to
drain, or 15 minutes when `force_kill_running_jobs: true`. Jobs already running keep
writing for that entire time — `harvester/harvest.py:1206` `_index_dataset_in_opensearch`
fires per dataset.

## The new (correct) order

```
release-space.yml:
  disable-harvester → create-services → deploy → await-rollout → reindex
                                                                   └─ migrate (unchanged)
  enable-harvester   ← if: always()
```

Writes stop before any new code is live, and resume only after the release finishes.

## Implementation

All of it is in `.github/workflows/release-space.yml`. `commit.yml` and `deploy.yml`
need **no changes** — they already call `release-space.yml`, so both paths inherit the
fix. That is the point of having extracted it.

### 0. Branch first

Work from `develop`, which is the only ref that has the pipeline:

```bash
git fetch origin
git checkout -b fix-disable-before-deploy origin/develop
```

Separately, and independent of this change, bring the long-running branch up to date so
the workflows do not fork:

```bash
git checkout 5885-zero-downtime-opensearch-timeouts
git merge origin/develop        # 23 commits, brings in the whole pipeline
```

Do **not** target `main`. Everything so far has gone `feature → develop`, and `main` has
none of it.

### 1. New first job: `disable-harvester`

Copy the call pattern from `migrate_opensearch_cluster.yml:295-306` verbatim, changing
`inputs.environment` → `inputs.space`:

```yaml
  disable-harvester:
    name: Disable harvesting (${{ inputs.space }})
    # Only when a rebuild will follow. A routine deploy must NOT pause harvesting --
    # rolling deploys are already safe for harvest jobs, and pausing every deploy would
    # be a behaviour change nobody asked for.
    if: ${{ inputs.reindex && !inputs.dry_run }}
    uses: ./.github/workflows/toggle_harvester.yml
    with:
      state: disable
      environment: ${{ inputs.space }}
    secrets:
      CF_SERVICE_USER: ${{ secrets.CF_SERVICE_USER }}
      CF_SERVICE_AUTH: ${{ secrets.CF_SERVICE_AUTH }}
      SLACK_WEBHOOK_URL: ${{ secrets.SLACK_WEBHOOK_URL }}
```

`toggle_harvester.yml` already exists and is already `workflow_call`-able. Its contract
(`.github/workflows/toggle_harvester.yml`):

| input | type | required | default |
| --- | --- | --- | --- |
| `state` | string | yes | — (`check` \| `disable` \| `enable`) |
| `max_tasks` | string | no | `"3"` |
| `environment` | string | yes | — |
| `notification` | boolean | no | `true` |

It declares an explicit `secrets:` block with `CF_SERVICE_USER` and `CF_SERVICE_AUTH`
**required**, so pass them explicitly — `secrets: inherit` is not sufficient here, and
this is the one callee in the chain that differs from `migrate_opensearch_cluster.yml`
(which deliberately declares none and requires `inherit`).

### 2. Chain `create-services` behind it

`create-services` currently has no `needs`. It becomes:

```yaml
  create-services:
    needs: disable-harvester
    # disable-harvester is skipped on a plain deploy, and a job whose needs were skipped
    # is itself skipped before its `if:` is evaluated -- so the skipped case must be
    # accepted explicitly or no deploy would ever run.
    if: >-
      ${{ !cancelled()
      && contains(fromJson('["success","skipped"]'), needs.disable-harvester.result) }}
```

This is the trap already hit twice in this repo (`migrate_opensearch_cluster.yml:456-461`,
and both `deploy-prod` and `release-prod`). Getting it wrong here breaks **every** deploy,
not just labelled ones.

### 3. New last job: `enable-harvester`

```yaml
  enable-harvester:
    name: Re-enable harvesting (${{ inputs.space }})
    needs: [disable-harvester, deploy, await-rollout, reindex]
    # `always()`, not `!cancelled()`: harvesting must resume even if the deploy failed or
    # the run was cancelled. Because the disable now lives OUTSIDE migrate, re-enabling
    # is this workflow's responsibility too -- migrate's own enable-harvester only covers
    # the part it disabled itself.
    if: always() && needs.disable-harvester.result == 'success'
    uses: ./.github/workflows/toggle_harvester.yml
    with:
      state: enable
      max_tasks: ${{ inputs.max_tasks }}
      environment: ${{ inputs.space }}
    secrets:
      CF_SERVICE_USER: ${{ secrets.CF_SERVICE_USER }}
      CF_SERVICE_AUTH: ${{ secrets.CF_SERVICE_AUTH }}
      SLACK_WEBHOOK_URL: ${{ secrets.SLACK_WEBHOOK_URL }}
```

Gate on `disable-harvester.result == 'success'` so a plain deploy (where the disable was
skipped) does not gratuitously re-enable and reset `HARVEST_RUNNER_MAX_TASKS`.

`inputs.max_tasks` already exists on `release-space.yml` and is already threaded to
migrate — reuse it, do not add a new input.

### 4. Leave `migrate_opensearch_cluster.yml` alone

Its internal `disable-harvester` / `enable-harvester` stay. Two reasons: the standalone
dispatch path must keep working unchanged, and the disable is idempotent —
`bin/set_harvest_runner_capacity.sh` just sets `HARVEST_RUNNER_MAX_TASKS` via
`cf set-env`, so setting it twice is a no-op.

One consequence to be aware of, not a bug: migrate's inner `enable-harvester` runs when
the *rebuild* finishes, and `release-space.yml`'s outer one runs when the *release*
finishes. Both set the same value from the same `max_tasks`, so the end state is correct
either way; the outer call is the one that guarantees it on a failed deploy.

## Concurrency — nothing changes, and here is why

`toggle_harvester.yml` holds `toggle-harvester-${{ inputs.environment }}` at job level.
That group is **distinct from every other group in the chain**, which is exactly why
`migrate_opensearch_cluster.yml:192-193` records that calling toggle is safe while calling
`rebuild_opensearch_index.yml` would deadlock.

Full group inventory after this change — all distinct, no workflow-level collision:

| workflow | group | level |
| --- | --- | --- |
| `commit.yml` | `release-develop` | job (on the `uses:` job) |
| `deploy.yml` | `release-main` | workflow |
| `release-space.yml` | *none* | — |
| `release-space.yml` jobs | `opensearch-maintenance-<space>` | job |
| `migrate_opensearch_cluster.yml` | `opensearch-maintenance-<env>` | workflow |
| `toggle_harvester.yml` | `toggle-harvester-<env>` | job |

`release-space.yml` must continue to hold **no** workflow-level group: it calls migrate,
which takes `opensearch-maintenance-<space>` at its own workflow level, and a hold above
would queue the child behind its own ancestor forever.

Nesting after this change is 4 levels at most
(`deploy.yml → release-space.yml → migrate → toggle`), against a documented limit of ten.
The new `disable`/`enable` branch is only 3.

## What this does NOT fix

State this in the PR description; it is the difference between "writes are safe" and
"the release is safe".

Disabling harvesting stops the **writes**. It does not stop the **reads**: from the
moment the deploy lands until the promote, the new code — and catalog — still query the
*old* index. On a breaking mapping change that means wrong results for the full
provision + rebuild duration (~20 min on `es-medium`, hours on a prod `es-large`).

Closing that needs the parked two-phase path: `migrate_opensearch_cluster.yml` supports
`stop_after: verify`, which builds and verifies the new cluster and moves nothing, so a
human promotes when both apps are ready. **`release-space.yml` currently hardcodes
`stop_after: decommission` and exposes no `stop_after` input, so the automated label path
cannot park.** Follow-up work, tracked separately: thread `stop_after` through and select
it from a second label (a `breaking index change` label, alongside the existing
`force re-index recommended` and `required db migration`).

## Verification

1. **Static** — `actionlint .github/workflows/release-space.yml .github/workflows/commit.yml .github/workflows/deploy.yml`.
   It catches reusable-workflow input and `needs`/`if` errors that otherwise only appear
   at runtime. Also confirm `toggle_harvester.yml` still receives all three secrets: a
   missing required secret on a `uses:` job fails the run as `startup_failure` with **no
   log and no annotation**, and actionlint does not model it (see
   `memory/reusable-workflow-permission-cap.md` for the same class of failure).
2. **The regression that matters most — a plain, unlabelled deploy must still work.**
   Push any commit to `develop` with no reindex label and confirm `disable-harvester`
   **skips**, `create-services` and `deploy` still **run**, and `enable-harvester`
   **skips**. If step 2's `if:` is wrong this is where it shows, and it would otherwise
   break every deploy in the repo.
3. **Ordering, labelled** — merge a labelled PR to `develop` and confirm from the run
   graph that `Disable harvesting (development)` completes **before**
   `create services (development)` starts. Check the timestamps, not just the order in
   the UI.
4. **Failure path** — confirm harvesting is re-enabled when the deploy fails. Easiest
   without breaking anything real: temporarily point the deploy at a nonexistent space in
   a scratch branch, or inspect a genuinely failed run. `cf env datagov-harvest | grep
   HARVEST_RUNNER_MAX_TASKS` must come back to `3`, not `0`.
5. **Both paths** — the change is in `release-space.yml`, so `deploy.yml` inherits it
   with no edit. Confirm with a `dry_run: true` dispatch of **2 - Deploy** that the
   staging and prod calls still resolve their inputs (the dry run skips the reindex, so
   `disable-harvester` should skip too).

## Files

| File | Change |
| --- | --- |
| `.github/workflows/release-space.yml` | add `disable-harvester` (first) and `enable-harvester` (`always()`); chain `create-services` behind the disable with an explicit skipped-needs `if:` |
| `.github/workflows/commit.yml` | **none** — inherits via `release-space.yml` |
| `.github/workflows/deploy.yml` | **none** — inherits via `release-space.yml` |
| `.github/workflows/migrate_opensearch_cluster.yml` | **none** — keep its internal toggle for the standalone dispatch path |
| `docs/ops/migrate-opensearch-cluster.md` | note that a labelled release pauses harvesting for its whole duration, and that reads still hit the old index until the promote |
