# Migrating to a new OpenSearch cluster

How to rebuild the dataset index on a brand-new cloud.gov OpenSearch cluster and
move `datagov-harvest` and `datagov-catalog` onto it, with no degradation of live
search and no downtime.

## Why this exists

`flask search rebuild-index` drops the `datasets` index and refills it from
PostgreSQL, so the cluster it runs against returns nothing until the backfill
finishes — and bulk-indexing the whole dataset table competes with catalog queries
for CPU and I/O the whole time.

Rebuilding on a separate cluster removes both problems: the live cluster receives
no rebuild traffic at all and keeps serving its existing index at full speed.
Cutting over afterwards is a `cf set-env` plus a rolling restart.

**Every rebuild should therefore target a replacement cluster.** `--cluster live`
still exists, but it takes search down for the duration and is only appropriate
where that is acceptable.

This is also the only way to **resize**. The cloud.gov broker does not support
changing an instance's plan (`cf update-service` can change the engine version,
not the plan), so a bigger cluster necessarily means a new instance.

## How it works

- **`OPENSEARCH_SERVICE_NAME`** — `.profile` resolves the live cluster's
  credentials from the *bound service instance of this name*, defaulting to
  `datagov-catalog-opensearch`. Cutover flips this variable. Rollback flips it
  back. Nothing else changes.
- **The replacement cluster is `<canonical>-next`** — a fixed name, derived by
  `.profile` from `OPENSEARCH_SERVICE_NAME`, exactly like `-db` and `-secrets`.
  Nothing sets it: **binding an instance by that name is the entire handoff.**
  When one is bound, `.profile` exports `OPENSEARCH_NEXT_HOST`,
  `OPENSEARCH_NEXT_ACCESS_KEY`, and `OPENSEARCH_NEXT_SECRET_KEY`; when none is,
  those stay empty and `--cluster next` fails cleanly.

  No `cf set-env` and **no restart** are needed to reach it. The rebuild runs via
  `cf run-task`, and a task starts a fresh container that reads current bindings —
  verified in staging on 2026-08-10, where a task resolved a newly bound
  replacement with no variable set and no restart performed. (A long-running *web*
  instance still needs a restart to notice a change, which is why the promote's
  rolling restarts remain mandatory.)
- **`flask search rebuild-index --cluster next`** builds the index against those
  `NEXT` credentials. `--cluster live` (the default) is unchanged from before.
- **The index name stays `datasets`.** Index names are scoped to a cluster, so
  there is nothing to collide with — catalog needs no query changes, only a
  different host.

### There is no index to coordinate

Worth being explicit, because it is the part that sounds hardest: **the index is
called `datasets` on every cluster.** Both apps read `INDEX_NAME = "datasets"` from
their vendored copy of the same search code, and index names are scoped to a cluster,
so a replacement cluster has nothing to collide with. Nothing to tell catalog,
nothing to deploy in lockstep, no window where the two apps disagree about which
index to read.

The only thing the two apps must agree on is **which cluster**, and that is one
environment variable per app. Which is why the whole cutover is a single workflow
run.

### The whole migration is one workflow

Dispatch **Migrate OpenSearch Cluster** and it runs the entire thing:

| # | Stage | What it does |
| --- | --- | --- |
| 1 | disable harvesting | calls **Toggle Harvester** |
| 2 | provision | `cf create-service` + bind both apps + expose the credentials to the harvester |
| 3 | drain | waits for in-flight harvest jobs |
| 4 | rebuild | `rebuild-index --cluster next` into the new cluster |
| 5 | verify | `compare --cluster next` — must be 0 missing, 0 extra, 0 updated |
| 6 | promote | renames the new cluster to the canonical name, then restarts both apps |
| 7 | verify | resolved host per app, then `compare --cluster live` |
| 8 | decommission | deletes the cluster it replaced |
| 9 | re-enable harvesting | always, even if something above failed |

The migration is complete the moment stage 7 passes. Stage 8 runs as its own job because
`cf delete-service --wait` blocks until AWS has torn down every node — over 10 minutes
for a development `es-medium`, longer for a 5-node `es-large` — and a slow teardown of
an already-unused cluster must not be reported as a failed migration.

Everything through stage 5 leaves the live cluster **completely untouched**, so it
keeps serving search at full speed and any failure there costs nothing: the
half-built replacement is deleted automatically and the live cluster is exactly as
it was.

Deletion happens **last**, after both verification gates, so the replaced cluster
stays available for rollback through every risky step. Pass
`keep_old_cluster: true` to skip it entirely and delete by hand later.

The one-run default is what you want for a routine migration or a resize. For a
**schema-breaking change**, use [two-phase mode](#two-phase-mode-schema-breaking-changes).

Two properties make this safe:

- `cf bind-service` does **not** affect a running app. Cloud Foundry only
  refreshes `VCAP_SERVICES` on restart or push, so the new cluster can be
  provisioned and bound days ahead with zero risk.
- Manifest application is **additive** — it does not remove existing bindings —
  so the replacement instance is bound out-of-band and deliberately *not* listed
  in `manifest.yml` (a manifest cannot reference an instance that does not exist).

### Two-phase mode: schema-breaking changes

When the new mapping is incompatible with the currently deployed code, both apps have
to ship that code *before* they read the new cluster. Dispatch twice:

```
run 1   stop_after: verify     build and verify, then STOP
        ── deploy harvester and catalog code carrying the new schema ──
run 2   start_at: cutover      promote, verify, delete
```

After run 1 the new cluster is built, bound, verified, and **parked**: both apps are
still serving the old cluster and nothing reads the new one. That state is safe
indefinitely — binding does not affect a running app — so run 1 can happen days
ahead. Rollback during the gap is "do nothing". The only cost is paying for two
clusters. Run 1 prints the exact command to finish.

The new cluster carries whatever mapping the *deployed* harvester code had when the
rebuild ran, so the two schemas coexist: old cluster with the old mapping serving
live, new cluster with the new mapping waiting.

> **Why the split is before the promote and not inside it.** `datagov-catalog`'s
> `.profile` resolves `${APP_NAME}-opensearch` — literally
> `datagov-catalog-opensearch` — and does **not** read `OPENSEARCH_SERVICE_NAME`, so
> the rename is the only thing that can move catalog. Stopping between moving the
> harvester and renaming would leave the harvester writing the new schema to one
> cluster while catalog reads the old schema from another, and dataset changes would
> silently not appear in search. Both apps move together, in run 2, or neither moves.

## Before you start

- [ ] This repo deployed with the `OPENSEARCH_SERVICE_NAME` indirection in the
      target space.
- [ ] Quota headroom for a second cluster of the same size. A duplicate prod
      `es-large` is a real cost; confirm with cloud.gov it fits the
      `gsa-datagov` quota before provisioning.
- [ ] Rehearsed end to end in `development`, then `staging`, including a
      rollback.
- [ ] For staging and prod only: confirmed the new cluster's hostname is reachable
      from `datagov-catalog`. See the egress-proxy note below.

**Neither app is repointed.** Both resolve the canonical service-instance name, so the
promote stage renames the *cluster* underneath that name rather than telling the apps to
look elsewhere: `<canonical>` → `<canonical>-old`, then `<next>` → `<canonical>`. Two
renames and two restarts, no `cf set-env` on either app.

The restarts are mandatory, not cosmetic. A rename is metadata-only — the AWS endpoint
and credentials do not change — and `.profile` resolves the host exactly once, at
container start, so a running app keeps using the endpoint it captured at boot and would
stay on the old cluster indefinitely without one.

Catalog therefore needs no code change, and `cf set-env datagov-catalog
OPENSEARCH_SERVICE_NAME` does nothing at all.

> **Check catalog's egress proxy in staging and prod.** This repo's `.profile`
> excludes both OpenSearch hosts from the egress proxy; catalog's sets `no_proxy` to
> `.apps.internal` only, so where a proxy is attached, catalog's OpenSearch traffic
> goes *through* it — and the proxy has a hostname allowlist maintained outside both
> repos. Every new cluster gets a new broker-generated hostname, and a rename does
> not preserve it.
>
> In `development` this does not apply: catalog runs with no egress proxy attached
> (`no_proxy` and `https_proxy` are both empty inside the container), so it reaches
> any cluster directly. Verify per space before promoting rather than assuming —
> either get the new hostname allowlisted, or change catalog's `.profile` to match
> this repo's. Otherwise catalog search breaks the moment the rename lands and
> nothing in either repo explains why.
>
> `bin/report_opensearch_cluster.sh datagov-harvest datagov-catalog` is the fastest
> check that both apps actually resolved the cluster you expect.

Throughout, `<space>` is `development`, `staging`, or `prod`, and the new instance
is `datagov-catalog-opensearch-next` (named after catalog, like the live one,
because both apps bind it).

## Automatic reindex on merge

**Label a PR `force re-index recommended` and the index rebuilds itself** after each
space deploys — development on a merge to `develop`, and staging then prod on a merge to
`main`. Use it whenever the change touches the mapping or the document shape:
`search/mappings.py`, `search/config.py` (`SETTINGS`), `search/documents.py`,
`search/transforms.py`, `search/spatial.py`, or a `Dataset` column the transformer
reads.

Nothing else detects this. `MAPPINGS` carries no version or hash, and
`OpenSearchClient._ensure_index()` only creates an index when one is *absent* — so
deploying a mapping change against an existing index is a **silent no-op** and search
quietly serves the old shape.

### One pipeline, three spaces

`release-space.yml` is the per-space release — create services → push → wait for the
rollout → rebuild the index → network policies. It is called once by `commit.yml` for
`development` and twice by `deploy.yml` for `staging` then `prod`, so all three spaces
run identical logic. The only per-space differences are `on_build_failure` (`keep` in
prod, where re-provisioning an `es-large` costs hours; `delete` elsewhere, so a failure
self-heals) and `force_kill_running_jobs` (`false` in prod — long harvest jobs are not
cancelled unattended).

### Each release path is a queue

| path | queue | scope |
| --- | --- | --- |
| `develop` → development | `release-develop` (job level) | that one release |
| `main` → staging + prod | `release-main` (workflow level) | **both** spaces in one hold |

`release-main` has to be workflow-level because that run spans two spaces: a job-level
group would be released between staging and prod and let a second merge's staging deploy
slip into the gap. `release-develop` is job-level on purpose — `commit.yml` also runs
`lint`/`test` on *every* branch, and a workflow-level hold there would serialize
unrelated PRs' CI.

A second merge to the same branch waits for the whole release. That is deliberate: a
`cf push` landing mid-migration is what broke a staging run on 2026-08-10, redeploying
the app and removing the `--cluster` flag the in-flight rebuild depended on. The two
paths are independent, so a development migration no longer blocks a staging/prod
release — within a single space, overlap is prevented by the
`opensearch-maintenance-<space>` job groups inside `release-space.yml`, which also cover
hand-dispatched migrations and the restart cron.

Consequence worth knowing: **a labelled merge blocks later merges to that branch for the
length of its migrations** — hours, for a prod `es-large`. An urgent hotfix queues behind
it. Dispatch the workflow manually with `reindex: skip` to jump that queue, and only
cancel a running pipeline *before* a promote begins.

### How the label is detected

Not from the push's own commit range. A release queue allows one pending run, and a
third merge *cancels* the pending one — so a labelled merge could be superseded and its
reindex lost. Instead `detect-reindex.yml` measures from the `head_sha` of the last
**successful** run of the *calling* workflow (`deploy.yml` on `main`, `commit.yml` on
`develop`), and a cancelled or failed run never advances that watermark. The obligation
stays detectable until a run actually completes.

Detection lives in the caller, not in `release-space.yml`, for a concrete reason: a
workflow invoked via `uses:` produces no workflow run of its own — the API attributes it
to the caller — so a reusable file has no run history to measure from.

`.github/scripts/detect-reindex-label.sh` **fails closed**: no watermark, rewritten
history, or a range truncated past the compare API's 250-commit cap all stop the deploy
rather than guess. Nothing is lost when it refuses — dispatch manually with
`reindex: force` or `reindex: skip` to state the intent. If a reindex is owed but does
not finish, the run opens a tracking issue from `.github/reindex_owed.md`.

### If someone forgot the label

Dispatch the workflow for that branch by hand — **1 - Commit** for `develop`,
**2 - Deploy** for `main` — with `reindex: force`. Both take the same inputs:

| input | effect |
| --- | --- |
| `reindex: auto` | read the PR labels (the default, same as a merge) |
| `reindex: force` | rebuild regardless — for a forgotten label |
| `reindex: skip` | deploy only, jumping the reindex queue |
| `dry_run: true` | report what the reindex *would* do; no cluster is touched |

### Before a labelled merge reaches prod

**Confirm `GSA/datagov-catalog` is running code that matches the new mapping.** The
harvester writes the new document shape; if catalog still expects the old one, search
returns wrong results and `compare` cannot see it — it checks id sets and
`last_harvested_date`, never document shape. Each promote says this in Slack, and probes
catalog's `/search` afterwards, warning if a common term returns zero results.

Every space promotes automatically. `development` is the cheapest rehearsal: `es-medium`
is the fastest plan and catalog there runs with no egress proxy, so merging a labelled PR
to `develop` first exercises the entire flow at low cost.

## Running it manually

Actions → **Migrate OpenSearch Cluster** → *Run workflow*:

| Input | Use |
| --- | --- |
| `environment` | the target space |
| `start_at` | `provision` normally; `rebuild` or `cutover` to resume a failed run |
| `stop_after` | `decommission` normally; `verify` for [two-phase mode](#two-phase-mode-schema-breaking-changes) |
| `next_service_name` | the replacement instance, default `datagov-catalog-opensearch-next` |
| `next_plan` | blank matches the live plan; set it to **resize** |
| `keep_old_cluster` | `true` keeps the replaced cluster for a manual soak |
| `on_build_failure` | `delete` removes a failed replacement (default); `keep` retains it so you can resume with `start_at: rebuild` |
| `force_kill_running_jobs` | cancel harvest jobs still running after 15 minutes |
| `max_tasks` | `HARVEST_RUNNER_MAX_TASKS` to restore afterwards (`3` for prod) |
| `max_failed_records` | how many records may fail to index and still pass, default `50` — set it to the number of bad records you expect. Used by the rebuild *and* both verifications. `0` requires an exact match. Failed ids are always logged in full — see [step 5](#5-verify-before-cutting-over) |

**Resuming.** Use `start_at` to skip stages already done — that is what makes a
re-dispatch safe, not blanket idempotence. Provisioning deliberately **refuses** when
the replacement instance already exists, so a run that got past that stage must be
resumed with `start_at: rebuild` (or `cutover`) rather than from the beginning. The
refusal is the point: two rebuilds writing into one cluster interleave into an index
that verifies as garbage. It also skips the expensive part — a prod `es-large`
provision can take a couple of hours.

**If it fails.** Before the promote, the replacement cluster is deleted automatically
and the live cluster is untouched — just fix the cause and re-dispatch. On a slow space
where re-provisioning costs hours, pass `on_build_failure: keep` so the cluster survives
and you can resume with `start_at: rebuild`. *During* the
promote, the workflow deliberately leaves everything in place, because renames may be
half-applied: run
`bin/report_opensearch_cluster.sh datagov-harvest datagov-catalog` to see where each app
actually landed, then finish or reverse the renames by hand. Harvesting is re-enabled
either way.

The rest of this document is the by-hand equivalent — useful for understanding what the
workflow does, for recovering from a partial failure, and for rollback.

## 1. Provision the cluster

AWS quotes roughly 15–30 minutes **per node**, so an `es-large` (5 nodes) can take
a couple of hours. Start well ahead of the window.

```bash
cf target -o gsa-datagov -s <space>

# Match the live plan, or size up -- this is the only chance to change it.
#   prod: es-large   staging: es-medium-ha   development: es-medium
cf create-service --wait aws-elasticsearch <plan> datagov-catalog-opensearch-next \
  -c '{"ElasticsearchVersion":"OpenSearch_2.11"}'

cf bind-service datagov-harvest datagov-catalog-opensearch-next
cf bind-service datagov-catalog datagov-catalog-opensearch-next
```

Or run `CREATE_OPENSEARCH_NEXT=1 bash create_cloudgov_services.sh`, which
provisions and binds both apps (set `OPENSEARCH_NEXT_PLAN` to override the plan).

Both apps are bound now, and both are still using the live cluster — binding is
inert until restart. Confirm with `cf services`; the cutover in step 6 re-checks
and refuses to proceed if either app is unbound.

If the space has never had egress configured:
`cf bind-security-group trusted_local_networks_egress gsa-datagov --space <space>`.

## 2. Confirm the harvester can reach the new cluster

There is nothing to set. The instance is named `datagov-catalog-opensearch-next`,
which is what `.profile` derives and looks for, so the bind in step 1 already
exposed it.

```bash
# Confirm the harvester resolved BOTH clusters.
bin/report_opensearch_cluster.sh datagov-harvest
```

No restart either: the rebuild runs as a `cf run-task`, and a task starts a fresh
container that reads current bindings. (`report_opensearch_cluster.sh` reads a
running web container, so `OPENSEARCH_NEXT_HOST` may show empty there until the
app is next restarted — that is expected and does not affect the rebuild.)

Elsewhere in this runbook `--strategy rolling` is not optional; a plain
`cf restart` takes the app down. Catalog needs nothing yet.

> Use `bin/report_opensearch_cluster.sh` rather than `cf env` to check the
> resolved host. `cf env` shows only user-provided variables, `VCAP_*`, and
> env-var groups — **it does not show `OPENSEARCH_HOST`**, which `.profile`
> exports inside the container. `cf env` will confirm
> `OPENSEARCH_SERVICE_NAME` (which you set with `cf set-env`) but not what it
> resolved to. The script reports both.

> Both apps also restart on a schedule — `datagov-harvest` every 10 minutes,
> `datagov-catalog` every 15 (the **Restart Apps** workflow). That is harmless
> here: a cron restart just re-reads whatever `OPENSEARCH_SERVICE_NAME` currently
> says, so it can only ever apply a value you already set deliberately. Expect it
> to make log timelines noisier than the steps below imply. It does mean an app
> can pick up an env change *before* you restart it yourself, so treat every
> `cf set-env` as effective immediately.

## 3. Pause the writers

Two things write to OpenSearch unattended and both must stop:

1. **Harvesting** — run **Toggle Harvester** → `disable` for `<space>`.
2. **The nightly sync** — for `staging` and `prod` only, set the repository variable
   `OPENSEARCH_SYNC_PAUSED_ENVIRONMENTS` to include `<space>` (comma-separated).
   Otherwise the 6am `compare --update` cron writes into whichever cluster is
   live mid-migration.

   Do **not** put `development` in that variable: the nightly sync is only scheduled
   for `staging` and `prod`, and `synchronize_opensearch_index.yml` validates the
   value against exactly those two names and *fails the workflow* on anything else.
   A `development` migration needs no sync pause.

Then wait for in-flight jobs to drain. The rebuild workflow does this for you
(`bin/wait_for_harvest_tasks.sh`); it is listed here because a manual run must
not skip it.

> Announce a **dataset slug-edit freeze** for the window. Slug edits from the
> admin UI do not update `last_harvested_date`, so `compare` cannot detect them —
> a slug changed mid-migration would silently stay stale in the new cluster, and
> `slug` is catalog's dataset URL. If one does slip through, find it in the
> mutation audit log and re-index that id afterwards.

## 4. Rebuild into the new cluster

Run the **Rebuild OpenSearch Index** workflow with:

- `environment`: `<space>`
- `cluster`: **`next`**

Confirm in the task log that it names the *new* host — this line is the proof the
live cluster was left alone:

```
Target cluster: next (<new host>)
```

A fresh cluster starts with no `datasets` index. `rebuild-index` connects with
automatic index creation disabled, then creates the index once with the current
mapping, the extended 300-second timeout, and idempotent timeout recovery before
backfilling. It finishes with:

```
Rebuild complete: datasets is ready on the next cluster.
```

Live search is unaffected for this entire step. Watch catalog's latency in New
Relic to confirm.

## 5. Verify before cutting over

```bash
cf run-task datagov-harvest -k 2G -m 3G --name os-verify \
  --command "flask search compare --cluster next"
bin/monitor_cf_logs.sh datagov-harvest os-verify
```

**Must report 0 extra and 0 updated.** Those never get a tolerance: an extra
document means a delete did not happen and a stale one means an update did not, and
neither is explained by bad source data. Use `compare --cluster next --update` to
repair, then re-verify. (3G because `compare` holds every DB id and every OpenSearch
id in memory at once.)

**Missing documents get an allowance** — `--max-failed-records`, default `50`. Set
it to the number of bad records you expect. Some records simply cannot be indexed:
one staging dataset carries an empty-string key in its `dcat` JSON, which OpenSearch
rejects with `mapper_parsing_exception`, so no rebuild will ever land it. Without an
allowance, one such record blocks the migration permanently — which is exactly what
happened on 2026-08-10.

It is an absolute count, not a percentage, deliberately. The number you type is the
number of failures that will pass, at any corpus size. A percentage of a large corpus
silently authorizes far more than any real backlog (1% of 548k is ~5.5k), which is
enough to hide a systemic indexing failure.

The same number is passed to **both** `rebuild-index` and `compare`, and they must
agree — otherwise the gate rejects precisely the records the rebuild was designed to
skip. The workflow does this from one input for that reason; if you run the commands
by hand, pass the same value to each.

Tolerated does not mean unnoticed. Every missing id is printed in full under
`MISSING DATASET IDS (not in OpenSearch)`, with the count and the percentage it
represents, so they become a backlog rather than a surprise. The rebuild log's
`SKIPPED DATASET IDS (not indexed)` block gives the reason for each. Grep either
banner in the task log:

```bash
# Why a given id was rejected
cf logs datagov-harvest --recent | grep -A20 "SKIPPED DATASET IDS"
```

Pass `--max-failed-records 0` to require an exact match. Raise it only after
checking *why* the extra records were rejected — a jump in the count is the signal
that something systemic broke, and a large allowance suppresses that signal.

## 6. Cut over

Run the **Cutover OpenSearch Cluster** workflow:

- `environment`: `<space>`
- `service_name`: `datagov-catalog-opensearch-next`
- `apps`: `datagov-harvest datagov-catalog` (the default)

It moves both apps in one step — pre-flights that the instance exists and is
bound to every app, then per app sets `OPENSEARCH_SERVICE_NAME`, does a blocking
rolling restart, and reports the resolved host. It logs each app's *previous*
value, which is what you pass back to roll back.

The equivalent by hand:

```bash
bin/cutover_opensearch_cluster.sh datagov-catalog-opensearch-next \
  datagov-harvest datagov-catalog
```

Apps move in the order given, each fully rolled before the next. Harvester first
is deliberate: it is the only writer, so moving it first means no write lands on
the cluster being left behind once harvesting resumes. Both restarts are rolling,
so neither app drops traffic, and harvesting is still disabled so the gap between
them carries no risk.

If `.profile` cannot find the named instance it **fails the start** rather than
booting an app that silently indexes nothing. A failed rolling restart leaves the
old instances serving traffic — check `cf logs --recent` and fix the name. The
binding pre-check exists to catch that before anything changes.

## 7. Verify, then resume

- `bin/report_opensearch_cluster.sh datagov-harvest datagov-catalog` — both apps
  resolve the new host.
- Catalog search returns results: a keyword query, a facet, and a dataset
  permalink (proves `slug` is right).
- Harvester UI loads and dataset pages render.
- No OpenSearch error-rate spike for either app in New Relic.

> **New Relic shows the new cluster as a different external service.**
> `opensearch-py` is not auto-instrumented as a datastore, so OpenSearch calls
> appear as generic external HTTP calls keyed on the endpoint hostname. The AWS
> endpoint is broker-generated (`cg-broker-<prefix>-<random>`) and unrelated to
> the CF instance name, so **any** new cluster gets a new hostname — renaming the
> instance does not preserve continuity.
>
> Nothing in either repo needs changing: there are no committed dashboards,
> alerts, or custom instrumentation, and no config names the cluster. But any
> saved view, filter, or chart *you built in the New Relic UI* pinned to the old
> external host will go blank. Check those manually, and look at the new host when
> reviewing error rates above.

Then:

1. **Toggle Harvester** → `enable` (`max_tasks` = `3` for prod).
2. Clear `<space>` from `OPENSEARCH_SYNC_PAUSED_ENVIRONMENTS` if you set it — if you
   forget, the nightly sync stays off indefinitely.
3. Re-index any dataset ids whose slugs were edited during the window.

## Rollback

Re-run the **Cutover OpenSearch Cluster** workflow with the *old* instance name —
rollback is the same operation, not a separate procedure:

- `service_name`: `datagov-catalog-opensearch`
- `apps`: `datagov-catalog datagov-harvest` (catalog first here, so reads move
  back before the writer does)

Or by hand:

```bash
bin/cutover_opensearch_cluster.sh datagov-catalog-opensearch \
  datagov-catalog datagov-harvest
```

Rolling back **before** harvesting resumes is clean — the old cluster is exactly
as you left it.

Rolling back **after** harvesting has run on the new cluster leaves the old one
stale by however long you ran. Restore availability with the restarts above, then
catch it up:

```bash
cf run-task datagov-harvest -k 2G -m 3G --name os-rollback-sync \
  --command "flask search compare --update"
```

This stays available for as long as you keep the old instance — which is the
reason not to delete it for a week or two.

## Decommission (delete old, rename new to canonical)

> **The workflow does this for you**, in an order that avoids the failure window
> described below: it moves the harvester explicitly, renames old→`-old` and
> new→canonical back to back, then *clears* the harvester's overrides so it resolves
> the canonical name via the `.profile` default, and finally restarts catalog. The
> old cluster is deleted last, after both verification gates.
>
> `bin/promote_opensearch_cluster.sh` and `bin/delete_opensearch_cluster.sh` do
> exactly that and can be run directly. The manual sequence below is retained for
> recovering from a partial failure and for understanding the hazard.

Do this after a soak period, once you are certain you will not roll back — or let the
workflow do it immediately, which is the default. `keep_old_cluster: true` gives you
the soak while still automating everything else.

The goal is to end with the new cluster holding the canonical name
`datagov-catalog-opensearch` and no env overrides, so steady state matches what
it was before the migration.

> **Read this first — the naive sequence has a real failure window.**
>
> If you rename before repointing, both apps' `OPENSEARCH_SERVICE_NAME`
> names `datagov-catalog-opensearch-next`, which no longer exists.
>
> - **Running instances are fine.** `VCAP_SERVICES` is read at container start
>   and the rename is metadata-only — the AWS endpoint and credentials do not
>   change — so live traffic is unaffected.
> - **New container starts fail.** `.profile` cannot resolve the old name, hits
>   the empty-host guard, and exits non-zero.
> - **Both apps restart on a cron** — `datagov-harvest` every 10 minutes,
>   `datagov-catalog` every 15 — so this window *will* be hit unless you pause
>   them. `--strategy rolling` means the old instances keep serving rather than
>   an outage, but you get a failed deployment, and `bin/check-and-renew` then
>   skips subsequent cron restarts because it sees an ACTIVE deployment.
>
> Step 4 below closes the window; step 1 removes the cron risk. Do not skip
> either. If a deployment does fail, it is recoverable — finish step 4 and
> restart both apps.

**1. Pause the restart crons.** Disable the **Restart Apps** workflow in this
repo and in `GSA/datagov-catalog` (Actions → the workflow → *Disable workflow*).
This is what makes the window controllable rather than a race.

**2. Delete the old cluster** to free the canonical name.

```bash
cf unbind-service datagov-harvest datagov-catalog-opensearch
cf unbind-service datagov-catalog datagov-catalog-opensearch
cf delete-service datagov-catalog-opensearch
```

Wait for `cf service datagov-catalog-opensearch` to report the instance is gone
before continuing — the rename fails while the name is still taken.

**3. Rename the new cluster.** Bindings survive a rename, so both apps stay bound.

```bash
cf rename-service datagov-catalog-opensearch-next datagov-catalog-opensearch
```

The failure window opens here.

**4. Repoint both apps and close the window.** Run the **Cutover OpenSearch
Cluster** workflow with `service_name: datagov-catalog-opensearch`, or:

```bash
bin/cutover_opensearch_cluster.sh datagov-catalog-opensearch \
  datagov-harvest datagov-catalog
```

Either way this sets `OPENSEARCH_SERVICE_NAME` to the canonical name and rolls
both apps. The window is now closed.

**5. Clean up and re-enable.**

There is no replacement pointer to clear. The rename in step 3 moved the instance
off the `-next` name, and `.profile` derives that name rather than reading a
variable, so `--cluster next` already resolves to nothing.

Optionally `cf unset-env` `OPENSEARCH_SERVICE_NAME` on both apps — it now
matches the `.profile` default, so removing it returns to a bare steady state.
Do this *only after* step 4 has completed successfully, and follow it with one
more rolling restart of each app.

Then re-enable the **Restart Apps** workflows in both repos, and confirm:

```bash
bin/report_opensearch_cluster.sh datagov-harvest datagov-catalog
```

Both apps should resolve the new cluster's host under the canonical name.

Finally, delete the leftover physical index from any aborted rebuild with the
**Delete OpenSearch Physical Index** workflow.

### The lower-risk alternative

If you would rather not have a failure window at all: keep the new instance's
name as-is permanently and leave `OPENSEARCH_SERVICE_NAME` pointed at it. Steps
2 and 5 are all you need, with no rename and no window. The only cost is that the
live instance is called `...-next`, which reads oddly; provisioning the next
generation as `-v3` (etc.) avoids that going forward. This trades a cosmetic wart
for removing the one genuinely risky step in the migration.

Either way, step 5's `unset-env` still applies — `--cluster next` must not resolve
to whatever is currently live.

Also delete the leftover physical index from any aborted rebuild with the
**Delete OpenSearch Physical Index** workflow.

## Catalog-side changes

`GSA/datagov-catalog` is the main read consumer. It binds the *same* instance
(`manifest.yml` → `((app_name))-opensearch` = `datagov-catalog-opensearch`) and reads
the same three credential keys.

**Nothing is required on the catalog side.** Catalog's `.profile` resolves
`${APP_NAME}-opensearch` — literally `datagov-catalog-opensearch` — so it always reads
whichever instance currently holds the canonical name. The promote stage's rename is
what moves it; it just needs the restart that the workflow performs. No `--cluster`
plumbing, no `OPENSEARCH_NEXT_*`, no query or index-name changes (`datasets` is
unchanged).

> **Corollary worth knowing: `cf set-env datagov-catalog OPENSEARCH_SERVICE_NAME` does
> nothing.** Catalog does not read that variable. Any procedure that tries to move
> catalog by setting it will appear to succeed while leaving catalog on the old
> cluster — `bin/cutover_opensearch_cluster.sh` included, which is why it is a
> harvester-side tool. `bin/report_opensearch_cluster.sh` exposes the mismatch: the
> `cf env` value and the resolved host disagree.

Two optional improvements, neither required by the workflow:

1. **`.profile` `no_proxy`** — add `${OPENSEARCH_HOST}` so catalog's OpenSearch traffic
   bypasses the egress proxy, removing the hostname-allowlist dependency in spaces that
   have a proxy attached. See the warning under
   [Before you start](#before-you-start).
2. **`OPENSEARCH_SERVICE_NAME` indirection** — a `vcap_get_service_by_name` helper and
   the same empty-host guard as this repo. This would allow moving catalog without a
   rename, making the rename purely cosmetic rather than load-bearing.

## Emergency fallback: rename-service swap

If the `.profile` change is not deployed and you must move clusters now:

```bash
cf rename-service datagov-catalog-opensearch datagov-catalog-opensearch-old
cf rename-service datagov-catalog-opensearch-next datagov-catalog-opensearch
cf restart datagov-harvest --strategy rolling
cf restart datagov-catalog --strategy rolling
```

Avoid this when you have the choice:

- Both manifests name the instance literally, so between the two renames any
  `cf push` fails, and rollback means renaming twice more under pressure.
- **The cron restarts decide when it takes effect, not you.** Harvester restarts
  every 10 minutes and catalog every 15, so both apps pick up the swap on their
  own schedule, in whichever order the crons fire. You cannot move the harvester,
  verify writes, and only then move catalog — which is the main reason the
  env-var cutover in step 6 is preferred.
- There is no per-app audit trail: `cf env <app>` shows nothing about which
  cluster the app resolved.

## Related

| Workflow | Purpose |
| --- | --- |
| **Migrate OpenSearch Cluster** | **The whole migration in one dispatch — start here** |
| Rebuild OpenSearch Index | Rebuild only, without provisioning or promoting |
| Cutover OpenSearch Cluster | Point apps at a named instance; the rollback tool |
| Toggle Harvester | Pause and resume harvesting |
| Sync OpenSearch | `compare` / `compare --update`; nightly cron |
| Delete OpenSearch Physical Index | Remove a leftover `datasets-*` index |

| Script | Purpose |
| --- | --- |
| `bin/report_opensearch_cluster.sh` | Which cluster each app actually resolved (`cf env` cannot show this) |
| `bin/provision_opensearch_cluster.sh` | Create + bind the replacement cluster and expose it to the harvester |
| `bin/promote_opensearch_cluster.sh` | Move both apps and give the replacement the canonical name, in the window-free order |
| `bin/delete_opensearch_cluster.sh` | Unbind and delete an instance, refusing if any app still serves from it |
| `bin/cutover_opensearch_cluster.sh` | Check bindings, then set the env var and roll each app in order |
| `bin/lib/opensearch_plan.sh` | Plan per space and engine version, shared so clusters cannot differ |

- `docs/ops/README.md` — other operational notes
- `create_cloudgov_services.sh` — service provisioning
- Local testing: `docker compose --profile next-cluster up -d opensearch-next`,
  then set `OPENSEARCH_NEXT_HOST=opensearch-next`
