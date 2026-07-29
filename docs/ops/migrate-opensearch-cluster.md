# Migrating to a new OpenSearch cluster

How to rebuild the dataset index on a brand-new cloud.gov OpenSearch cluster and
move `datagov-harvest` and `datagov-catalog` onto it, with no degradation of live
search and no downtime.

## Why this exists

`flask search rebuild-index` is already zero-*downtime*: it builds a new physical
index, backfills from PostgreSQL, and atomically swaps the `datasets` alias, so
readers never see a missing index. But run against the live cluster it is not
zero-*impact* — bulk-indexing the whole dataset table competes with catalog
queries for CPU and I/O, and search latency suffers for the hours it takes.

Rebuilding on a separate cluster removes that entirely: the live cluster receives
no rebuild traffic at all. Cutting over afterwards is a `cf set-env` plus a
rolling restart.

This is also the only way to **resize**. The cloud.gov broker does not support
changing an instance's plan (`cf update-service` can change the engine version,
not the plan), so a bigger cluster necessarily means a new instance.

## How it works

- **`OPENSEARCH_SERVICE_NAME`** — `.profile` resolves the live cluster's
  credentials from the *bound service instance of this name*, defaulting to
  `datagov-catalog-opensearch`. Cutover flips this variable. Rollback flips it
  back. Nothing else changes.
- **`OPENSEARCH_NEXT_SERVICE_NAME`** — names a second bound instance. When set,
  `.profile` also exports `OPENSEARCH_NEXT_HOST`, `OPENSEARCH_NEXT_ACCESS_KEY`,
  and `OPENSEARCH_NEXT_SECRET_KEY`.
- **`flask search rebuild-index --cluster next`** builds the index against those
  `NEXT` credentials. `--cluster live` (the default) is unchanged from before.
- **The index name stays `datasets`.** Index names are scoped to a cluster, so
  there is nothing to collide with — catalog needs no query changes, only a
  different host.

### There is no index to coordinate

Worth being explicit, because it is the part that sounds hardest: **neither app
ever names an index.** Both read `INDEX_NAME = "datasets"` from the shared
`datagov_data_access` package, and `datasets` is an *alias*. The rebuild creates a
physical index (`datasets-<runid>-<attempt>`) and repoints the alias atomically
inside OpenSearch, so the physical name is an internal detail that never leaves
the cluster. Nothing to tell catalog, nothing to deploy in lockstep, no window
where the two apps disagree about which index to read.

The only thing the two apps must agree on is **which cluster**, and that is one
environment variable per app. Which is why the whole cutover is a single workflow
run.

### The whole migration, in steps

| # | Step | How |
| --- | --- | --- |
| 1 | Provision + bind the new cluster | one `cf create-service` + two `cf bind-service` (or the provisioning script) |
| 2 | Expose its credentials to the harvester | one `cf set-env` + rolling restart |
| 3 | Pause harvesting and the nightly sync | **Toggle Harvester**, one repo variable |
| 4 | Rebuild into the new cluster | **Rebuild OpenSearch Index** (`cluster: next`) |
| 5 | Verify | one `cf run-task` (`compare --cluster next`) |
| 6 | Move both apps | **Cutover OpenSearch Cluster** (one run, both apps) |
| 7 | Verify and resume | **Toggle Harvester**, clear the repo variable |

Six of the seven are a single command or one workflow dispatch. Rollback is step 6
re-run with the old name. The optional decommission rename adds four more steps
and is the only part with a real failure window — see
[Decommission](#decommission-delete-old-rename-new-to-canonical).

Two properties make this safe:

- `cf bind-service` does **not** affect a running app. Cloud Foundry only
  refreshes `VCAP_SERVICES` on restart or push, so the new cluster can be
  provisioned and bound days ahead with zero risk.
- Manifest application is **additive** — it does not remove existing bindings —
  so the replacement instance is bound out-of-band and deliberately *not* listed
  in `manifest.yml` (a manifest cannot reference an instance that does not exist).

## Before you start

- [ ] Both repos deployed with the `OPENSEARCH_SERVICE_NAME` indirection in the
      target space (this repo, and `GSA/datagov-catalog` — see
      [Catalog-side changes](#catalog-side-changes)).
- [ ] **Catalog's `no_proxy` fix is deployed.** See the warning below. Blocking.
- [ ] Quota headroom for a second cluster of the same size. A duplicate prod
      `es-large` is a real cost; confirm with cloud.gov it fits the
      `gsa-datagov` quota before provisioning.
- [ ] Rehearsed end to end in `development`, then `staging`, including a
      rollback.

> **Blocking: catalog's egress proxy.** This repo's `.profile` excludes
> `$OPENSEARCH_HOST` from the egress proxy; catalog's sets `no_proxy` to
> `.apps.internal` only, so catalog's OpenSearch traffic goes *through* the proxy,
> which has a hostname allowlist maintained outside both repos. The new cluster
> has a new hostname. Either get it allowlisted, or (preferred) change catalog's
> `.profile` to match this repo's and remove the dependency. Verify in staging
> first — otherwise catalog search breaks the moment you cut it over, and nothing
> in either repo explains why.

Throughout, `<space>` is `development`, `staging`, or `prod`, and the new instance
is `datagov-catalog-opensearch-next` (named after catalog, like the live one,
because both apps bind it).

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

## 2. Expose the new credentials to the harvester

```bash
cf set-env datagov-harvest OPENSEARCH_NEXT_SERVICE_NAME datagov-catalog-opensearch-next
cf restart datagov-harvest --strategy rolling

# Confirm the harvester resolved BOTH clusters.
bin/report_opensearch_cluster.sh datagov-harvest
```

`--strategy rolling` is not optional anywhere in this runbook; a plain
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
2. **The nightly sync** — set the repository variable
   `OPENSEARCH_SYNC_PAUSED_ENVIRONMENTS` to include `<space>` (comma-separated).
   Otherwise the 6am `compare --update` cron writes into whichever cluster is
   live mid-migration.

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
- `switch_alias`: `true` (required for `next`; the command refuses otherwise)
- `delete_old_index`: `false`

Confirm in the task log that it names the *new* host — this line is the proof the
live cluster was left alone:

```
Target cluster: next (<new host>)
```

A fresh cluster always starts with an empty concrete index named `datasets`,
created as a side effect of the first connection. The rebuild converts it to an
alias in the same atomic request, and says so:

```
Converted the legacy concrete index 'datasets' into an alias.
Rebuild complete: datasets now points to datasets-<runid>-<attempt>.
```

Live search is unaffected for this entire step. Watch catalog's latency in New
Relic to confirm.

## 5. Verify before cutting over

```bash
cf run-task datagov-harvest -k 2G -m 3G --name os-verify \
  --command "flask search compare --cluster next"
bin/monitor_cf_logs.sh datagov-harvest os-verify
```

**Must report 0 missing, 0 extra, 0 updated.** Do not continue otherwise; use
`compare --cluster next --update` to repair, then re-verify. (3G because
`compare` holds every DB id and every OpenSearch id in memory at once.)

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
2. Clear `<space>` from `OPENSEARCH_SYNC_PAUSED_ENVIRONMENTS` — if you forget,
   the nightly sync stays off indefinitely.
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

After a soak period (a week minimum), once you are certain you will not roll back.

The goal is to end with the new cluster holding the canonical name
`datagov-catalog-opensearch` and no env overrides, so steady state matches what
it was before the migration.

> **Read this first — this sequence has a real failure window.**
>
> Between the rename and the final repoint, both apps' `OPENSEARCH_SERVICE_NAME`
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

```bash
cf unset-env datagov-harvest OPENSEARCH_NEXT_SERVICE_NAME
```

Do not skip this. Leaving it set means `OPENSEARCH_SERVICE_NAME` and
`OPENSEARCH_NEXT_SERVICE_NAME` both name the now-live cluster, so
`--cluster next` would resolve to the live host. The commands refuse to run in
that state rather than silently operating on production, but the fix is to unset
the variable.

Optionally also `cf unset-env` `OPENSEARCH_SERVICE_NAME` on both apps — it now
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
(`manifest.yml` → `((app_name))-opensearch` = `datagov-catalog-opensearch`) and
reads the same three credential keys. It needs less than the harvester — no
`--cluster` plumbing and no `OPENSEARCH_NEXT_*`:

1. **`.profile`** — the same `OPENSEARCH_SERVICE_NAME` indirection: a
   `vcap_get_service_by_name` helper, `OPENSEARCH_SERVICE_NAME` defaulting to
   `datagov-catalog-opensearch`, and the same empty-host guard.
2. **`.profile` `no_proxy`** — add `${OPENSEARCH_HOST}`, per the warning above.
3. **`manifest.yml`** — leave alone, same reasoning as here.
4. **Cutover** — handled by the workflow in step 6; no catalog-side action.

No query or index-name changes: `datasets` is unchanged.

> **Blocking prerequisite.** Catalog's `.profile` does **not** read
> `OPENSEARCH_SERVICE_NAME` today — it resolves the instance as
> `${APP_NAME}-opensearch`. Until change 1 ships, setting that variable on
> `datagov-catalog` has no effect and the cutover will silently leave catalog on
> the old cluster. `bin/report_opensearch_cluster.sh` will show the mismatch
> (the `cf env` value and the resolved host disagreeing), which is the fastest
> way to catch it.

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
| Rebuild OpenSearch Index | `cluster: next` to build on the replacement cluster |
| Cutover OpenSearch Cluster | Point both apps at a cluster; also the rollback tool |
| Toggle Harvester | Pause and resume harvesting |
| Sync OpenSearch | `compare` / `compare --update`; nightly cron |
| Delete OpenSearch Physical Index | Remove an unused `datasets-*` index |

| Script | Purpose |
| --- | --- |
| `bin/report_opensearch_cluster.sh` | Which cluster each app actually resolved (`cf env` cannot show this) |
| `bin/cutover_opensearch_cluster.sh` | Check bindings, then set the env var and roll each app in order |

- `docs/ops/README.md` — other operational notes
- `create_cloudgov_services.sh` — service provisioning
- Local testing: `docker compose --profile next-cluster up -d opensearch-next`,
  then set `OPENSEARCH_NEXT_HOST=opensearch-next`
