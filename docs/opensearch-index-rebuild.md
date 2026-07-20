# OpenSearch Index Rebuild and Alias Cutover

This document describes how Data.gov rebuilds the OpenSearch dataset index
without interrupting search traffic. Harvest processing is paused during the
rebuild so PostgreSQL remains the stable source of truth. A replay or change-log
table and a database-backed scheduling flag are not required.

## Goals

- Keep the existing OpenSearch index available to readers during the backfill.
- Prevent dataset mutations while the new index is being built.
- Fail before cutover if any dataset cannot be indexed or validation fails.
- Switch all readers and writers atomically through the `datasets` alias.
- Preserve queued harvest jobs and resume them after the operation.

Search remains available throughout the operation. Harvest jobs are delayed for
the duration of the backfill.

## Components

```mermaid
flowchart LR
    Operator[Operator] --> GHA[GitHub Actions<br>Rebuild OpenSearch Index]

    GHA --> Pause[Set max tasks to 0<br>rolling restart]
    GHA --> Drain[Poll active<br>harvest tasks]
    GHA --> Rebuild[Rebuild index<br>CF task]
    GHA --> Resume[Restore max tasks<br>rolling restart]

    Pause --> App[Harvester web instances]
    Resume --> App
    Scheduler[LoadManager schedulers] --> App
    Manual[Manual harvest triggers] --> App

    Drain --> CFAPI[Cloud Foundry v3 task API]
    Rebuild --> PG[(PostgreSQL datasets)]
    Rebuild --> Candidate[(Versioned OpenSearch index)]
    Candidate --> Alias[datasets alias]
    Alias --> Catalog[Catalog search traffic]
    Alias --> Harvester[Harvester OpenSearch writes]
```

The workflow saves the current `HARVEST_RUNNER_MAX_TASKS` setting, changes it to
`0`, and performs a rolling restart. When the setting is `0`:

- `_start_new_jobs()` does not dispatch scheduled jobs.
- `start_job()` cannot create a CF task.
- Manual harvest triggers do not create or start an immediate job.
- A completed harvest cannot chain another task through
  `check_for_more_work()`.
- Manual dataset slug edits are rejected.

The setting is read when each process starts, so changing it requires a restart.
The workflow initiates that restart directly rather than waiting for the
periodic restart workflow. It confirms that no rolling deployment remains
active, the desired number of web instances are present, every instance is
running with a new instance GUID, and the application environment has the
requested value. Future scheduled jobs may remain in the database with status
`new`.

## Automated rebuild sequence

The manual GitHub Actions workflow is
`.github/workflows/rebuild_opensearch_index.yml`.

```mermaid
sequenceDiagram
    autonumber
    actor O as Operator
    participant GH as GitHub Actions
    participant DB as PostgreSQL
    participant CF as Cloud Foundry
    participant H as Active harvest tasks
    participant OS as OpenSearch

    O->>GH: Run "Rebuild OpenSearch Index"
    GH->>CF: Save current max task setting
    GH->>CF: Set HARVEST_RUNNER_MAX_TASKS = 0
    GH->>CF: Start rolling restart
    CF-->>GH: All web instances restarted and running

    loop Until drained
        GH->>CF: List harvest-job-* tasks
        CF-->>GH: PENDING, RUNNING, and CANCELING tasks
        alt Force-kill enabled and tasks remain after 15 minutes
            GH->>CF: Cancel each active harvest task
            CF-->>GH: Tasks transition through CANCELING
        else Normal drain
            H-->>CF: Existing tasks reach terminal states
        end
    end
    Note over GH,CF: Require a 30-second stable-zero period

    GH->>CF: Run rebuild-index task
    CF->>CF: Confirm HARVEST_RUNNER_MAX_TASKS = 0
    CF->>CF: Confirm no active harvest tasks
    CF->>OS: Create datasets-<run>-<attempt>
    CF->>OS: Validate initial mapping

    loop PostgreSQL keyset batches
        CF->>DB: Read next dataset batch
        DB-->>CF: Dataset rows
        CF->>OS: Bulk index into candidate
        OS-->>CF: Success/failure details
    end

    CF->>OS: Refresh candidate
    CF->>DB: Recheck dataset count
    CF->>OS: Read candidate document count
    alt Alias switching enabled
        CF->>OS: Atomically switch datasets alias
        OS-->>CF: Alias update acknowledged
    else Build-only mode
        CF-->>GH: Leave datasets alias unchanged
    end
    CF-->>GH: Rebuild task succeeds

    GH->>CF: Restore saved max task setting
    GH->>CF: Start rolling restart
    CF-->>GH: All web instances restarted and running
```

### 1. Pause dispatch

The workflow reads the application environment through the Cloud Foundry API,
saves whether `HARVEST_RUNNER_MAX_TASKS` was set and its exact value, and then
runs:

```shell
cf set-env datagov-harvest HARVEST_RUNNER_MAX_TASKS 0
cf restart datagov-harvest --strategy rolling
```

It waits for Cloud Foundry to confirm the rolling restart is complete and every
desired web instance is running with the new application environment. Running
harvest tasks are not restarted or canceled. Tasks that began with the previous
setting may schedule one final follow-on task, so the workflow still drains the
Cloud Foundry task queue before starting the rebuild.

### 2. Drain active tasks

`bin/wait_for_harvest_tasks.sh` polls the Cloud Foundry v3 task API for
`harvest-job-*` tasks in these non-terminal states:

- `PENDING`
- `RUNNING`
- `CANCELING`

After the active count reaches zero, it must remain zero for 30 seconds. The
default behavior waits up to two hours and fails the workflow on timeout.

For manually dispatched workflows, the optional **Cancel harvest jobs still
running after 15 minutes** input changes the drain behavior. After 15 minutes,
the workflow requests cancellation of every active `harvest-job-*` task through
the Cloud Foundry API. It continues polling until all canceled tasks reach a
terminal state and the 30-second quiet period passes. API failures still fail
the workflow instead of assuming the system is drained.

### 3. Create and backfill the candidate

The workflow creates a unique physical index:

```text
datasets-<github-run-id>-<github-run-attempt>
```

It then invokes:

```shell
flask search rebuild-index \
  --target-index datasets-<run-id>-<attempt> \
  --allow-legacy-index-removal \
  --switch-alias
```

The command refuses to proceed unless `HARVEST_RUNNER_MAX_TASKS` is `0` and no
active harvest tasks remain. It:

1. Creates the empty physical index with the application mappings and settings.
2. Validates the mapping before indexing dynamic dataset fields.
3. Reads every PostgreSQL `Dataset` row using ID-based keyset pagination.
4. Bulk indexes batches of 1,000 documents by default.
5. Stops on any bulk failure.
6. Refreshes the candidate.
7. Confirms the PostgreSQL count did not change during the backfill.
8. Confirms the candidate document count equals the PostgreSQL count.

Because the candidate starts empty, dataset IDs are unique, every successful
bulk result is counted, and PostgreSQL mutations are paused, these checks ensure
that every source dataset is represented before cutover.

### 4. Atomically switch the alias

Normal operation addresses OpenSearch through the logical name `datasets`.
Physical indexes use versioned names.

```mermaid
flowchart TB
    subgraph Before["Before a subsequent rebuild"]
        A1[datasets alias] --> O1[(datasets-old)]
        N1[(datasets-new<br>backfilled and validated)]
    end

    Switch[Atomic update_aliases request]

    subgraph After["After cutover"]
        A2[datasets alias] --> N2[(datasets-new)]
        O2[(datasets-old<br>retained for recovery)]
    end

    Before --> Switch --> After
```

For subsequent rebuilds, one `update_aliases` request removes the alias from the
old physical index and adds it to the candidate with `is_write_index: true`.
The old physical index is retained.

### First alias conversion

Before this feature is used for the first time, `datasets` may still be a
concrete index. An alias cannot have the same name as an existing concrete
index. The first cutover therefore performs these actions atomically:

1. `remove_index` for the legacy concrete `datasets` index.
2. Add the `datasets` alias to the validated candidate.

The `--allow-legacy-index-removal` option makes this one-time deletion explicit.
The legacy index is not retained, so take an OpenSearch snapshot first if its
contents must be independently recoverable. Later rebuilds retain the previous
versioned index.

### Build without switching the alias

The workflow input **Switch the datasets alias to the rebuilt index** is enabled
by default. Disabling it passes `--no-switch-alias`, which creates, backfills,
and validates the physical index but leaves `datasets` unchanged. The workflow
still restores harvest capacity afterward.

This is a build-only mode, not currently a safe production handoff for a
breaking schema change:

- Data.gov Catalog and Harvester currently address the hardcoded logical name
  `datasets`; redeploying Catalog alone cannot select the generated physical
  name.
- Once harvesting resumes, Harvester writes to the existing `datasets` target,
  so the unswitched candidate can become stale.
- A rolling Catalog deployment is not an atomic reader cutover.

A coordinated incompatible-schema rollout must keep writers paused while both
applications are configured for the new schema and target. Prefer a
configurable, versioned logical alias over embedding a generated physical index
name in application code. Resume harvesting only after Harvester writes and all
Catalog readers target the same new logical index.

## Failure behavior

```mermaid
flowchart TD
    Start[Capacity set to 0] --> Build{Create, backfill,<br>and validation succeed?}
    Build -- No --> Old[Existing alias/index remains live]
    Build -- Yes --> Switch{Alias switch enabled?}
    Switch -- No --> Candidate[Validated candidate retained]
    Switch -- Yes --> Flip{Alias switch acknowledged?}
    Flip -- No --> Old
    Flip -- Yes --> New[New index serves traffic]
    Old --> Finally[Always attempt capacity restore]
    Candidate --> Finally
    New --> Finally
    Finally --> Resume{Restore and restart succeed?}
    Resume -- Yes --> Running[Queued harvest jobs restart]
    Resume -- No --> Safe[Search remains available;<br>verify application environment]
```

- A failure before alias cutover leaves the existing live index unchanged.
- A partially built candidate may remain for diagnosis and can be deleted
  manually.
- Alias changes are submitted as one atomic OpenSearch operation.
- The workflow uses `if: always()` to restore the exact saved capacity setting
  and perform another verified rolling restart after earlier failures.
- If the runner is force-canceled or recovery fails,
  `HARVEST_RUNNER_MAX_TASKS` may remain `0` as the safe failure mode. Search
  continues to work.

Manual recovery:

```shell
cf env datagov-harvest
cf set-env datagov-harvest HARVEST_RUNNER_MAX_TASKS <environment-value>
cf restart datagov-harvest --strategy rolling
```

Use the value in the environment's `vars.<environment>.yml` file. Only restore
capacity after confirming that no index rebuild is still running, then verify
all desired app instances are running.

## Operational runbook

### Prerequisites

1. Deploy the application version containing the zero-capacity scheduling gates
   and rebuild command.
2. Confirm external consumers access `datasets` by name and can use an alias.
3. For the first conversion, decide whether an OpenSearch snapshot is required.
4. Avoid direct database writes or uncoordinated maintenance commands during the
   rebuild.

### Run

1. Open **Actions → Rebuild OpenSearch Index**.
2. Select `development`, `staging`, or `prod`.
3. Optionally enable **Cancel harvest jobs still running after 15 minutes**.
4. Leave **Switch the datasets alias to the rebuilt index** enabled for the
   normal atomic cutover, or disable it for build-only mode.
5. Run the workflow.
6. Monitor the capacity change, rolling restart, drain, rebuild, validation,
   optional alias switch, capacity restore, and final rolling restart.

GitHub Actions applies the concurrency group
`opensearch-maintenance-<environment>`, so rebuild and synchronization workflows
and the periodic Harvester restart workflow for the same environment do not
overlap. The group uses `queue: max`, so later maintenance requests wait instead
of replacing an already queued operation.

### Verify

1. Confirm the workflow completed successfully.
2. Run `cf env datagov-harvest` and confirm
   `HARVEST_RUNNER_MAX_TASKS` has its original value.
3. If alias switching was enabled, confirm the `datasets` alias points to the
   new physical index. Otherwise, confirm it did not change.
4. Confirm queued harvest jobs have started.
5. Exercise catalog searches and inspect OpenSearch/harvester logs.

### Remove the retained physical index manually

The rebuild workflow intentionally retains the previous physical index for
rollback and does not delete it automatically. The **Monitor index rebuild**
step prints `Previous index retained: <index-name>` and identifies the new
index targeted by the `datasets` alias.

Deleting the old index is a manual post-rebuild step. In production, the
operator should use their discretion and delete it only after the workflow,
catalog searches, harvest processing, and application logs indicate that the
new index is operating correctly and the rollback window has passed.

> [!WARNING]
> Do not retain obsolete indexes longer than necessary. They consume cluster
> storage and resources, and failure to delete them may result in degradation
> of the OpenSearch cluster. After verifying that production is operating
> correctly, delete the old index.

Use **Actions → Delete OpenSearch Physical Index** to remove the old index:

1. Select the Cloud Foundry environment.
2. Enter the exact physical index name printed by the rebuild, such as
   `datasets-123456-1`.
3. Run the workflow and verify the Cloud Foundry task succeeds.

The workflow shares the environment-specific OpenSearch maintenance concurrency
group with rebuild and synchronization operations. The command only accepts
physical names beginning with `datasets-`, verifies that the index exists, and
refuses to delete the index currently targeted by the `datasets` alias. The
logical `datasets` name and aliases cannot be deleted through this workflow.

## Related implementation

- `.github/workflows/rebuild_opensearch_index.yml` — operation orchestration.
- `.github/workflows/delete_opensearch_index.yml` — guarded old-index cleanup.
- `bin/manage_harvest_runner_capacity.sh` — capacity capture, rolling restarts,
  health confirmation, and restoration.
- `bin/wait_for_harvest_tasks.sh` — active-task drain and quiet period.
- `app/commands/search.py` — candidate creation, validation, cutover, and cleanup.
- `harvester/runner_settings.py` — shared harvest capacity configuration.
- `harvester/lib/load_manager.py` — scheduling gates.
- `harvester/lib/cf_handler.py` — active CF harvest-task discovery.
- `harvester/opensearch.py` — physical-index and alias operations.

The older `search reset-mapping` command is blocked after `datasets` becomes an
alias because deleting an alias expression could delete the live backing index.
Use the rebuild workflow for future mapping changes.
