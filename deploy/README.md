# Server Deployment Bundle

This folder contains the first server-oriented deployment bundle for the foreign ingest host.

Current goal:

- run `eveq run-coverage-worker-once` on a long-lived Ubuntu server
- run `eveq run-history-sync-worker-once` on a long-lived Ubuntu server when history backlog is the primary bottleneck
- keep runtime configuration outside git
- make failures visible in systemd and log files

## Intended host role

Primary target:

- foreign ESI ingest host
- `/srv/eveqtrading`

Recommended layout on server:

```text
/srv/eveqtrading/
  app/                # git checkout or released code
  data/               # sqlite db and other durable data
  runs/               # traced run artifacts
  logs/               # service logs
  tmp/
  scripts/
  secrets/
```

Example placements:

- repo checkout: `/srv/eveqtrading/app/EVEQTrading`
- env file: `/srv/eveqtrading/secrets/eveq-foreign-batch.env`
- type list: `/srv/eveqtrading/app/EVEQTrading/deploy/examples/type_ids.foreign-ingest.example.txt`

## Files

- `scripts/run_region_batch_pipeline.sh`
  - bash entrypoint for one batch refresh+scan run
- `scripts/run_coverage_worker.sh`
  - bash entrypoint for one guarded coverage worker run
- `scripts/run_history_sync_worker.sh`
  - bash entrypoint for one guarded history sync worker run
- `scripts/run_jita_passive_cache_worker.sh`
  - bash entrypoint for one guarded jita passive cache worker run
- `scripts/run_region_order_mirror_worker.sh`
  - bash entrypoint for one guarded region order mirror worker run
- `scripts/export_live_state.sh`
  - create a consistent SQLite snapshot plus export manifest on the foreign host
- `scripts/pull_foreign_state.sh`
  - run remote export, pull snapshot and run artifacts onto the domestic host
- `scripts/export_market_shortlist_report.sh`
  - generate one market shortlist report on the server from existing DB/cache only and export stable `.md/.json/.manifest`
- `scripts/fetch_market_shortlist_report.ps1`
  - desktop-side PowerShell helper that SSH-triggers the remote shortlist export and downloads the report bundle locally
- `scripts/run_retention_cleanup.sh`
  - bash entrypoint for traced retention cleanup
- `systemd/eveq-region-batch.service`
  - oneshot service wrapper
- `systemd/eveq-region-batch.timer`
  - recurring timer wrapper
- `systemd/eveq-coverage-worker.service`
  - oneshot coverage worker wrapper
- `systemd/eveq-coverage-worker.timer`
  - recurring coverage worker timer
- `systemd/eveq-history-sync-worker.service`
  - oneshot history sync worker wrapper
- `systemd/eveq-history-sync-worker.timer`
  - recurring history sync worker timer
- `systemd/eveq-jita-passive-cache-worker.service`
  - oneshot jita passive cache worker wrapper
- `systemd/eveq-jita-passive-cache-worker.timer`
  - recurring jita passive cache worker timer
- `systemd/eveq-region-order-mirror-worker.service`
  - oneshot region order mirror worker wrapper
- `systemd/eveq-region-order-mirror-worker.timer`
  - recurring region order mirror worker timer
- `systemd/eveq-foreign-sync.service`
  - domestic-side pull wrapper
- `systemd/eveq-foreign-sync.timer`
  - domestic-side recurring pull timer
- `systemd/eveq-retention-cleanup.service`
  - oneshot retention cleanup wrapper
- `systemd/eveq-retention-cleanup.timer`
  - recurring retention cleanup timer
- `examples/eveq-foreign-batch.env.example`
  - environment template
- `examples/eveq-foreign-coverage-worker.env.example`
  - coverage worker environment template
- `examples/eveq-foreign-history-sync.env.example`
  - history sync worker environment template
- `examples/eveq-foreign-jita-passive-cache-worker.env.example`
  - jita passive cache worker environment template
- `examples/eveq-foreign-region-order-mirror-worker.env.example`
  - region order mirror worker environment template
- `examples/eveq-domestic-sync.env.example`
  - domestic pull template
- `examples/eveq-foreign-market-shortlist-report.env.example`
  - server-side shortlist export template
- `examples/eveq-retention-cleanup.env.example`
  - retention cleanup template
- `examples/type_ids.foreign-ingest.example.txt`
  - small sample target list

## Basic install sketch

1. Copy the repo to the server.
2. Create a virtual environment and install the project.
3. Copy either:
   - `examples/eveq-foreign-batch.env.example`
   - or `examples/eveq-foreign-coverage-worker.env.example`
   - or `examples/eveq-foreign-history-sync.env.example`
   - or `examples/eveq-foreign-region-order-mirror-worker.env.example`
   to a secrets path and edit it.
4. `chmod +x deploy/scripts/run_region_batch_pipeline.sh deploy/scripts/run_coverage_worker.sh deploy/scripts/run_history_sync_worker.sh deploy/scripts/run_jita_passive_cache_worker.sh deploy/scripts/run_region_order_mirror_worker.sh`
5. Copy the systemd files into `/etc/systemd/system/`.
6. Adjust `EnvironmentFile=` to the actual env path.
7. `systemctl daemon-reload`
8. Enable the timer you actually intend to use:
   - `systemctl enable --now eveq-region-batch.timer`
   - or `systemctl enable --now eveq-coverage-worker.timer`
   - or `systemctl enable --now eveq-history-sync-worker.timer`
   - or `systemctl enable --now eveq-jita-passive-cache-worker.timer`
   - or `systemctl enable --now eveq-region-order-mirror-worker.timer`

## Domestic pull sketch

1. Deploy the same repo to the domestic host.
2. Create a dedicated SSH key on the domestic host for pulling from the foreign host.
3. Add the domestic public key to the foreign host user's `authorized_keys`.
4. Copy `examples/eveq-domestic-sync.env.example` to a secrets path and edit it.
5. Copy `systemd/eveq-foreign-sync.service` and `systemd/eveq-foreign-sync.timer` into `/etc/systemd/system/`.
6. `chmod +x deploy/scripts/export_live_state.sh deploy/scripts/pull_foreign_state.sh`
7. `systemctl daemon-reload`
8. `systemctl enable --now eveq-foreign-sync.timer`

## Retention cleanup sketch

1. Copy `examples/eveq-retention-cleanup.env.example` to a secrets path and edit it.
2. `chmod +x deploy/scripts/run_retention_cleanup.sh`
3. Copy `systemd/eveq-retention-cleanup.service` and `systemd/eveq-retention-cleanup.timer` into `/etc/systemd/system/`.
4. `systemctl daemon-reload`
5. `systemctl enable --now eveq-retention-cleanup.timer`

Current design intent:

- foreign host remains the primary ESI ingest node
- domestic host actively pulls from foreign instead of waiting on push
- the pull flow triggers a remote export first, then downloads a consistent DB snapshot
- `runs/` artifacts are synced separately so AI-side result explanation can stay traceable
- `run-coverage-worker-once` is now the preferred foreign ingest control entry because it adds:
  - internal lock file control
  - state file persistence
  - one full cycle of coverage expansion + ready scan + diagnosis
  - optional failure backoff so repeated timer runs do not hammer ESI after a hard failure
- `run-history-sync-worker-once` is the preferred foreign ingest control entry when the primary goal is:
  - eat down `history` backlog first
  - refresh only the latest day for already-cached types
  - avoid mixing backlog-building with orders/scan work on the same timer
  - timer only wakes the worker; one worker run can now execute multiple history sync cycles continuously
  - the practical throttle is `max_runtime_seconds` / optional `max_cycles_per_run`, not "one timer tick only does one 50-item cycle"
- the export manifest now also carries `worker_state`, so the domestic host can see:
  - whether foreign ingest is running, completed, failed, or skipped by lock
  - the latest `worker_run_id` / `cycle_run_id`
  - whether the worker is temporarily `skipped_backoff` and when it can run again
  - when no explicit `EVEQ_WORKER_STATE_PATH` is provided, `export_live_state.sh` now prefers:
    - `region-order-mirror-worker-state.json`
    - `jita-passive-cache-worker-state.json`
    - then `history-sync-worker-state.json`
    - and falls back to `coverage-worker-state.json`
  - the export lock default also follows the same idea:
    - prefer `eveq-region-order-mirror-worker.lock`
    - prefer `eveq-jita-passive-cache-worker.lock`
    - else `eveq-history-sync-worker.lock`
    - else fall back to `eveq-region-batch.lock`
  - domestic `report-market` now keeps the new cache worker context in `latest_sync.worker_state_summary`:
    - `cycle_count`
    - `last_cycle_run_id`
    - `last_scan_run_id`
    - `orders_cache_profile`
    - `stop_reason`
- retention cleanup now has one explicit traced entrypoint instead of ad-hoc manual deletions

Important env-file note:

- `eveq-foreign-batch.env.example` is written so the same file can be consumed by both `systemd EnvironmentFile=` and shell `source`.
- If an environment value contains spaces, keep it quoted. `USER_AGENT` is the main current example.
- Unquoted space-containing values may still appear to work in `systemd`, but they will break shell-based manual runs.

## History worker tuning note

Recommended first pass for foreign backlog catch-up:

- keep `EVEQ_BACKFILL_CHUNK_SIZE=50`
- keep `EVEQ_MAX_BACKFILL_CHUNKS=1`
- keep `EVEQ_REFRESH_CHUNK_SIZE=200`
- keep `EVEQ_MAX_REFRESH_CHUNKS=1`
- set `EVEQ_CONCURRENT_REQUESTS=4`
- set `EVEQ_MAX_RUNTIME_SECONDS=540`
- set `EVEQ_REQUEST_WALL_TIMEOUT_SECONDS=20`
- optionally set `EVEQ_MAX_CYCLES_PER_RUN` if you want a second hard ceiling

This keeps each history sync cycle small and traceable, but allows a single worker invocation to chain many cycles back-to-back until:

- there are no due history targets left
- the runtime budget is exhausted
- or an explicit `EVEQ_MAX_CYCLES_PER_RUN` ceiling is reached

Important nuance:

- `chunk_size` controls how many types one fetch chunk contains
- `max_*_chunks=1` keeps one history sync cycle bounded
- `concurrent_requests` controls how many history HTTP requests run in parallel inside one fetch batch
- `max_runtime_seconds` then controls how many bounded cycles one worker wake can chain together
- removing `max_*_chunks` entirely makes one cycle try to chew through the whole backlog, which defeats the runtime budget

## Jita passive cache worker tuning note

Recommended first pass for foreign passive-market progress:

- `EVEQ_ORDERS_CHUNK_SIZE=50`
- `EVEQ_MAX_ORDERS_CHUNKS=1`
- `EVEQ_CONCURRENT_REQUESTS=4`
- `EVEQ_ORDERS_FETCH_MODE=auto`
- `EVEQ_MAX_RUNTIME_SECONDS=540`
- timer cadence:
  - `OnUnitInactiveSec=15min`

This keeps one cache cycle bounded:

- one cycle refreshes only one small orders chunk
- then immediately runs one traced `run-jita-passive-cache-cycle`
- one worker wake can chain many bounded cycles until:
  - there are no remote-due order targets left
  - the runtime budget is exhausted
  - or an explicit `EVEQ_MAX_CYCLES_PER_RUN` ceiling is reached

Practical interpretation:

- use `run-jita-passive-cache-cycle` for manual one-shot inspection
- use `run-jita-passive-cache-worker-once` for always-on foreign progression
- do not remove `EVEQ_MAX_ORDERS_CHUNKS` unless you intentionally want one cycle to chew a very large remote target set

Current implementation detail:

- history network fetch now supports bounded parallelism
- orders network fetch now also supports bounded parallelism across type targets
- SQLite writes still happen on the main thread after each response finishes
- this keeps database state changes and trace artifacts deterministic while improving ESI throughput

Order-ingest tuning note:

- `EVEQ_CONCURRENT_REQUESTS` now applies to both:
  - history fetch
  - orders fetch
- practical first pass on the foreign ingest host:
  - start with `EVEQ_CONCURRENT_REQUESTS=4`
  - then raise gradually to `6` or `8` only after checking:
    - ESI error rate
    - service stability
    - whether wall-clock batch time keeps improving
- current order path is still type-target based, not one giant full-region order mirror
- so concurrency is the first safe throughput lever, not the final architecture

Region-pages orders mode:

- orders fetch now supports:
  - `per_type`
  - `region_pages`
  - `auto`
- `region_pages` means:
  - call the region-wide public orders endpoint without `type_id`
  - fetch region pages concurrently
  - split returned orders back into per-type snapshots locally
- `auto` means:
  - first estimate the per-type page cost from local snapshot summaries
  - for clearly small/light target sets, stay on `per_type`
  - otherwise probe region page count with one first-page request, then choose the cheaper side
- this keeps downstream code unchanged because the stored shape is still:
  - one snapshot per type
  - one `sync_scope_state` row per type
- first practical use case:
  - many target types in one run
  - especially when the target count is large enough that `per_type` request overhead dominates
- current recommendation:
  - keep server default at `per_type` unless you are intentionally testing or running a broad refresh pass
  - switch to `region_pages` only when the selected type set is wide enough to justify one full-region page sweep
  - use `auto` when you want the software to make a conservative choice instead of hardcoding one mode

Foreign validation note:

- the first foreign smoke after adding the feature exposed one easy-to-miss bug:
  - `history_sync.py` was rebuilding `HistoryFetchConfig` without forwarding `concurrent_requests`
- after fixing that pass-through bug, a foreign 90-second smoke with:
  - `backfill_chunk_size=50`
  - `max_backfill_chunks=1`
  - `concurrent_requests=4`
  completed `2` cycles and advanced `100` backfill targets
- this was materially faster than the earlier single-request path on the same host

Why the extra wall-time limit matters:

- `urllib` socket timeout mainly limits inactivity on one socket operation
- in live ESI traffic, one history request can still hold a connection open for much longer than the nominal socket timeout
- `EVEQ_REQUEST_WALL_TIMEOUT_SECONDS` caps the total lifetime of one history request so a single bad type does not pin the whole worker run indefinitely

## Region order mirror worker note

Current preferred foreign orders-ingest entry:

- `run-region-order-mirror-worker-once`

Recommended first pass on the foreign host:

- `EVEQ_TARGET_UNIVERSE=history_ready`
- `EVEQ_REFRESH_INTERVAL_MINUTES=30`
- `EVEQ_CONCURRENT_REQUESTS=16`
- `EVEQ_REQUEST_TIMEOUT_SECONDS=30`
- timer cadence:
  - `OnUnitInactiveSec=5min`

Interpretation:

- timer wakes frequently
- worker itself decides whether the 30-minute mirror cache is still fresh
- most timer ticks should therefore end quickly as `skipped_fresh`
- when the mirror is due, one worker run performs one whole-region sweep and refreshes all current `history_ready` order snapshots

Current foreign measurement on `2026-04-07`:

- `concurrent_requests=16`
  - completed successfully
  - refreshed about `13.9k` history-ready types
  - fetched about `410` region pages
  - wall clock about `399s`
- `concurrent_requests=24`
  - crossed the stability line
  - region-page requests timed out
  - not suitable as the default

## Current scope limits

- This bundle now includes a first domestic pull path, but not yet full production-hardening or retention management.
- This bundle does not yet include database migration orchestration.
- This bundle assumes Python and the project environment are already installed.
- The current desktop environment did not have `bash`, so Linux shell syntax was not executed locally before server-side validation.

## Server-generated shortlist report

Goal:

- server generates the market shortlist from its existing SQLite DB
- if production `runs/` artifacts are incomplete, the export still works by reading DB-resident scan/opportunity/order/history data directly
- local desktop only downloads `.md`, `.json`, and manifest
- local desktop must not trigger full ESI refreshes

Server-side script:

- `deploy/scripts/export_market_shortlist_report.sh`
- priority source:
  - `EVEQ_DB_PATH`
- optional context:
  - `EVEQ_RUNS_ROOT`
  - used only as metadata in the export manifest
- does not call any refresh worker and does not hit ESI
- writes:
  - `${EVEQ_REPORT_EXPORT_DIR}/market_shortlist.md`
  - `${EVEQ_REPORT_EXPORT_DIR}/market_shortlist.json`
  - `${EVEQ_REPORT_EXPORT_DIR}/market_shortlist_manifest.json`
- also archives each export into:
  - `${EVEQ_REPORT_EXPORT_DIR}/market_shortlist-<timestamp>/`

Recommended server env file:

- copy `examples/eveq-foreign-market-shortlist-report.env.example`
- edit at least:
  - `EVEQ_TOTAL_BUDGET_ISK`
  - optional `EVEQ_RISK_PROFILE`
  - optional `EVEQ_MAX_ROWS`
  - optional `EVEQ_SCAN_RUN_ID`

Manual server run example:

```bash
set -a
source /srv/eveqtrading/secrets/eveq-foreign-market-shortlist-report.env
set +a
bash /srv/eveqtrading/app/EVEQTrading/deploy/scripts/export_market_shortlist_report.sh
```

Desktop pull helper:

- `deploy/scripts/fetch_market_shortlist_report.ps1`
- flow:
  - SSH to server
  - set remote env vars for the report generation
  - run `export_market_shortlist_report.sh`
  - SCP the stable `market_shortlist.md/.json/.manifest` files back to the desktop
  - place them under local `runs/server-reports/market-shortlist-.../`

Desktop example:

```powershell
powershell -ExecutionPolicy Bypass -File .\deploy\scripts\fetch_market_shortlist_report.ps1 `
  -RemoteHost ubuntu@<server> `
  -SshKeyPath D:\path\to\key `
  -TotalBudgetIsk 50000000000 `
  -RiskProfile standard `
  -MaxRows 30
```

Important safety rule:

- do not run `python -m eveqtrading run-jita-passive-cache-cycle` on the desktop when the intent is only to read a report
- use the PowerShell fetch helper instead, so the desktop remains read-only with respect to ESI traffic
