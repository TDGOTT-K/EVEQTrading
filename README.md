# EVEQTrading

This repository contains the current executable source code for a local and server-side EVE Online market data workflow.

This public source snapshot excludes private credentials, character data, operational records, internal notes, and previous Git history.

## What is implemented

- `eveq`: market-data database setup, SDE import, public ESI history and order ingestion, batch/worker commands, scan/report/advice commands, and character account capture/report commands
- `eve-sso`: reusable EVE Online PKCE SSO CLI with local token storage
- `eve-esi`: authenticated character-read ESI CLI built on top of `eve_sso_core`
- `deploy/`: shell scripts, example env files, and `systemd` units for running server-side workers

## Repository layout

```text
src/        Python packages and CLI entry points
sql/        SQLite schema
tests/      automated tests
fixtures/   small local input examples
deploy/     server-side scripts, env examples, and systemd units
```

## Requirements

- Python 3.9 or newer
- local EVE SDE files if you want to import reference data
- outbound access to ESI if you want to fetch live market or character data

## Install

```powershell
python -m venv .venv
. .venv\Scripts\Activate.ps1
pip install -e .
```

Installed console commands:

- `eveq`
- `eve-sso`
- `eve-esi`

## Minimal local setup

Create a local database:

```powershell
eveq init-db --db-path .\state\eveq.db
```

Import local SDE reference data:

```powershell
eveq import-sde --db-path .\state\eveq.db --sde-root C:\path\to\eve-online-static-data-jsonl
```

Fetch one market history target:

```powershell
eveq fetch-market-history --db-path .\state\eveq.db --output-root .\runs --region-id 10000002 --type-id 34
```

Fetch one market order target:

```powershell
eveq fetch-market-orders --db-path .\state\eveq.db --output-root .\runs --region-id 10000002 --type-id 34
```

Run a database-backed scan:

```powershell
eveq run-db-scan --db-path .\state\eveq.db --output-root .\runs --region-id 10000002 --type-id 34
```

## Available `eveq` command groups

Data bootstrap and ingestion:

- `init-db`
- `import-sde`
- `fetch-market-history`
- `run-history-batch-fetch`
- `plan-history-sync`
- `run-history-sync-cycle`
- `run-history-sync-worker-once`
- `fetch-market-orders`
- `run-orders-batch-fetch`
- `plan-order-refresh`
- `run-region-order-mirror`
- `run-region-order-mirror-worker-once`

Scanning, reporting, and advice:

- `run-db-scan`
- `scan-jita-passive-simple`
- `scan-jita-passive-full`
- `run-jita-passive-cycle`
- `run-jita-passive-cache-cycle`
- `run-jita-passive-cache-worker-once`
- `run-order-refresh-cycle`
- `run-order-refresh-worker-once`
- `run-region-pipeline`
- `run-region-batch-pipeline`
- `benchmark-scan`
- `plan-market-coverage`
- `run-coverage-cycle`
- `run-coverage-worker-once`
- `run-retention-cleanup`
- `report-market`
- `report-jita-passive-cache`
- `explain-jita-passive-cache-opportunity`
- `diagnose-jita-passive-cache`
- `explain-opportunity`
- `advise-market`
- `plan-market-entry`
- `list-market-shortlist`
- `advise-jita-passive-cache`
- `brief-jita-passive-cache`
- `diagnose-scan`
- `diagnose-coverage`
- `run-scan`

Authenticated character tools:

- `capture-character-account`
- `report-character-account`

Use `eveq <command> --help` for the required arguments of a specific command.

## `eve-sso` and `eve-esi`

Example login flow:

```powershell
eve-sso --client-id YOUR_EVE_CLIENT_ID --callback-url http://127.0.0.1:8080/callback login --scope esi-wallet.read_character_wallet.v1 --set-default
eve-sso list --json
eve-sso token --scope esi-wallet.read_character_wallet.v1
```

Example authenticated reads:

```powershell
eve-esi --store $HOME\.eve-sso\tokens.json wallet --output .\wallet.json
eve-esi --store $HOME\.eve-sso\tokens.json assets --output .\assets.json
eve-esi --store $HOME\.eve-sso\tokens.json orders --output .\orders.json
```

If you need a sample credential file format, see `doc/appkey.example.txt`. Do not commit real secrets.

## Tests

Run the test suite with:

```powershell
python -m unittest discover -s tests
```

### Validation on Windows

The publication snapshot was checked with Python 3.9 on Windows: 165 of 167 tests passed. Two lock-handling tests (`test_coverage_worker` and `test_jita_passive_cache_worker`) fail in process-existence detection (`os.kill(pid, 0)`). Windows worker lock handling needs further verification.

## Deployment

For server-side workers and timers, see `deploy/README.md`.
