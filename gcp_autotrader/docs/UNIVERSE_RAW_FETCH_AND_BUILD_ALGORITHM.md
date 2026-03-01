# Universe Raw Fetch + Instrument Universe Build (Code-Truth Algorithm)

This document describes the **current implemented logic** for:
- Upstox raw universe fetch
- Trading universe build/append into sheet `🧾 Universe Instruments`

Scope:
- Python runtime under `gcp_autotrader/src/autotrader`
- Current Google Sheets schema (as defined in code)
- Current scheduler/API orchestration

Out of scope:
- Legacy Apps Script logic (`*.gs`) — this document is for the active Python pipeline.

---

## 1) Components Involved

- API endpoints:
  - `POST /jobs/raw-universe-refresh`  
    `/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader/src/autotrader/web/api.py:158`
  - `POST /jobs/universe-build`  
    `/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader/src/autotrader/web/api.py:195`
  - `POST /jobs/universe-refresh-append-backfill` (chained pipeline)  
    `/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader/src/autotrader/web/api.py:679`

- Core service logic:
  - `UniverseService.refresh_raw_universe_from_upstox(...)`  
    `/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader/src/autotrader/services/universe_service.py:312`
  - `UniverseService.build_trading_universe_from_upstox_raw(...)`  
    `/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader/src/autotrader/services/universe_service.py:342`
  - `UniverseService._load_latest_upstox_raw_universe(...)`  
    `/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader/src/autotrader/services/universe_service.py:334`

- Upstox adapter:
  - download + decode instruments snapshot  
    `/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader/src/autotrader/adapters/upstox_client.py:315`
    `/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader/src/autotrader/adapters/upstox_client.py:323`

- Storage adapter (GCS path contracts):
  - `/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader/src/autotrader/adapters/gcs_store.py:77`

- Sheet schema + read/write:
  - Universe tab headers  
    `/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader/src/autotrader/adapters/sheets_repository.py:46`
  - Universe read/write helpers  
    `/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader/src/autotrader/adapters/sheets_repository.py:700`
    `/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader/src/autotrader/adapters/sheets_repository.py:736`
    `/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader/src/autotrader/adapters/sheets_repository.py:741`
    `/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader/src/autotrader/adapters/sheets_repository.py:745`

- Locking:
  - Firestore lock transaction  
    `/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader/src/autotrader/adapters/firestore_state.py:77`

- Scheduler wiring:
  - morning chained universe pipeline  
    `/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader/deploy/create_scheduler_jobs.sh:50`

---

## 2) Data Stores and Paths

### 2.1 Raw universe storage (GCS)

Raw Upstox instrument dump is stored in GCS in two places:
- Versioned snapshot: `raw/upstox/universe/<run_date>/complete.json.gz`
- Moving latest pointer: `raw/upstox/universe/latest/complete.json.gz`
- Metadata JSON: `raw/upstox/universe/latest/meta.json`

Path functions:
- `/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader/src/autotrader/adapters/gcs_store.py:77`
- `/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader/src/autotrader/adapters/gcs_store.py:81`
- `/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader/src/autotrader/adapters/gcs_store.py:85`

### 2.2 Trading universe storage (Google Sheet)

Target tab: `🧾 Universe Instruments`  
Header contract:
- `/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader/src/autotrader/adapters/sheets_repository.py:46`

Important columns used by build logic:
- `A`: `#`
- `B`: `Symbol`
- `C`: `Exchange`
- `D`: `Segment`
- `I`: `Enabled`
- `K`: `Notes`
- `S`: `Raw CSV (JSON)` (raw row JSON string)
- `V`: `Instrument Key`
- `W`: `Source Segment`
- `X`: `Security Type`

---

## 3) End-to-End Algorithm

## 3.1 Stage A — Raw Upstox universe fetch

Entry:
- `POST /jobs/raw-universe-refresh`  
  `/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader/src/autotrader/web/api.py:158`

High-level flow:
1. Validate `X-Job-Token`.
2. Ensure core sheets exist.
3. Acquire lock `raw_universe_refresh` (TTL 1800s).
4. Call `UniverseService.refresh_raw_universe_from_upstox()`.
5. Write START/DONE/ERROR actions to `🧩 Project Log`.

Service logic:
1. Download Upstox instruments gzip:
   - `UpstoxClient.fetch_instruments_complete_gz()`  
     `/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader/src/autotrader/adapters/upstox_client.py:315`
2. Decode gzip JSON payload:
   - `UpstoxClient.decode_instruments_gz_json(...)`  
     `/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader/src/autotrader/adapters/upstox_client.py:323`
3. Compute `run_date` (`today_ist()`).
4. Write raw bytes to:
   - versioned path
   - latest path
5. Build metadata:
   - `provider`, `runDate`, `fetchedAt`, `path`, `latestPath`, `itemCount`, `sourceUrl`
6. Write metadata JSON to latest meta path.
7. Return metadata object.

Code:
- `/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader/src/autotrader/services/universe_service.py:312`

---

## 3.2 Stage B — Build/append trading universe from raw snapshot

Entry:
- `POST /jobs/universe-build`  
  `/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader/src/autotrader/web/api.py:195`

High-level flow:
1. Validate `X-Job-Token`.
2. Ensure core sheets.
3. Acquire lock `universe_build` (TTL 1800s).
4. Call `UniverseService.build_trading_universe_from_upstox_raw(limit, replace)`.
5. Write START/DONE/ERROR actions to `🧩 Project Log`.

Raw load:
1. Read `latest/meta.json`.
2. Read `latest/complete.json.gz`.
3. Decode JSON rows.
4. If latest blob missing: raise runtime error.

Code:
- `/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader/src/autotrader/services/universe_service.py:334`

Filtering + normalization algorithm (`for raw in raw_rows`):
1. Skip non-dict rows.
2. Respect optional `limit`.
3. Extract normalized fields:
   - `seg` from `segment` / `exchange_segment`
   - `exchange` from `exchange` or from `seg` prefix
   - `symbol` from `trading_symbol` / `tradingsymbol` / `symbol`
   - `instrument_key` from `instrument_key` / `instrumentKey`
   - `instrument_type`, `security_type`, `isin`, `name`
4. Hard eligibility filters:
   - `symbol` and `instrument_key` must be non-empty
   - `seg` must be `NSE_EQ` or `BSE_EQ`
   - `exchange` must be `NSE` or `BSE`
   - if instrument_type present, it must be `EQ` or `EQUITY`
   - `is_enabled` must be truthy (default true)
   - `is_delisted` false
   - `is_suspended` false
   - `suspended` false
5. Dedup key:
   - `key = isin or symbol`
6. Dedup winner rule:
   - first candidate stored
   - if duplicate key appears and current preferred exchange is not NSE, replace with NSE candidate.

Code:
- `/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader/src/autotrader/services/universe_service.py:350`

Row materialization (`out_rows`):
1. Sort deduped rows by `(symbol, exchange)`.
2. Skip any symbol already present in existing sheet symbols (append mode).
3. Build output row with defaults:
   - `Segment=CASH`, `Allowed Product=BOTH`, `Strategy=AUTO`, `Sector=UNKNOWN`
   - `Beta=1.0`, `Enabled=Y`, `Priority=0`
   - score fields initialized to `0`/empty
   - `Notes=isin=<...>|name=<...>|source=upstox_bod`
   - `Raw CSV (JSON)` stores full raw object JSON
   - `Data Provider=UPSTOX`
   - `Instrument Key`, `Source Segment`, `Security Type` filled
4. Write:
   - `replace=true`: replace full universe table
   - else append only new rows
5. Return summary:
   - `rows`, `appended`, `replaced`, `rawSeen`, `rawEligible`, `rawSnapshotDate`

Code:
- `/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader/src/autotrader/services/universe_service.py:406`

---

## 3.3 Stage C — Chained morning orchestration

Entry:
- `POST /jobs/universe-refresh-append-backfill`  
  `/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader/src/autotrader/web/api.py:679`

Algorithm:
1. Validate token and ensure sheets.
2. Acquire multi-lock set:
   - `raw_universe_refresh`
   - `universe_build`
   - optional `score_cache_backfill_full` if `run_backfill=true`
   - optional `score_refresh` if `run_score_refresh=true`
3. Execute in order:
   - raw refresh
   - universe build append/replace
4. If append count > 0 and not replace:
   - read universe rows
   - capture last appended symbols as `priority_symbols`
5. Optional kickoff backfill batch (for new symbols).
6. Optional score refresh.
7. Write single DONE payload to `🧩 Project Log`.

Code:
- `/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader/src/autotrader/web/api.py:700`

Scheduled default:
- At `06:15` IST weekdays:
  - `autotrader-universe-refresh-append-backfill-0615`
  - URI includes `replace=false`, `run_backfill=true`, `backfill_max_passes=1`.
- Config:
  `/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader/deploy/create_scheduler_jobs.sh:50`

---

## 4) Sheet Structure Truth (Universe Tab)

Defined headers:
- `/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader/src/autotrader/adapters/sheets_repository.py:49`

Universe row parsing for runtime:
- Reads enabled rows only (`I == Y`).
- Uses provider/instrument columns to power downstream cache and scanner.
- Reader implementation:
  `/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader/src/autotrader/adapters/sheets_repository.py:700`

Row count + existing symbol set used for append idempotency:
- `/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader/src/autotrader/adapters/sheets_repository.py:745`

---

## 5) Locking, Idempotency, and Failure Semantics

Lock mechanism:
- Firestore transactional lock documents in collection `locks`.
- Lock acquisition returns `None` when active lock owned by another runner.
- Functions:
  - acquire: `/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader/src/autotrader/adapters/firestore_state.py:77`
  - release: `/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader/src/autotrader/adapters/firestore_state.py:99`

What this guarantees:
- Prevents overlapping raw refresh/build pipelines from corrupting append sequence.
- Chained pipeline can fail fast with `{"skipped":"lock_busy"}`.

Idempotency behavior:
- Raw refresh always rewrites latest snapshot and writes versioned snapshot for the day.
- Universe build append mode skips symbols already present in sheet.
- Replace mode rebuilds entire table from filtered dedup set.

---

## 6) Observability for This Flow

All lifecycle actions are logged to `🧩 Project Log` via `LogSink.action(...)`:
- START
- DONE
- ERROR
- LOCK_BUSY

LogSink implementation:
- `/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader/src/autotrader/services/log_sink.py:40`

Sheet sink target:
- `SheetNames.ACTIONS = "🧩 Project Log"`  
  `/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader/src/autotrader/adapters/sheets_repository.py:31`

---

## 7) Practical Contract (for AI/automation consumers)

If you need a deterministic contract for this stage, treat it as:

### Inputs
- Upstox complete instruments gzip endpoint
- Existing `🧾 Universe Instruments` rows
- Request params:
  - `limit`, `replace`
  - chained-mode flags (`run_backfill`, etc.)

### Outputs
- GCS raw artifacts:
  - `raw/upstox/universe/<date>/complete.json.gz`
  - `raw/upstox/universe/latest/complete.json.gz`
  - `raw/upstox/universe/latest/meta.json`
- Updated `🧾 Universe Instruments` rows (append or replace)
- Action logs in `🧩 Project Log`
- API response summary with counts

### Hard invariants
- Only equity cash universe from segments `NSE_EQ`/`BSE_EQ`.
- Universe row must include non-empty `Symbol` and `Instrument Key`.
- Dedup key is `ISIN` fallback `Symbol`, with NSE preference on duplicates.
- Append mode never rewrites existing symbols.

---

## 8) Known Boundaries (Current Code)

- No separate “raw universe” sheet: raw source-of-truth is GCS object + meta JSON.
- Build dedup skip check is symbol-based in sheet (not instrument-key-based).
- Invalid/changed instrument keys are handled downstream (cache/scoring), not in build stage.
- Corporate-action/rename reconciliation is **NOT IMPLEMENTED** in build stage.

