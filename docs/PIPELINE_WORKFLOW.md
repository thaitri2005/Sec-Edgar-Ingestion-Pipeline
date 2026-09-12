# SEC EDGAR Pipeline Workflow and Operations Runbook

This document is the operational source of truth for how the pipeline currently
works. It describes the implemented code, not a future design. Use it when
starting a run, diagnosing an interruption, validating results, or changing the
pipeline.

For installation and configuration basics, see [README.md](../README.md).

## 1. What the pipeline does

The pipeline discovers SEC EDGAR filings from quarterly `master.idx` files. It
always downloads the complete EDGAR submission as TXT and tracks the primary
filing document as HTML. HTML is downloaded when SEC publishes it separately or
recorded as `NOT_AVAILABLE` for legitimate source cases.

It supports these base forms:

- `10-K`
- `10-Q`
- `8-K`

Selecting a base form always includes its amendment automatically. For example,
selecting `10-K` includes both `10-K` and `10-K/A`. The actual form remains in
SQLite, while both forms are stored under the base-form directory.

The pipeline does not concatenate filings or create a separate merged metadata
file. SQLite and the individual raw artifacts are the outputs.

## 2. Important terms

| Term | Meaning |
| --- | --- |
| CIK | SEC company identifier. The pipeline stores it as a ten-digit string. |
| Accession number | Unique SEC filing identifier, such as `0000320193-24-000123`. |
| Base form | User selection: `10-K`, `10-Q`, or `8-K`. |
| Actual form | The filed type, including `/A` when the filing is an amendment. |
| Filing year | The year in `filing_date`, which controls discovery and the storage path. |
| Run | One form, year range, and CIK-selection fingerprint recorded in SQLite. |
| Artifact | One tracked filing output: required `TXT` or conditionally available `HTML`. |
| Batch | A stable group of discovered filings processed sequentially. |

The `--start-year` and `--end-year` options filter by the year the filing was
submitted to EDGAR. They do not filter by the fiscal period end date.

## 3. End-to-end flow

```text
CLI command
    |
    v
Load and validate YAML configuration
    |
    v
Create storage directories and initialize SQLite schema
    |
    v
Build selection: form + inclusive years + CIK mode
    |
    v
Create or resume matching run
    |
    v
Read SEC quarterly master indexes
    |
    v
Filter, normalize, deduplicate, order, and batch filings in SQLite
    |
    v
Process batches sequentially
    |
    +--> Download/reuse complete submission TXT
    |        |
    |        v
    |    Parse report date and primary HTML filename
    |        |
    |        v
    +--> Download/reuse primary HTML, or record NOT_AVAILABLE
             |
             v
       Validate files and atomically store them
             |
             v
       Commit artifact and filing status to SQLite
             |
             v
       stats / verify / retry-failed
```

## 4. Startup and configuration

Every command first loads `configs/config.yaml`. The relevant settings are:

```yaml
storage:
  root_directory: ../SEC_DATA

download:
  max_workers: 3
  batch_size: 10000
  retry_limit: 5
  request_delay_seconds: 0.35
  request_timeout_seconds: 60
  backoff_base_seconds: 2.0
  checksum_algorithm: sha256

discovery:
  start_year: 2009
  end_year: null
  cik_file: null
  all_ciks: false

sec:
  user_agent: "Research Organization contact@example.com"
```

Before live access, replace the example user agent with a real organization or
operator name and contact email. `SEC_USER_AGENT` overrides the YAML value when
set in the environment.

Relative paths are resolved from the directory containing the YAML file. On the
current workstation, `configs/config.yaml` is under `D:\Seed Grant Project`, so
`root_directory: ../SEC_DATA` resolves to
`D:\Seed Grant Project\SEC_DATA`.

The checked-in defaults and `configs/config.example.yaml` use three workers and
a 0.35-second request delay. The current private production profile uses five
workers and a 0.20-second delay. Requests still pass through one shared global
limiter, so worker count does not bypass the configured SEC request rate.

Configuration validation rejects:

- fewer than one worker;
- a batch size below one;
- a request delay below 0.1 seconds;
- invalid timeouts, retry counts, or checksum algorithms;
- years before 1993 or an end year before the start year; and
- a user agent without a contact email.

At startup, the local storage backend creates `raw`, `metadata`, `logs`, and
`tmp` directories. SQLite enables foreign keys, WAL journal mode, normal
synchronous mode, and a 30-second busy timeout.

## 5. Filing and company selection

Exactly one CIK mode is required.

### Selected-company mode

Pass a CSV containing a column named `cik`:

```csv
cik
0000320193
0000789019
```

Blank rows are ignored. Valid CIKs are zero-padded to ten digits and
deduplicated. Invalid rows stop the command with their row number.

```powershell
.\.venv\Scripts\sec-edgar.exe --config configs\config.yaml discover `
  --form 10-K --start-year 2024 --end-year 2024 `
  --cik-file configs\ciks.csv
```

### All-company mode

No CIK CSV is needed. The target list is populated from companies that have a
matching filing in the selected indexes.

```powershell
.\.venv\Scripts\sec-edgar.exe --config configs\config.yaml discover `
  --form 10-K --start-year 2016 --end-year 2019 --all-ciks
```

### Selection fingerprint and resume behavior

The pipeline hashes these values into a stable selection fingerprint:

- base form;
- inclusive start and end years;
- CIK mode; and
- normalized CIK list, or `ALL`.

Repeating an identical selection resumes the latest matching run whose status is
`DISCOVERING`, `PENDING`, `RUNNING`, `PARTIAL`, or `INTERRUPTED`. A completed
`SUCCESS` run is not selected for resume. Use `--new-run` only when a separate
run record is intentional.

## 6. Discovery stage

For every year in the inclusive range, discovery requests:

```text
https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/master.idx
```

Past years use all four quarters. For the current year, only quarters through
the current UTC quarter are requested.

For each index, the pipeline:

1. Rejects known SEC block pages and responses without a master-index header.
2. Loads one quarterly index response into memory, then iterates through its
   lines without constructing a second complete filing dataset.
3. Keeps the selected base form and its `/A` amendment.
4. Normalizes and optionally filters CIKs.
5. Validates accession-number format.
6. Constructs the SEC filing index and submission TXT URLs.
7. Inserts companies, filings, run membership, stable ordinal, and batch number
   into SQLite.
8. Deduplicates filings on accession number.
9. Commits after every completed quarterly index.

Batch number is calculated from stable discovery order and the configured batch
size. Batches are operational groupings only; they do not appear in raw file
paths.

On successful discovery, the run changes from `DISCOVERING` to `PENDING`.
If discovery fails, it remains `DISCOVERING` with the error so the same selection
can resume. Discovery currently replays the selected quarterly indexes from the
beginning; accession and run-membership constraints make that replay idempotent.

## 7. Download stage

The download service performs these steps:

1. Rejects a run whose discovery is incomplete.
2. Recovers stale filing/artifact records from `RUNNING` to `PENDING`.
3. Marks the run `RUNNING`.
4. Processes batch numbers sequentially.
5. Processes filings within a batch using `max_workers` threads.
6. Commits metadata after each completed filing.

The scheduler keeps up to twice the worker count submitted so workers remain
busy without submitting the entire batch at once.

### Per-filing sequence

For each filing:

1. Calculate its TXT storage path.
2. Reuse a valid existing TXT when possible; otherwise download it.
3. Validate that the TXT is nonempty, resembles an EDGAR submission, contains
   the expected accession, and is not a known SEC block page.
4. Parse the TXT document headers for the fiscal report date and the primary
   HTML filename whose `<TYPE>` matches the actual form.
5. Fall back to the first HTML filename only if there is no exact form match.
6. Build the primary-document URL from the filing index directory.
7. If the primary filename is text-only, record HTML as `NOT_AVAILABLE` and
   retain the valid complete-submission TXT; this is a successful filing state.
8. If a separately referenced primary HTML URL returns HTTP 404, record HTML as
   `NOT_AVAILABLE`; a valid TXT still allows the filing to succeed.
9. Reuse a valid existing HTML when possible; otherwise download it. Some older
   SEC primary-document responses retain an SGML `<DOCUMENT>` wrapper; in that
   case, stream only the content inside `<TEXT>...</TEXT>` to the `.htm` file.
10. Validate that HTML is nonempty, begins with an HTML tag or fragment rather
    than SEC SGML, and is not a known SEC block page.
11. Mark the filing `SUCCESS` when TXT succeeds and HTML is either `SUCCESS` or
    legitimately `NOT_AVAILABLE`.

### TXT use for NLP

The TXT artifact is intentionally preserved byte-for-byte as the complete SEC
submission. It is the archival source for downstream NLP, but it is not clean
report prose: it can contain the primary filing, exhibits, certifications,
XBRL, XML, and other documents. NLP preparation must select the `<DOCUMENT>`
whose `<TYPE>` matches the filing's actual form, take its `<TEXT>` content, and
then perform HTML parsing and text normalization. Feeding the complete TXT
directly to a model would mix the primary report with unrelated artifacts.

The ingestion pipeline does not perform that NLP transformation. Keeping raw
TXT immutable allows later preprocessing to be changed and reproduced without
redownloading from SEC.

### Important SEC URL rule

The complete submission and the filing documents use different SEC paths:

```text
TXT:
/Archives/edgar/data/{cik}/{accession-with-dashes}.txt

HTML document directory:
/Archives/edgar/data/{cik}/{accession-without-dashes}/{primary-filename}
```

Primary HTML URLs must therefore be based on the filing index directory, not the
submission TXT directory. The integration tests model this difference because a
mistake here produces HTTP 404 responses in live runs.

## 8. HTTP behavior and SEC protections

All discovery and download workers share one `SecClient` and one thread-safe
rate limiter. The configured request delay is applied globally, not separately
per worker.

The client retries:

- connection and timeout errors;
- HTTP 403, 408, and 429; and
- HTTP 5xx responses.

Retries use exponential backoff with jitter. A valid `Retry-After` header takes
precedence. A 404 from a separately referenced primary HTML document becomes
the valid `NOT_AVAILABLE` artifact state. Other HTTP 404 responses are
non-retryable because they normally indicate a bad URL or unavailable resource.

The default `0.35` second interval is approximately 2.86 requests per second,
below the SEC ceiling. Do not reduce it aggressively for production runs.

## 9. Atomic storage and layout

Each download is written beside its final destination with a `.part` suffix.
The writer flushes and syncs the file, then uses an atomic replace. Failed writes
remove the temporary file.

```text
SEC_DATA/
|-- raw/
|   |-- 10-K/{cik}/{filing_year}/{accession}.txt
|   |-- 10-K/{cik}/{filing_year}/{accession}.htm
|   |-- 10-Q/...
|   `-- 8-K/...
|-- metadata/
|   `-- metadata.db
|-- logs/
|   `-- pipeline.jsonl
`-- tmp/
```

On the current workstation, this root is
`D:\Seed Grant Project\SEC_DATA`.

Amendments use the base-form directory. Raw filings are never concatenated, and
the batch number does not affect their location.

## 10. SQLite source of truth

The database is `SEC_DATA/metadata/metadata.db`.

| Table | Purpose |
| --- | --- |
| `schema_version` | Current database schema version. |
| `companies` | Unique normalized CIK and latest observed name. |
| `filings` | One row per accession, including URLs, dates, form, status, and errors. |
| `artifacts` | TXT and HTML metadata, paths, sizes, checksums, retries, and status. |
| `runs` | Selection, configuration snapshot, lifecycle status, and timestamps. |
| `run_targets` | Exact selected CIKs, or discovered matching CIKs in all-company mode. |
| `run_filings` | Stable run membership, ordinal, and batch number. |

Filing statuses are:

```text
PENDING -> RUNNING -> SUCCESS
                   `-> FAILED
```

Artifacts use the same states plus `NOT_AVAILABLE`. That state applies only to
the HTML artifact when SEC identifies the primary filing document as text-only
or its separately referenced primary HTML URL returns 404. It is not a download
failure and has no local HTML path, size, or checksum.

Run statuses are:

| Status | Meaning |
| --- | --- |
| `DISCOVERING` | Quarterly index discovery is incomplete or resumable. |
| `PENDING` | Discovery completed; downloads have not completed. |
| `RUNNING` | Download processing is active or was not shut down cleanly. |
| `SUCCESS` | Every filing has valid TXT and HTML is `SUCCESS` or legitimately `NOT_AVAILABLE`. |
| `PARTIAL` | Downloading ended but at least one filing is incomplete or failed. |
| `INTERRUPTED` | User interruption or an unexpected service-level failure occurred. |
| `FAILED` | Reserved run state; normal incomplete downloads currently finish as `PARTIAL`. |

SQLite is updated transactionally after every filing. A successful artifact is
retained when the other artifact fails, and later retries reuse the valid file.

## 11. Commands and when to use them

| Command | Use |
| --- | --- |
| `run` | Perform discovery and then download every batch. Convenient, but gives no review point between stages. |
| `discover` | Populate SQLite only. Recommended first step for large production selections. |
| `download --run-id N` | Download pending filings for a previously discovered run. |
| `stats [--run-id N]` | Show counts by status, form, year, batch, and bytes. Defaults to latest run. |
| `verify [--run-id N]` | Compare SQLite metadata with files. Read-only by default. |
| `verify --full-checksum` | Also recalculate and compare SHA-256 checksums. Slower but strongest validation. |
| `verify --mark-failed` | Mark filings with validation problems as failed. This changes SQLite state. |
| `retry-failed [--run-id N]` | Reset failed records and process incomplete filings again. |

Exit codes:

- `0`: command completed successfully;
- `1`: unexpected command failure;
- `2`: command-line/configuration error, partial download, or verification
  discrepancies; and
- `130`: interrupted with Ctrl+C or SIGTERM.

## 12. Recommended pilot procedure

Use selected-company mode and one completed year:

```powershell
# 1. Discover only
.\.venv\Scripts\sec-edgar.exe --config configs\config.yaml discover `
  --form 10-K --start-year 2024 --end-year 2024 `
  --cik-file configs\ciks.csv

# 2. Inspect the returned run ID
.\.venv\Scripts\sec-edgar.exe --config configs\config.yaml stats --run-id N

# 3. Download
.\.venv\Scripts\sec-edgar.exe --config configs\config.yaml download --run-id N

# 4. Verify metadata, content, sizes, and checksums
.\.venv\Scripts\sec-edgar.exe --config configs\config.yaml verify `
  --run-id N --full-checksum
```

This process was exercised live with Apple and Microsoft 2024 10-K filings on
2026-08-13: two filings were discovered, downloaded, and checksum-verified.

## 13. Recommended large production procedure

For a large all-company run, separate discovery from downloading:

```powershell
# 1. Discover all matching filings and note the printed run ID
.\.venv\Scripts\sec-edgar.exe --config configs\config.yaml discover `
  --form 10-K --start-year 2016 --end-year 2019 --all-ciks

# 2. Review expected scale before downloading
.\.venv\Scripts\sec-edgar.exe --config configs\config.yaml stats --run-id N

# 3. Confirm available disk space, then download
.\.venv\Scripts\sec-edgar.exe --config configs\config.yaml download --run-id N

# 4. Inspect counts
.\.venv\Scripts\sec-edgar.exe --config configs\config.yaml stats --run-id N

# 5. Verify all files
.\.venv\Scripts\sec-edgar.exe --config configs\config.yaml verify `
  --run-id N --full-checksum
```

Do not use `--new-run` merely to continue an interrupted selection. Resume the
existing run so successful artifacts and stable batch membership are preserved.

## 14. Interruption and recovery playbooks

### Ctrl+C or machine restart

1. Stop with Ctrl+C once and allow workers to finish or cancel safely.
2. Inspect the run with `stats --run-id N`.
3. Resume with `download --run-id N`, or repeat the identical original `run`
   selection.
4. Startup recovery changes stale `RUNNING` records to `PENDING`.
5. Existing valid files are reused rather than downloaded again.

### Run ends as PARTIAL

```powershell
.\.venv\Scripts\sec-edgar.exe --config configs\config.yaml stats --run-id N
.\.venv\Scripts\sec-edgar.exe --config configs\config.yaml retry-failed --run-id N
.\.venv\Scripts\sec-edgar.exe --config configs\config.yaml verify --run-id N
```

`download` processes `PENDING` and recovered `RUNNING` filings. It does not reset
filings already marked `FAILED`; `retry-failed` performs that reset.

### Verification finds corrupt or missing files

Run read-only verification first, then explicitly mark and retry:

```powershell
.\.venv\Scripts\sec-edgar.exe --config configs\config.yaml verify `
  --run-id N --full-checksum

.\.venv\Scripts\sec-edgar.exe --config configs\config.yaml verify `
  --run-id N --full-checksum --mark-failed

.\.venv\Scripts\sec-edgar.exe --config configs\config.yaml retry-failed --run-id N
```

## 15. Monitoring and logs

Console output reports discovery quarters, batch starts, periodic progress, and
the final run status. Detailed JSON-lines logs are written to:

```text
SEC_DATA/logs/pipeline.jsonl
```

Each record includes a UTC timestamp, severity, event name, message, and relevant
context such as run ID, batch, accession, CIK, retry count, duration, status, or
error. Logs rotate according to `logging.max_bytes` and `backup_count`.

Useful monitoring commands:

```powershell
.\.venv\Scripts\sec-edgar.exe --config configs\config.yaml stats --run-id N

.\.venv\Scripts\sec-edgar.exe --config configs\config.yaml stats `
  --run-id N --json

Get-Content SEC_DATA\logs\pipeline.jsonl -Tail 20
```

## 16. Safe upgrade procedure

1. Stop the active downloader cleanly.
2. Record the active run ID and run `stats`.
3. Back up `SEC_DATA/metadata/metadata.db` and its WAL files while no process is
   writing to the database.
4. Pull and review the code change.
5. Reinstall the editable package if packaging or dependencies changed:

   ```powershell
   .\.venv\Scripts\python.exe -m pip install -e ".[dev]"
   ```

6. Run the offline checks:

   ```powershell
   .\.venv\Scripts\python.exe -m pytest
   .\.venv\Scripts\python.exe -m compileall -q src
   ```

7. Resume the existing run ID and verify results.

Runtime data, personal configuration, CIK lists, logs, databases, and `.part`
files are ignored by Git.

## 17. Code ownership map

| Module | Responsibility |
| --- | --- |
| `cli.py` | Command parsing and top-level orchestration. |
| `config.py` | YAML loading, path resolution, environment override, and validation. |
| `discovery.py` | CIK loading, selection fingerprints, master-index parsing, and discovery. |
| `sec_client.py` | SEC sessions, global rate limiting, retries, and streaming responses. |
| `downloader.py` | Batch scheduling, TXT/HTML acquisition, reuse, and per-filing results. |
| `validator.py` | Form/CIK/accession normalization, artifact checks, and TXT header parsing. |
| `storage.py` | Storage protocol, safe paths, atomic writes, and checksums. |
| `metadata.py` | SQLite schema, transactions, status transitions, statistics, and run queries. |
| `checkpoint.py` | Recovery of stale running records. |
| `operations.py` | Read-only verification and explicit mark-failed repair action. |
| `logging_config.py` | Console and rotating JSON-lines logging. |
| `models.py` | Status enums and immutable data-transfer records. |

## 18. Testing and production confidence

The offline suite covers configuration, index parsing, amendment inclusion, CIK
normalization, storage validation, SEC retry behavior, batching, duplicate
avoidance, partial artifact reuse, interrupted-run recovery, and verification.
It uses mocked SEC responses and temporary SQLite/storage directories.

The pipeline has completed the following large all-company downloads on the
current workstation:

| Run | Selection | Discovered | Successful | Failed | Final status |
| --- | --- | ---: | ---: | ---: | --- |
| 2 | 10-K, 2009–2025 | 149,158 | 149,157 | 1 | `PARTIAL` |
| 4 | 10-Q, 2009–2025 | 375,990 | 375,989 | 1 | `PARTIAL` |
| 6 | 8-K, 2018–2025 | 560,166 | 560,166 | 0 | `SUCCESS` |

That is 1,085,312 successful filings out of 1,085,314 discovered. The two
`PARTIAL` runs each retain one unresolved source edge case after retries; the
successful artifacts remain usable. Treat a new form/range or a material
pipeline change as a monitored production change:

- inspect statistics after discovery;
- monitor logs and disk consumption;
- verify after downloading;
- retain SQLite backups; and
- avoid running multiple downloader processes against the same data root unless
  that concurrency pattern is tested explicitly.

## 19. Operator checklists

### Before a run

- Confirm the SEC user agent contains a real contact email.
- Confirm the form and inclusive filing-year range.
- Confirm exactly one CIK mode.
- Run the offline test suite after code changes.
- Prefer discovery-only for large selections.
- Review discovered count and estimated storage capacity.
- Record the run ID.

### After a run

- Confirm the run status and counts with `stats`.
- Run `verify --full-checksum`.
- Investigate errors before bulk retrying.
- Use `verify --mark-failed` only when repair is intended.
- Back up `metadata.db` after a major completed run.
- Preserve the JSONL logs needed for audit or diagnosis.

## 20. Companion: Vietnam annual-report NLP pipeline

Everything above describes the implemented SEC EDGAR pipeline. The separate
Vietnamese annual-report pipeline now has its own `vn-reports` CLI, package,
configuration, SQLite schema, and runtime root.

The companion design deliberately reuses the operational principles that have
worked for SEC ingestion:

- immutable raw source artifacts;
- SQLite as the checkpoint and provenance source of truth;
- stable run membership and batches;
- streaming/resumable downloads and atomic finalization;
- SHA-256 validation;
- stage-specific status, verification, and failed-only retry; and
- versioned, reproducible downstream transformations.

The data model remains separate because the Vietnamese source consists of PDFs
identified by dataset records, tickers, and report years—not SEC CIKs,
accessions, forms, complete-submission TXT, and primary HTML.

The fixed initial scope is:

| Property | Value |
| --- | --- |
| Source | Zenodo record `20949551` |
| Dataset version | `1.0.0` |
| Report years | 2008–2025 inclusive |
| Expected reports | 13,884 PDFs |
| Source download | Four archives, approximately 134.7 GB |
| NLP outputs | Page JSONL and document TXT |
| OCR | Selective Vietnamese/English OCR for low-quality pages |
| Live exchange collection | Deferred |
| Runtime root on this workstation | `D:\Seed Grant Project\VN_DATA` |

See [VIETNAM_ANNUAL_REPORT_PIPELINE.md](VIETNAM_ANNUAL_REPORT_PIPELINE.md) for
the full data flow, CLI, configuration, storage layout, SQLite schema,
PDF/OCR quality model, recovery rules, pilot procedure, and production plan.
