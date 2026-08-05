# SEC EDGAR Local Ingestion Pipeline

Fault-tolerant local acquisition of SEC EDGAR `10-K`, `10-Q`, and `8-K` filings. Selecting a base form automatically includes its amendment form (`/A`). The pipeline stores the complete submission TXT and actual primary HTML document for every filing.

The original `Data_Collection_Method.ipynb` remains a reference and is not used at runtime.

## Features

- Quarterly `master.idx` discovery with inclusive year filtering.
- Optional CIK CSV targeting or explicit all-company discovery.
- Automatic 10,000-filing batches with three parallel workers by default.
- SQLite metadata, checkpoints, stable batch membership, and crash recovery.
- Atomic writes, SHA-256 checksums, retries, global SEC rate limiting, and validation.
- Local storage abstraction designed for future cloud backends.
- Structured JSON-lines logs and operational `stats`, `verify`, and `retry-failed` commands.

## Installation

Python 3.11 or newer is required.

```powershell
git clone <repository-url>
cd Sec-Edgar-Ingestion-Pipeline
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e .
Copy-Item configs/config.example.yaml configs/config.yaml
Copy-Item configs/ciks.example.csv configs/ciks.csv
```

Linux activation uses `source .venv/bin/activate`. Keep personal configuration, CIK lists, logs, data, and SQLite files outside Git.

## Configuration

Edit `configs/config.yaml`:

```yaml
storage:
  root_directory: D:/SEC_DATA

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
  cik_file: ciks.csv
  all_ciks: false

sec:
  user_agent: "Research Organization contact@example.com"
```

Relative paths are resolved from the configuration file directory. `SEC_USER_AGENT` overrides the YAML user agent and is recommended for server deployment. The program rejects request delays below 0.1 seconds so the process cannot exceed the SEC's published ten-request-per-second ceiling.

The CIK CSV must contain a `cik` header. CIKs are validated, zero-padded to ten digits, and deduplicated before discovery.

## Commands

Run discovery and every download batch automatically:

```powershell
sec-edgar --config configs/config.yaml run --form 10-K --start-year 2009 --end-year 2025 --cik-file configs/ciks.csv
```

If `--form` is omitted in an interactive terminal, choose `10-K`, `10-Q`, or `8-K` from a prompt. Non-interactive runs must provide it explicitly.

Use all companies matching the selected form and years:

```powershell
sec-edgar --config configs/config.yaml run --form 8-K --start-year 2020 --end-year 2025 --all-ciks
```

Separate discovery and downloading for easier debugging:

```powershell
sec-edgar --config configs/config.yaml discover --form 10-Q --start-year 2022 --end-year 2025 --cik-file configs/ciks.csv
sec-edgar --config configs/config.yaml download --run-id 1
```

Operational commands default to the latest run when `--run-id` is omitted:

```powershell
sec-edgar --config configs/config.yaml stats --run-id 1
sec-edgar --config configs/config.yaml stats --run-id 1 --json
sec-edgar --config configs/config.yaml verify --run-id 1
sec-edgar --config configs/config.yaml verify --run-id 1 --full-checksum
sec-edgar --config configs/config.yaml verify --run-id 1 --mark-failed
sec-edgar --config configs/config.yaml retry-failed --run-id 1
```

`verify` is read-only unless `--mark-failed` is supplied. Marking failures allows `retry-failed` to redownload only missing or invalid artifacts.

## Resume Behavior

The selection of form, year range, and normalized CIK list produces a stable fingerprint. Repeating the same `run` command resumes the latest unfinished matching run. Use `--new-run` only when a separate run record is intentional.

SQLite is updated after every filing. On restart, stale `RUNNING` rows return to `PENDING`. Existing files are validated and reconciled, so successfully downloaded TXT or HTML artifacts are not downloaded again.

## Storage Layout

```text
SEC_DATA/
├── raw/
│   ├── 10-K/0000320193/2024/{accession}.txt
│   ├── 10-K/0000320193/2024/{accession}.htm
│   ├── 10-Q/
│   └── 8-K/
├── metadata/metadata.db
├── logs/pipeline.jsonl
└── tmp/
```

Amendments retain their actual form in SQLite but use the matching base-form directory. Batches never affect raw file paths, and raw filings are never concatenated.

## SQLite Metadata

SQLite is the source of truth. The main tables are `companies`, `filings`, `artifacts`, `runs`, `run_targets`, and `run_filings`.

```sql
SELECT *
FROM filings
WHERE cik = '0000320193'
  AND base_form = '10-K'
  AND substr(filing_date, 1, 4) BETWEEN '2015' AND '2020';

SELECT * FROM filings WHERE status = 'FAILED';

SELECT filing_type, COUNT(*)
FROM filings
GROUP BY filing_type;

SELECT f.accession_number, a.kind, a.local_path, a.file_size, a.checksum
FROM filings AS f
JOIN artifacts AS a USING (accession_number)
WHERE f.accession_number = '0000320193-24-000123';
```

Filing status becomes `SUCCESS` only when both required artifacts validate and their metadata commits. Valid partial artifacts are preserved across retries.

## Logging

Console logs show concise progress. `SEC_DATA/logs/pipeline.jsonl` contains rotating structured records with run ID, batch ID, accession, CIK, attempts, duration, status, and error context. No operational code uses notebook-style `print` logging.

## Dedicated Server Deployment

1. Clone the repository to `/opt/sec-edgar-ingestion`.
2. Create a virtual environment and run `pip install -e .`.
3. Put configuration and CIK files under `/etc/sec-edgar/`.
4. Put the data root on the dedicated storage volume.
5. Adapt `deploy/sec-edgar.service.example`, copy it to `/etc/systemd/system/sec-edgar.service`, and run:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now sec-edgar.service
sudo systemctl status sec-edgar.service
```

Create separate services for forms that should run independently. The service must use explicit form, years, and CIK mode because it has no interactive terminal. `SIGTERM` stops scheduling new work and leaves resumable SQLite state.

For Git-based upgrades, stop the service, back up `metadata.db`, pull the reviewed revision, reinstall the package, run tests, and restart the service. Runtime data remains outside the repository.

## Development

```powershell
python -m pip install -e ".[dev]"
python -m pytest
python -m compileall -q src
```

Tests use mocked SEC responses and temporary storage; the default suite never contacts SEC EDGAR.

Automated access must identify the operator and comply with the [SEC developer and fair-access guidance](https://www.sec.gov/about/developer-resources).
