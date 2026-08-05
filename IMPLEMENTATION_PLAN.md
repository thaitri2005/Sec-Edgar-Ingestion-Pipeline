# SEC EDGAR Local Ingestion Pipeline

## Summary

- Replace the notebook workflow with a modular, installable Python application while leaving `Data_Collection_Method.ipynb` unchanged as reference only.
- Preserve quarterly discovery, 10,000-filing chunks, three-worker parallelism, automatic batch iteration, and resumability.
- Store raw HTML and TXT filings by form, CIK, year, and accession without concatenating raw files.
- Use SQLite as the sole metadata and checkpoint source.

## Application Interface

- Provide `run`, `discover`, `download`, `stats`, `verify`, and `retry-failed` CLI commands.
- Accept `10-K`, `10-Q`, or `8-K`, automatically including the matching amendment form.
- Support inclusive start and end years from CLI overrides or YAML defaults.
- Support a deduplicated CIK CSV or explicit all-company discovery.
- Require explicit non-interactive arguments for dedicated-server execution.

## Architecture and Data Flow

- Use an installable package under `src/sec_edgar_pipeline/` with separate configuration, HTTP, discovery, downloading, storage, metadata, validation, checkpoint, logging, and CLI modules.
- Stream and filter quarterly `master.idx` records, persist stable run order and batch numbers, then process every batch automatically.
- Download complete submission TXT files, identify the primary filing document, and download the actual HTML document.
- Store files under `SEC_DATA/raw/{base_form}/{cik}/{filing_year}/{accession}.{txt,htm}` using atomic temporary files.
- Isolate filesystem operations behind a storage backend protocol for future cloud implementations.

## SQLite and Reliability

- Store metadata in `SEC_DATA/metadata/metadata.db` using WAL mode, foreign keys, versioned schema setup, and indexes.
- Maintain companies, filings, artifacts, runs, run targets, and run filings.
- Use `PENDING`, `RUNNING`, `SUCCESS`, and `FAILED` statuses; filing success requires both valid artifacts.
- Recover stale running work, preserve valid partial downloads, and retry only missing or invalid artifacts.
- Apply thread-safe rate limiting, bounded retries, exponential backoff, atomic writes, SHA-256 checksums, and content validation.
- Keep SQLite as the only checkpoint and metadata source; do not create batch CSV merge files.

## Operations, Tests, and Deployment

- Provide operational statistics, read-only verification, explicit failure marking, and failed-only retry commands.
- Log structured JSON lines to rotating files and concise progress to the console.
- Add unit and integration tests using mocked HTTP and temporary storage only.
- Add packaging, pinned dependencies, example configuration, Git exclusions, README instructions, and a Linux systemd example.
- Keep runtime data, logs, private configuration, and SQLite files outside Git.

## Assumptions

- CIK CSV runs process every unique CIK in the file; `--all-ciks` recreates broad notebook discovery.
- The default start year is 2009 and the end year is configurable.
- Amendment filings retain their actual form in metadata but share their base-form storage directory.
- Existing notebook/cloud outputs are not imported in the initial implementation.
