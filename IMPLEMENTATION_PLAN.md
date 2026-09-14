# Data Ingestion Pipelines Implementation Plan

Last updated: 2026-09-13

## Project Status

| Pipeline | Scope | Status | Detailed documentation |
| --- | --- | --- | --- |
| SEC EDGAR | 10-K/10-Q for 2009–2025; 8-K for 2018–2025 | Implemented and operational | [SEC workflow](docs/PIPELINE_WORKFLOW.md) |
| Vietnam annual reports | Historical reports for 2008–2025 | Implemented; catalog validated, production PDF download pending | [Vietnam implementation plan](docs/VIETNAM_IMPLEMENTATION_PLAN.md) |

## SEC EDGAR Pipeline

## Summary

- Replace the notebook workflow with a modular, installable Python application while leaving `Data_Collection_Method.ipynb` unchanged as reference only.
- Preserve quarterly discovery, 10,000-filing chunks, three-worker parallelism, automatic batch iteration, and resumability.
- Store raw TXT and, when available, primary HTML by form, CIK, year, and accession without concatenating raw files.
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
- Download complete submission TXT files, identify the primary filing document, and download the separately hosted HTML document when available.
- Store files under `SEC_DATA/raw/{base_form}/{cik}/{filing_year}/{accession}.{txt,htm}` using atomic temporary files.
- Isolate filesystem operations behind a storage backend protocol for future cloud implementations.

## SQLite and Reliability

- Store metadata in `SEC_DATA/metadata/metadata.db` using WAL mode, foreign keys, versioned schema setup, and indexes.
- Maintain companies, filings, artifacts, runs, run targets, and run filings.
- Use `PENDING`, `RUNNING`, `SUCCESS`, and `FAILED` statuses plus artifact-level `NOT_AVAILABLE`; filing success requires valid TXT and HTML that is either valid or legitimately unavailable.
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

## Current Workstation Layout

- Treat `D:\Seed Grant Project` as the canonical base folder.
- Resolve `configs/config.yaml` value `../SEC_DATA` to
  `D:\Seed Grant Project\SEC_DATA`.
- Resolve `configs/vn_reports.yaml` value `../VN_DATA` to
  `D:\Seed Grant Project\VN_DATA`.
- Keep both runtime roots ignored by Git while retaining them under the same
  movable base folder as the code and documentation.

## Companion Pipeline: Vietnam Annual Reports

This is a separate implementation, not an extension of the SEC database schema.
It is exposed through the independent `vn-reports` CLI.

- Use version `1.0.0` of the Zenodo *Vietnam Listed Companies Annual Reports
  PDF Dataset* (`10.5281/zenodo.20949551`) as the fixed historical source.
- Select report years 2008–2025 inclusive: 13,884 expected PDFs contained in
  four archives totaling approximately 134.7 GB.
- Defer live HSX/HNX/UPCoM discovery and post-2025 updates.
- Preserve original PDFs unchanged and create page-level JSONL plus
  document-level UTF-8 TXT for NLP.
- Extract native PDF text first and selectively OCR only empty or low-quality
  pages with Vietnamese and English language support.
- Use a separate `VN_DATA` root, SQLite database, configuration, package
  namespace, and `vn-reports` CLI.
- Track catalog, download, extraction, OCR, and normalization stages
  independently so each stage can resume and retry without invalidating valid
  upstream artifacts.
- Pin dataset, code, extraction, OCR, and normalization versions for research
  reproducibility.

The implemented architecture, schema, storage layout, quality controls, and
operating model are documented in
[`docs/VIETNAM_ANNUAL_REPORT_PIPELINE.md`](docs/VIETNAM_ANNUAL_REPORT_PIPELINE.md).
The ordered phases, checklists, dependencies, verification gates, pilot, and
production acceptance criteria are maintained in
[`docs/VIETNAM_IMPLEMENTATION_PLAN.md`](docs/VIETNAM_IMPLEMENTATION_PLAN.md).

Current Vietnam milestone status: the package and complete stage workflow are
implemented. Live run 1 cataloged exactly 13,884 documents across 1,391 tickers
for 2008–2025, and all 13,884 source PDFs were downloaded and checksum-verified.
Native extraction produced usable page artifacts for 13,858 reports; 26 malformed
source PDFs are recorded as permanent extraction failures. Selective `vie+eng`
OCR is now in bounded corpus calibration. A scanned 29-page production report
completed with 36,748 extracted characters and one sparse-page quality warning.

## Plan Maintenance

- Update this project-level status whenever a pipeline or major milestone moves
  between planned, in-progress, operational, or deferred states.
- Maintain detailed phase status in the pipeline-specific implementation plan.
- Update workflow documentation and operator instructions in the same commit as
  behavior, configuration, schema, storage, or command changes.
- Do not mark work complete until its documented verification gate passes.
