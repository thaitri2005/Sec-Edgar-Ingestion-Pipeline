# Vietnam Annual-Report Pipeline Implementation Plan

## Document control

| Field | Value |
| --- | --- |
| Status | Planned; implementation has not started |
| Last updated | 2026-09-13 |
| Canonical project root | `D:\Seed Grant Project` |
| Runtime data root | `D:\Seed Grant Project\VN_DATA` |
| Historical scope | Report years 2008–2025 inclusive |
| Dataset | Zenodo record `20949551`, version `1.0.0` |
| Expected source reports | 13,884 PDFs |

This plan turns the architecture in
[VIETNAM_ANNUAL_REPORT_PIPELINE.md](VIETNAM_ANNUAL_REPORT_PIPELINE.md) into
ordered implementation work. Checkboxes describe repository state, not whether
the source dataset itself exists.

## 1. Completion definition

The initial historical pipeline is complete when it can:

- catalog the fixed Zenodo dataset and select reports from 2008–2025;
- resume and verify all archive downloads;
- safely extract and checksum every selected source PDF;
- extract page-level native text without changing the source PDFs;
- detect pages that need OCR and OCR only those pages with Vietnamese and
  English language support;
- produce reproducible page JSONL and document TXT outputs;
- track every stage, attempt, artifact, version, and error in SQLite;
- resume after interruption and retry only failed or incomplete stage work;
- report coverage and quality by company, report year, extraction method, and
  status; and
- pass the pilot acceptance gate before the full 13,884-report run begins.

Live exchange crawling and post-2025 updates are outside this milestone.

## 2. Status overview

| Phase | Deliverable | Status | Depends on |
| --- | --- | --- | --- |
| 0 | Source, license, and storage preflight | Not started | None |
| 1 | Package and CLI scaffold | Not started | Phase 0 |
| 2 | Configuration, storage, and SQLite foundation | Not started | Phase 1 |
| 3 | Zenodo catalog ingestion | Not started | Phase 2 |
| 4 | Resumable archive download | Not started | Phase 3 |
| 5 | Safe PDF extraction and reconciliation | Not started | Phase 4 |
| 6 | Native PDF text extraction and quality scoring | Not started | Phase 5 |
| 7 | Selective Vietnamese/English OCR | Not started | Phase 6 |
| 8 | Versioned text normalization | Not started | Phases 6–7 |
| 9 | Operations, verification, and recovery commands | Not started | Phases 3–8 |
| 10 | Automated test suite and documentation | Not started | Phases 1–9 |
| 11 | Stratified pilot and threshold calibration | Not started | Phase 10 |
| 12 | Full historical production run | Not started | Phase 11 |

Only change a phase to `In progress` when repository work has begun. Change it
to `Complete` only after its verification gate passes.

## 3. Phase 0 — source and environment preflight

### Work

- [ ] Save the Zenodo citation, DOI, record ID, version, publication metadata,
  and license in the project documentation.
- [ ] Confirm the four archive names, byte sizes, and published checksums from
  the record metadata.
- [ ] Inspect `file_index_full.csv`, checksum manifests, coverage files, and the
  data dictionary without downloading the full corpus.
- [ ] Confirm that report year, ticker, source record ID, archive member path,
  size, and PDF checksum are sufficient to identify each selected document.
- [ ] Estimate disk requirements for archives, extracted PDFs, page JSONL,
  normalized TXT, optional OCR PDFs, temporary files, and safety margin.
- [ ] Confirm installation options for PyMuPDF and Tesseract with `vie` and
  `eng` language data on the production workstation.
- [ ] Record any dataset anomalies as explicit assumptions or review items.

### Verification gate

- Source metadata and licensing are documented.
- Expected archive and PDF counts reconcile with the published manifests.
- Available disk space exceeds the documented production requirement.
- No production download begins during this phase.

## 4. Phase 1 — package and CLI scaffold

### Work

- [ ] Add a separate package namespace under `src/vn_annual_reports/`.
- [ ] Add the `vn-reports` console entry point without changing `sec-edgar`.
- [ ] Define command shells for `catalog`, `download`, `extract`, `ocr`,
  `normalize`, `stats`, `verify`, and `retry-failed`.
- [ ] Add `configs/vn_reports.example.yaml`; keep the private
  `configs/vn_reports.yaml` ignored by Git.
- [ ] Add dependency groups so OCR tooling is optional for users who only need
  cataloging, downloading, or native extraction.
- [ ] Ensure `VN_DATA/` remains ignored by Git.

### Verification gate

- `vn-reports --help` and every subcommand help page run successfully.
- Importing the Vietnam package does not initialize storage or access a network.
- Existing SEC CLI and tests remain unchanged and passing.

## 5. Phase 2 — configuration, storage, and SQLite foundation

### Work

- [ ] Implement typed configuration with paths resolved relative to the YAML
  file; `../VN_DATA` must resolve to `D:\Seed Grant Project\VN_DATA` here.
- [ ] Validate years, worker counts, timeouts, retry limits, hash algorithms,
  extraction thresholds, OCR languages, and normalization profiles.
- [ ] Implement safe path joining, `.part` files, flush/sync, atomic replace,
  streaming SHA-256, and cleanup of failed temporary files.
- [ ] Create the separate SQLite database at
  `VN_DATA/metadata/metadata.db` with schema versioning, foreign keys, WAL mode,
  indexes, and a busy timeout.
- [ ] Add tables for datasets, companies, documents, source files, runs, run
  documents, artifacts, stage executions, page quality, and review issues.
- [ ] Define stage states `PENDING`, `RUNNING`, `SUCCESS`, `FAILED`, `SKIPPED`,
  and `NOT_REQUIRED`, including allowed transitions.
- [ ] Store dataset, code, native extractor, OCR engine/languages, and
  normalization profile versions.

### Verification gate

- A temporary config creates the exact documented directory tree.
- Schema creation and repeated initialization are idempotent.
- Invalid configuration and unsafe paths fail before any mutation.
- Transaction and recovery tests prove that a partially committed stage cannot
  be reported as successful.

## 6. Phase 3 — Zenodo catalog ingestion

### Work

- [ ] Fetch and persist the Zenodo record metadata with retrieval timestamp.
- [ ] Download small metadata/manifests first and verify published hashes.
- [ ] Parse the file index as a stream where practical.
- [ ] Normalize tickers and years while preserving original values.
- [ ] Reject path traversal, malformed hashes, duplicate identities, invalid
  years, and references to unknown archives.
- [ ] Select 2008–2025 and create a stable dataset/run fingerprint.
- [ ] Insert documents in deterministic source-index order and assign stable
  operational batches.
- [ ] Generate pre-download coverage summaries and a review-issue report.

### Verification gate

- Catalog reruns are idempotent and do not duplicate documents.
- Selected document count is 13,884 or any difference is explained and recorded.
- Counts reconcile by year, ticker, archive, and status.
- No large archive is downloaded by `catalog`.

## 7. Phase 4 — resumable archive download

### Work

- [ ] Implement streaming archive downloads with bounded retries, exponential
  backoff, timeouts, and structured progress logs.
- [ ] Support HTTP range resume only when the server response proves it is safe;
  otherwise restart the individual archive cleanly.
- [ ] Preflight free disk space before each archive.
- [ ] Verify final byte size and published checksum before atomic promotion.
- [ ] Reuse a valid existing archive on resume.
- [ ] Record attempts, response metadata, duration, size, hash, and error details
  in SQLite.

### Verification gate

- Interrupted downloads resume or restart without corrupt final artifacts.
- Truncated, HTML error, and checksum-mismatched responses never become
  successful archives.
- A valid archive is not downloaded twice.

## 8. Phase 5 — safe PDF extraction and reconciliation

### Work

- [ ] Reject absolute archive paths, `..` traversal, links, unexpected file
  types, duplicate members, and decompression-limit violations.
- [ ] Extract selected PDFs through temporary files and atomic promotion.
- [ ] Validate PDF signature, nonzero size, expected size, and manifest SHA-256.
- [ ] Store PDFs under
  `VN_DATA/raw/annual-reports/{ticker}/{report_year}/{document_id}.pdf`.
- [ ] Reconcile every selected catalog row to exactly one verified PDF artifact.
- [ ] Preserve archive member names and source identifiers as provenance.

### Verification gate

- Security tests cover path traversal, duplicate members, decompression bombs,
  corrupt ZIPs, and disguised non-PDF content.
- Extraction is idempotent and reuses checksum-valid PDFs.
- Missing, duplicate, or mismatched PDFs appear in reports and SQLite.

## 9. Phase 6 — native extraction and quality scoring

### Work

- [ ] Extract text one page at a time with PyMuPDF while preserving page order.
- [ ] Write raw page JSONL with document ID, page number, dimensions, native
  text, character count, and extraction metadata.
- [ ] Calculate page and document quality signals: character count, printable
  ratio, replacement/control characters, word-like token ratio, and text
  coverage.
- [ ] Classify pages as acceptable native text, OCR candidates, or review cases.
- [ ] Keep extraction outputs versioned and never overwrite source PDFs.
- [ ] Make processing resumable at document level and bounded in memory.

### Verification gate

- Page count and ordering match the source PDF.
- Vietnamese diacritics survive UTF-8 serialization and round trips.
- Re-running the same extractor version produces stable output.
- Low-text scanned pages are selected for OCR while good native pages are not.

## 10. Phase 7 — selective OCR

### Work

- [ ] Render only OCR-candidate pages at a documented DPI.
- [ ] Run Tesseract with `vie+eng` and record engine, language-data, DPI, and
  preprocessing versions.
- [ ] Compare OCR output with native quality and select the better page result
  using explicit, testable rules.
- [ ] Mark pages that remain poor as review issues rather than silently passing.
- [ ] Delete disposable rendered page images after successful processing.
- [ ] Make searchable OCR PDFs optional because page JSONL/TXT are the required
  NLP outputs.

### Verification gate

- A manually reviewed sample includes Vietnamese prose, English sections,
  tables, scanned pages, and mixed native/scanned reports.
- OCR is not invoked for pages already passing native-text thresholds.
- Selected page text retains extraction method and provenance.

## 11. Phase 8 — versioned normalization

### Work

- [ ] Define a named normalization profile with Unicode NFC, whitespace rules,
  line-wrap handling, and repeated header/footer detection.
- [ ] Preserve page boundaries and extraction method in page JSONL.
- [ ] Build one UTF-8 document TXT in page order from the selected page text.
- [ ] Do not translate, replace missing values, discard tables, or overwrite raw
  extraction.
- [ ] Store output hashes and all transformation versions in SQLite.

### Verification gate

- Normalization is deterministic for a fixed profile and input hash.
- Page JSONL can reconstruct the document TXT in the documented order.
- A profile change creates new derived artifacts instead of overwriting prior
  outputs.

## 12. Phase 9 — operations, verification, and recovery

### Work

- [ ] Implement statistics by run, stage, status, year, ticker, extraction
  method, OCR usage, bytes, and quality band.
- [ ] Implement read-only verification of database rows, files, sizes, and
  content signatures, plus optional full checksums.
- [ ] Require an explicit flag before verification marks work failed.
- [ ] Implement failed-only retry by stage without invalidating successful
  upstream artifacts.
- [ ] Recover stale `RUNNING` work after interruption.
- [ ] Produce rotating JSONL logs with run, document, stage, attempt, duration,
  and error context.

### Verification gate

- Ctrl+C followed by the documented resume command loses no successful work.
- Deliberately missing, truncated, and hash-mismatched artifacts are detected.
- Retry scope is limited to failed/incomplete stages and documents.

## 13. Phase 10 — tests and documentation

### Work

- [ ] Add unit tests for configuration, identifiers, paths, state transitions,
  quality scoring, normalization, and manifest parsing.
- [ ] Add integration tests with a tiny synthetic archive, native-text PDF,
  scanned PDF, mixed PDF, corrupt PDF, and mocked HTTP responses.
- [ ] Test retry, range resume, checksum failure, interruption, stale-state
  recovery, artifact reuse, and safe extraction.
- [ ] Keep all automated tests offline and use temporary storage/SQLite.
- [ ] Add installation, Tesseract language-pack, pilot, production, recovery,
  verification, and citation instructions.
- [ ] Update this plan whenever scope, phase status, acceptance criteria, paths,
  or major implementation decisions change.

### Verification gate

- The full offline suite passes on Windows from a clean environment.
- SEC tests still pass.
- README, architecture, implementation plan, example config, and CLI help agree.

## 14. Phase 11 — stratified pilot

### Work

- [ ] Select a small reproducible sample spanning early/middle/recent years,
  multiple tickers, small/large PDFs, native text, scanned pages, and mixed
  documents.
- [ ] Run the complete catalog-to-normalization workflow on that sample.
- [ ] Manually review page ordering, Vietnamese characters, OCR decisions,
  tables, headers/footers, and normalized TXT.
- [ ] Calibrate extraction and OCR thresholds from recorded evidence.
- [ ] Measure runtime, peak temporary space, permanent storage growth, and OCR
  share to improve the production estimate.
- [ ] Save a pilot report and approved configuration snapshot.

### Verification gate

- Every pilot source PDF, output, status, checksum, and version reconciles.
- No unresolved systematic text-loss or page-order defect remains.
- Quality thresholds and production storage/time estimates are documented.
- The user explicitly approves moving to the full historical run.

## 15. Phase 12 — full historical production run

### Work

- [ ] Back up the approved config and SQLite database before each major stage.
- [ ] Catalog and review the final 2008–2025 selection.
- [ ] Download and verify archives before extraction.
- [ ] Extract and reconcile all selected PDFs.
- [ ] Run native extraction, then selective OCR, then normalization.
- [ ] Monitor stage counts, throughput, disk use, errors, and review queues.
- [ ] Retry transient failures without repeatedly retrying deterministic source
  defects.
- [ ] Run final full-checksum verification and export coverage/quality reports.
- [ ] Preserve dataset citation, configuration snapshot, code revision, logs,
  database backup, and transformation versions with the research output.

### Final acceptance gate

- All 13,884 selected documents are accounted for as successful or as explicit,
  reviewed exceptions.
- No document remains silently pending or stale-running.
- Every successful NLP document has a verified source PDF, page JSONL, document
  TXT, hashes, provenance, and processing versions.
- Counts reconcile by year, ticker, archive, stage, and extraction method.
- Recovery and reproduction instructions are sufficient for another operator.

## 16. Deferred backlog

These items must not delay the fixed historical milestone:

- live HSX, HNX, and UPCoM crawling;
- post-2025 incremental updates;
- automated company-identity resolution across ticker changes;
- English translation;
- table reconstruction into structured financial statements;
- cloud/object-storage backends;
- distributed multi-machine processing; and
- a public API or user interface.

Each deferred capability needs its own source, legal, schema, reliability, and
acceptance review before implementation.

## 17. Plan maintenance rule

Whenever implementation work changes, update in the same commit:

1. the phase status table in this file;
2. the relevant checkboxes and verification gate;
3. the architecture document if behavior or data contracts changed;
4. `IMPLEMENTATION_PLAN.md` if the project-level status changed; and
5. README instructions if an operator-visible command or prerequisite changed.

Do not mark a phase complete based only on code being written. Its verification
gate must pass, and any known exception must be recorded.
