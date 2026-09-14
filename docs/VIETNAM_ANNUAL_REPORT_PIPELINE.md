# Vietnam Annual Report NLP Pipeline — Design and Operations Plan

> **Status:** Initial pipeline implemented and metadata catalog validated on 2026-09-13. The controlled 2006–2010 archive pilot is initialized and paused at a resumable handoff; the remaining production archives have not started.

This pipeline will acquire and prepare Vietnamese listed-company annual reports for reproducible NLP research. It is deliberately separate from the implemented SEC EDGAR pipeline because Vietnamese annual reports use tickers and source-record identifiers rather than CIKs and accession numbers, and because their primary archival format is PDF rather than TXT/HTML.

The initial research scope is fixed to report years **2008–2025**. Live HSX/HNX collection is out of scope for the first implementation.

For the implemented SEC workflow, see [PIPELINE_WORKFLOW.md](PIPELINE_WORKFLOW.md).

## 1. Objective and outputs

The pipeline will preserve original PDFs and create reproducible, NLP-ready text while keeping enough provenance to trace every derived page back to its source document.

For each selected report, the expected outputs are:

1. The original annual-report PDF, preserved unchanged.
2. Page-level raw extracted text in JSON Lines format.
3. A document-level UTF-8 TXT file for ordinary NLP workflows.
4. Extraction and quality metadata in SQLite.
5. Checksums for both source and derived artifacts.

The pipeline will not extract standardized accounting values in its first version. The separate `vnfinancialdata` dataset may later be joined by ticker and report year, but it is not the source of the annual-report PDFs.

## 2. Fixed source and scope

The historical source is:

- **Dataset:** Vietnam Listed Companies Annual Reports PDF Dataset, 2000–2025
- **Version:** 1.0.0
- **Zenodo record:** `20949551`
- **DOI:** `10.5281/zenodo.20949551`
- **Selected report years:** 2008–2025 inclusive
- **Expected selected reports:** 13,884
- **License:** CC BY 4.0; attribution and the dataset citation must be retained

The selected reports are contained in four source archives:

| Archive | Archive coverage | Selected coverage | Published size |
| --- | ---: | ---: | ---: |
| `vn_bctn_2006_2010.zip` | 2006–2010 | 2008–2010 | 4.1 GB |
| `vn_bctn_2011_2015.zip` | 2011–2015 | 2011–2015 | 18.7 GB |
| `vn_bctn_2016_2020.zip` | 2016–2020 | 2016–2020 | 51.1 GB |
| `vn_bctn_2021_2025.zip` | 2021–2025 | 2021–2025 | 60.8 GB |

The total download is approximately 134.7 GB. Reports from 2006–2007 are present in the first required archive but are excluded from run membership and NLP processing.

The source record also provides:

- `file_index_full.csv`, the master document index;
- `checksums_pdf_sha256.csv`, per-PDF SHA-256 values;
- `checksums_archives_sha256.csv`, archive SHA-256 values;
- `coverage_by_year.csv` and `coverage_by_period.csv`; and
- `needs_review.csv` and `data_dictionary.csv`.

These metadata files must be downloaded and versioned before any large archive is downloaded.

## 3. Important terms

| Term | Meaning |
| --- | --- |
| Report year | The business year described by the annual report. This controls selection and storage paths. |
| Publication date | The date the report was disclosed, when known. A 2025 report may be published in 2026. |
| Source record ID | The stable identifier supplied by the dataset index. |
| Document ID | Pipeline identifier derived from the source, source record ID, and expected checksum. |
| Source artifact | An immutable downloaded archive or annual-report PDF. |
| Derived artifact | Page JSONL, document TXT, OCR PDF, or normalized text generated from a source PDF. |
| Native extraction | Text read directly from the PDF text layer. |
| OCR extraction | Text recognized from page images when the native text layer is absent or unusable. |
| Extraction profile | Versioned tool, settings, and normalization rules used to create derived text. |

Report year and publication date must never be substituted for one another. This is necessary to prevent look-ahead bias in downstream financial research.

## 4. End-to-end flow

```text
Load and validate YAML configuration
    |
    v
Create VN_DATA directories and initialize a separate SQLite database
    |
    v
Download Zenodo metadata and checksum manifests
    |
    v
Validate dataset version and build the 2008–2025 selection
    |
    v
Create or resume a stable run in SQLite
    |
    v
Download the four required ZIP archives with resumable partial files
    |
    v
Verify archive checksums
    |
    v
Safely extract selected PDFs and verify every PDF checksum
    |
    v
Extract native text page by page
    |
    +--> Quality acceptable --> write raw page JSONL
    |
    `--> Quality inadequate --> OCR affected pages --> write raw page JSONL
                                      |
                                      v
                           Normalize document text
                                      |
                                      v
                     Write TXT and quality/provenance metadata
                                      |
                                      v
                        stats / verify / retry-failed
```

## 5. Application interface

The companion package lives under `src/vn_report_pipeline/` and exposes a separate `vn-reports` command. The commands are:

| Command | Purpose |
| --- | --- |
| `catalog` | Download and validate Zenodo index/checksum metadata and register selected documents. |
| `download --run-id N` | Download required archives and extract selected PDFs. |
| `extract --run-id N` | Perform native page-level text extraction. |
| `ocr --run-id N` | OCR only pages/documents that fail configured text-quality thresholds. |
| `normalize --run-id N` | Create reproducible normalized TXT from page-level raw text. |
| `stats --run-id N` | Report source, extraction, OCR, language, year, and quality counts. |
| `verify --run-id N` | Verify SQLite metadata, file presence, PDF validity, and optional checksums. |
| `retry-failed --run-id N --stage STAGE` | Retry failures only for download, extraction, OCR, or normalization. |
| `run` | Execute the enabled stages in order; intended only after the pilot is validated. |

Operational commands:

```powershell
vn-reports --config configs/vn_reports.yaml catalog `
  --start-year 2008 --end-year 2025

vn-reports --config configs/vn_reports.yaml stats --run-id 1
vn-reports --config configs/vn_reports.yaml download --run-id 1
vn-reports --config configs/vn_reports.yaml extract --run-id 1
vn-reports --config configs/vn_reports.yaml ocr --run-id 1
vn-reports --config configs/vn_reports.yaml normalize --run-id 1
vn-reports --config configs/vn_reports.yaml verify --run-id 1 --full-checksum
```

### Progress logging

Every long-running stage reports its pending-document total, the first completed
document, and another progress line after `logging.progress_every` documents or
30 seconds, whichever comes first. Progress lines include percentage, successful
and failed counts, documents per second, elapsed time, ETA, and the most recently
completed ticker, year, and document ID. Long native-extraction and OCR documents
also emit page-level heartbeats. Individual failures are logged immediately.

Console output is concise. Full structured context is written to
`VN_DATA/logs/pipeline.jsonl` and can be followed from another PowerShell window:

```powershell
Get-Content .\VN_DATA\logs\pipeline.jsonl -Tail 20 -Wait
```

## 6. Configuration

```yaml
storage:
  root_directory: ../VN_DATA

dataset:
  provider: zenodo
  record_id: "20949551"
  dataset_version: "1.0.0"
  start_year: 2008
  end_year: 2025

download:
  max_workers: 2
  retry_limit: 5
  request_timeout_seconds: 120
  backoff_base_seconds: 2.0
  checksum_algorithm: sha256
  resume_partial_archives: true

extraction:
  max_workers: 4
  native_engine: pymupdf
  preserve_page_boundaries: true
  minimum_characters_per_page: 100
  minimum_document_text_coverage: 0.70

ocr:
  enabled: true
  engine: tesseract
  languages:
    - vie
    - eng
  mode: low_quality_pages_only

normalization:
  unicode_form: NFC
  remove_repeated_headers_footers: true
  repair_line_wrap_hyphenation: true
```

On the current workstation, the private configuration file is
`D:\Seed Grant Project\configs\vn_reports.yaml`. Because storage paths resolve
relative to the configuration file, `../VN_DATA` becomes
`D:\Seed Grant Project\VN_DATA`. This keeps both datasets under the same base
folder without mixing their databases or artifacts; do not relocate it to
`D:\VN_DATA` or a `C:` path.

Quality thresholds are starting values for the pilot, not permanent assumptions. They must be calibrated against manually reviewed Vietnamese reports before the full extraction run.

## 7. Discovery and run membership

`catalog` will use the Zenodo record metadata and `file_index_full.csv` rather than crawling stock-exchange pages.

It will:

1. Record the Zenodo record ID, DOI, dataset version, source URLs, and retrieval time.
2. Verify the downloaded metadata files against the hashes published by Zenodo.
3. Parse and validate source record IDs, tickers, report years, paths, sizes, and expected PDF checksums.
4. Select report years 2008–2025 inclusive.
5. Reject unsafe paths and malformed or duplicate source rows.
6. Create a stable selection fingerprint from provider, record ID, dataset version, and year range.
7. Insert documents in stable source-index order and assign operational batches.
8. Produce a coverage report before archive downloading begins.

Dataset index values are preserved exactly in provenance fields. Normalized ticker values may be added for queries, but they must not overwrite the original ticker fields.

Exchange membership is not assumed from the ticker alone. If HSX, HNX, or UPCoM membership is later enriched from another source, that source and effective dates must be stored separately because firms can transfer exchanges or change tickers.

## 8. Download and safe extraction

Archives are large, so downloads must stream to `.part` files and support HTTP range resume when the server permits it. Restarting the process must not discard a valid partial archive.

For each archive:

1. Reuse a complete file only after size and SHA-256 validation.
2. Resume a `.part` file only after confirming compatible server range behavior.
3. Apply bounded retries and exponential backoff to transient HTTP errors.
4. Flush and sync completed content before atomic rename.
5. Verify the archive checksum before extraction.
6. Reject ZIP entries with absolute paths, drive prefixes, or parent traversal.
7. Extract only files registered for the selected report years.
8. Validate each PDF signature, size, and expected SHA-256.
9. Atomically move each validated PDF to its final path.
10. Commit document and artifact status after every PDF.

Archive success and PDF success are separate states. A valid archive may contain a PDF that fails its individual checksum, and that problem must remain visible.

## 9. PDF text extraction for NLP

### Native extraction first

PyMuPDF should extract text independently for each page. The pipeline records page number, text, character count, extraction duration, and warnings. The raw extraction is immutable after it is committed under a versioned extraction profile.

Native extraction is accepted only after document-level and page-level quality checks. Useful signals include:

- non-whitespace character count;
- proportion of pages with meaningful text;
- replacement/control-character rate;
- repeated-glyph or obvious mojibake patterns;
- ratio of alphabetic text to drawing/image-only content; and
- Vietnamese/English language detection confidence.

### Selective OCR

OCR is expensive and must not run across the entire corpus by default. Pages with a usable native text layer are retained. Only empty or low-quality pages are rendered and OCRed using Vietnamese and English language models.

The first implementation should use OCRmyPDF/Tesseract when the host dependencies are available. The engine is kept behind an interface so a later benchmark can substitute PaddleOCR or another model without changing document identity or raw PDFs.

OCR output never replaces native raw extraction silently. Each page stores its method, engine version, language configuration, and quality metrics.

A page that remains blank or sparse after OCR is retained with its quality flag
and reported as a warning. It does not fail an otherwise usable document. Actual
OCR engine exceptions, corrupt inputs, or missing artifacts remain document-stage
failures. Native Tesseract diagnostics are suppressed at the console boundary;
the pipeline's structured progress, warning, and error records remain visible.

### Derived formats

Page JSONL is the canonical NLP extraction artifact:

```json
{"document_id":"zenodo-20949551-ACB-2024","ticker":"ACB","report_year":2024,"page":37,"text":"...","method":"native","language":"vi","character_count":2841,"profile":"native-v1"}
```

The document TXT is a convenience derivative created by joining pages with explicit markers:

```text
<<<PAGE 1>>>
...

<<<PAGE 2>>>
...
```

Keeping page boundaries permits page citations, targeted OCR repair, layout-aware processing, and manual validation.

## 10. Text normalization

Raw extracted/OCR text and normalized text are different artifacts. Normalization may:

- apply Unicode NFC;
- standardize line endings;
- remove repeated page headers and footers only when confidently detected;
- repair line-wrap hyphenation using documented rules;
- preserve paragraph and page boundaries;
- remove illegal control characters; and
- record language classification without translating text.

Normalization must not silently:

- replace missing observations with zero;
- translate Vietnamese to English;
- discard tables or financial-statement sections;
- merge different annual reports; or
- overwrite source PDFs or raw extraction.

Every normalization revision receives a new profile identifier so an analysis can reproduce the exact text preparation used.

## 11. Storage layout

```text
D:\Seed Grant Project\VN_DATA\
|-- source_metadata/
|   `-- zenodo/20949551/1.0.0/
|       |-- file_index_full.csv
|       |-- checksums_pdf_sha256.csv
|       |-- checksums_archives_sha256.csv
|       |-- coverage_by_year.csv
|       |-- coverage_by_period.csv
|       |-- needs_review.csv
|       `-- data_dictionary.csv
|-- archives/
|   `-- zenodo/20949551/1.0.0/{archive}.zip
|-- raw/
|   `-- annual-reports/{ticker}/{report_year}/{document_id}.pdf
|-- processed/
|   |-- pages/{profile}/{ticker}/{report_year}/{document_id}.jsonl
|   |-- text/{profile}/{ticker}/{report_year}/{document_id}.txt
|   `-- ocr-pdf/{profile}/{ticker}/{report_year}/{document_id}.pdf
|-- metadata/
|   `-- metadata.db
|-- logs/
|   `-- pipeline.jsonl
`-- tmp/
```

Source record IDs and checksums control identity; filenames and tickers alone do not. Batch numbers never affect final paths.

## 12. SQLite source of truth

The Vietnamese pipeline uses the separate database
`D:\Seed Grant Project\VN_DATA\metadata\metadata.db`. Sharing the SEC database
would create ambiguous identifiers and lifecycle rules.

| Table | Purpose |
| --- | --- |
| `schema_version` | Version of the Vietnamese metadata schema. |
| `datasets` | Provider, record ID, DOI, version, license, retrieval time, and citation. |
| `companies` | Normalized ticker plus source ticker and optional identity enrichment. |
| `documents` | One annual report per source record, with report year and source provenance. |
| `artifacts` | Archive, PDF, page JSONL, TXT, and OCR-PDF metadata and checksums. |
| `extraction_profiles` | Versioned engines, tool versions, thresholds, and normalization settings. |
| `page_quality` | Per-page extraction method, counts, language, quality flags, and errors. |
| `runs` | Dataset selection, configuration snapshot, lifecycle state, and timestamps. |
| `run_documents` | Stable run membership, ordinal, and batch number. |

Document stages are tracked independently:

```text
catalog_status      PENDING -> SUCCESS / FAILED
download_status     PENDING -> RUNNING -> SUCCESS / FAILED
extraction_status   PENDING -> RUNNING -> SUCCESS / NEEDS_OCR / FAILED
ocr_status          NOT_REQUIRED / PENDING -> RUNNING -> SUCCESS / FAILED
normalize_status    PENDING -> RUNNING -> SUCCESS / FAILED
```

A run is fully successful only when every selected document has a valid source PDF, page JSONL, and normalized TXT. OCR success is required only for documents/pages classified as needing OCR.

## 13. Verification and quality reporting

Verification has separate levels:

1. **Catalog:** selected count, duplicate keys, safe paths, and complete expected checksums.
2. **Archive:** file presence, expected size, ZIP readability, and SHA-256.
3. **PDF:** PDF signature, readability, page count, size, and SHA-256.
4. **Extraction:** valid JSONL, unique ordered pages, page-count agreement, and UTF-8 text.
5. **OCR:** every `NEEDS_OCR` page resolved or explicitly failed.
6. **Normalization:** TXT provenance matches the expected extraction and normalization profiles.

The final quality report should include counts by report year and ticker, plus:

- native-only documents;
- documents receiving partial or full OCR;
- empty/low-text pages;
- Vietnamese, English, mixed, and unknown language counts;
- median characters and pages per document;
- duplicate PDF checksums;
- checksum failures;
- unresolved extraction/OCR failures; and
- documents listed by the source as needing review.

Derived-text quality cannot be established by file presence alone. The pilot must include manual review of a stratified sample covering early scans, modern digital PDFs, Vietnamese reports, English reports, text-heavy documents, and graphics-heavy documents.

## 14. Interruption and recovery

The recovery behavior should mirror the SEC pipeline while remaining stage-aware:

- Ctrl+C stops scheduling new work and lets active atomic operations settle.
- Stale `RUNNING` stage rows return to `PENDING` on restart.
- Valid archives, PDFs, page JSONL, and TXT artifacts are reused.
- A failed derived artifact never invalidates an already verified source PDF.
- `retry-failed --stage extraction` does not redownload PDFs.
- `retry-failed --stage download` does not erase valid derived artifacts for other documents.
- Changing an extraction profile creates new derived artifacts instead of overwriting an older reproducible corpus.

## 15. Recommended pilot

Do not begin with all 134.7 GB. The pilot should:

1. Download all small metadata and checksum files.
2. Register the complete 2008–2025 selection and confirm the expected 13,884 records.
3. Select approximately 100 reports across 2008, 2012, 2016, 2020, and 2025.
4. Obtain those PDFs from one downloaded archive or a controlled sample source.
5. Run native extraction and manually inspect quality.
6. Calibrate `NEEDS_OCR` thresholds.
7. OCR a representative low-quality subset.
8. Verify page counts, checksums, Vietnamese characters, and TXT provenance.
9. Estimate full-corpus CPU time and derived storage.
10. Only then download and process all four archives.

Because Zenodo packages reports in multi-year ZIP archives, a pilot that needs arbitrary PDFs may still require downloading one complete archive. The 4.1 GB 2006–2010 archive is the lowest-risk end-to-end test; only its 2008–2010 members enter the selected run.

## 16. Production procedure

After the pilot passes:

1. Back up the pilot metadata database and record the accepted extraction profile.
2. Download the remaining required archives one at a time.
3. Verify each archive before extracting it.
4. Extract and verify selected PDFs before deleting any temporary data.
5. Run native extraction over all documents.
6. Review the quality report before scheduling OCR.
7. Run selective OCR only for flagged pages/documents.
8. Normalize text with a pinned profile.
9. Run full verification and retain the final statistics.
10. Record the dataset DOI/version, code commit, configuration, extraction profile, and OCR versions with every downstream study.

## 17. Explicitly deferred work

The following are not part of the initial 2008–2025 implementation:

- live HSX, HNX, or UPCoM discovery;
- collecting reports after report year 2025;
- standardized financial-statement value extraction;
- table reconstruction;
- translation;
- embedding generation or model-specific chunking;
- sentiment, ESG, readability, or topic-model features; and
- a combined SEC/Vietnam company identity database.

These are downstream or future-source concerns. The first milestone is a complete, verified, immutable PDF corpus with reproducible page-level Vietnamese/English text.

## 18. Attribution and research reproducibility

Retain the source license and cite the dataset in research outputs:

```text
Ngo, P. T. (2026). Vietnam Listed Companies Annual Reports PDF Dataset,
2000–2025 (Version 1.0.0) [Data set]. Zenodo.
https://doi.org/10.5281/zenodo.20949551
```

At minimum, each analysis should record:

- Zenodo record and DOI;
- dataset version;
- selected report years;
- document count;
- pipeline code commit;
- extraction and normalization profile IDs;
- OCR engine and language-model versions, when used; and
- the exact inclusion/exclusion rules applied after ingestion.
