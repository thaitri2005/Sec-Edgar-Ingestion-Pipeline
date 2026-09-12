from __future__ import annotations

import json
from dataclasses import dataclass

from vn_report_pipeline.metadata import MetadataRepository
from vn_report_pipeline.models import ArtifactKind, Stage
from vn_report_pipeline.storage import LocalStorage, validate_pdf


@dataclass(frozen=True, slots=True)
class VerificationIssue:
    document_id: str
    kind: str
    reason: str


def verify_run(
    repository: MetadataRepository,
    storage: LocalStorage,
    run_id: int,
    full_checksum: bool = False,
    mark_failed: bool = False,
) -> tuple[int, int, list[VerificationIssue]]:
    documents = {row["document_id"]: row for row in repository.documents(run_id)}
    artifacts = repository.artifacts_for_run(run_id)
    issues: list[VerificationIssue] = []
    bad_documents: set[str] = set()
    for artifact in artifacts:
        document_id = artifact["document_id"]
        kind = artifact["kind"]
        try:
            path = storage.resolve(artifact["local_path"])
            if not path.is_file():
                raise ValueError("file is missing")
            if path.stat().st_size != int(artifact["file_size"]):
                raise ValueError("file size differs from SQLite")
            if (
                full_checksum
                and storage.checksum(artifact["local_path"]) != artifact["checksum"]
            ):
                raise ValueError("SHA-256 differs from SQLite")
            if kind == ArtifactKind.PDF:
                validate_pdf(path, int(artifact["expected_size"]))
                if (
                    full_checksum
                    and storage.checksum(artifact["local_path"])
                    != artifact["expected_sha256"]
                ):
                    raise ValueError("PDF SHA-256 differs from source manifest")
            elif kind in {ArtifactKind.PAGES_NATIVE, ArtifactKind.PAGES_OCR}:
                count = 0
                with path.open("r", encoding="utf-8") as handle:
                    for count, line in enumerate(handle, 1):
                        payload = json.loads(line)
                        if int(payload.get("page_number", 0)) != count:
                            raise ValueError("page JSONL is not sequential")
                if count == 0:
                    raise ValueError("page JSONL is empty")
            elif kind == ArtifactKind.TEXT:
                text = path.read_text(encoding="utf-8")
                if not text.startswith("<<<PAGE 1>>>"):
                    raise ValueError("normalized TXT has no first-page marker")
        except Exception as error:
            issues.append(VerificationIssue(document_id, str(kind), str(error)))
            bad_documents.add(document_id)

    for document_id, row in documents.items():
        expected: list[str] = []
        if row["download_status"] == "SUCCESS":
            expected.append(ArtifactKind.PDF)
        if row["extraction_status"] in {"SUCCESS", "NEEDS_OCR"}:
            expected.append(ArtifactKind.PAGES_NATIVE)
        if row["ocr_status"] == "SUCCESS":
            expected.append(ArtifactKind.PAGES_OCR)
        if row["normalization_status"] == "SUCCESS":
            expected.append(ArtifactKind.TEXT)
        existing = {
            artifact["kind"]
            for artifact in artifacts
            if artifact["document_id"] == document_id
        }
        for kind in expected:
            if kind not in existing:
                issues.append(
                    VerificationIssue(
                        document_id, str(kind), "successful stage has no artifact"
                    )
                )
                bad_documents.add(document_id)

    if mark_failed:
        for issue in issues:
            repository.mark_document_stage_failed(
                run_id,
                issue.document_id,
                _stage_for_kind(issue.kind),
                f"Verification: {issue.reason}",
            )
    return len(documents) - len(bad_documents), len(documents), issues


def _stage_for_kind(kind: str) -> Stage:
    return {
        ArtifactKind.PDF: Stage.DOWNLOAD,
        ArtifactKind.PAGES_NATIVE: Stage.EXTRACTION,
        ArtifactKind.PAGES_OCR: Stage.OCR,
        ArtifactKind.TEXT: Stage.NORMALIZATION,
    }.get(kind, Stage.DOWNLOAD)
