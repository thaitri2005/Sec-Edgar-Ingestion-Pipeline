from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath

from sec_edgar_pipeline.metadata import MetadataRepository
from sec_edgar_pipeline.models import ArtifactKind, Status
from sec_edgar_pipeline.storage import StorageBackend
from sec_edgar_pipeline.validator import validate_artifact


@dataclass(frozen=True, slots=True)
class VerificationIssue:
    accession_number: str
    kind: str | None
    reason: str


@dataclass(frozen=True, slots=True)
class VerificationReport:
    run_id: int
    expected_filings: int
    valid_filings: int
    issues: tuple[VerificationIssue, ...]
    target_ciks_without_filings: tuple[str, ...]
    marked_failed: int = 0

    @property
    def valid(self) -> bool:
        return not self.issues and self.expected_filings == self.valid_filings


def verify_run(
    repository: MetadataRepository,
    storage: StorageBackend,
    run_id: int,
    full_checksum: bool = False,
    mark_failed: bool = False,
) -> VerificationReport:
    rows = repository.verification_rows(run_id)
    by_accession: dict[str, list[dict[str, object]]] = {}
    for row in rows:
        by_accession.setdefault(str(row["accession_number"]), []).append(dict(row))

    issues: list[VerificationIssue] = []
    valid_filings = 0
    failed_accessions: set[str] = set()

    for accession, artifact_rows in by_accession.items():
        filing_issues: list[VerificationIssue] = []
        artifact_kinds = {str(row["kind"]) for row in artifact_rows if row["kind"]}
        if artifact_kinds != {ArtifactKind.TXT, ArtifactKind.HTML}:
            filing_issues.append(
                VerificationIssue(accession, None, "Expected both TXT and HTML metadata records")
            )

        for row in artifact_rows:
            if not row["kind"]:
                continue
            kind = ArtifactKind(str(row["kind"]))
            if kind == ArtifactKind.HTML and row["artifact_status"] == Status.NOT_AVAILABLE:
                if row["local_path"] or row["file_size"] is not None or row["checksum"]:
                    filing_issues.append(
                        VerificationIssue(
                            accession,
                            kind,
                            "Unavailable HTML artifact unexpectedly has local file metadata",
                        )
                    )
                continue
            if row["artifact_status"] != Status.SUCCESS:
                filing_issues.append(
                    VerificationIssue(accession, kind, f"Artifact status is {row['artifact_status']}")
                )
                continue
            if not row["local_path"]:
                filing_issues.append(VerificationIssue(accession, kind, "Missing local path"))
                continue
            path = PurePosixPath(str(row["local_path"]))
            validation = validate_artifact(storage, path, kind, accession)
            if not validation.valid:
                filing_issues.append(VerificationIssue(accession, kind, validation.error or "Invalid file"))
                continue
            actual_size = storage.size(path)
            if row["file_size"] is None or actual_size != int(row["file_size"]):
                filing_issues.append(VerificationIssue(accession, kind, "File size differs from metadata"))
                continue
            if full_checksum and row["checksum"]:
                actual_checksum = storage.checksum(path, "sha256")
                if actual_checksum != row["checksum"]:
                    filing_issues.append(VerificationIssue(accession, kind, "Checksum mismatch"))

        filing_status = artifact_rows[0]["filing_status"] if artifact_rows else None
        if filing_status != Status.SUCCESS:
            filing_issues.append(
                VerificationIssue(accession, None, f"Filing status is {filing_status}")
            )
        if filing_issues:
            issues.extend(filing_issues)
            failed_accessions.add(accession)
        else:
            valid_filings += 1

    marked = 0
    if mark_failed and failed_accessions:
        marked = repository.mark_verification_failures(
            run_id,
            failed_accessions,
            "Verification detected missing or invalid artifacts",
        )

    return VerificationReport(
        run_id=run_id,
        expected_filings=len(by_accession),
        valid_filings=valid_filings,
        issues=tuple(issues),
        target_ciks_without_filings=tuple(repository.target_ciks_without_filings(run_id)),
        marked_failed=marked,
    )
