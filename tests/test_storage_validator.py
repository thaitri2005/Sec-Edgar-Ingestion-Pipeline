from __future__ import annotations

from pathlib import PurePosixPath

from sec_edgar_pipeline.models import ArtifactKind
from sec_edgar_pipeline.storage import LocalStorageBackend
from sec_edgar_pipeline.validator import (
    extract_submission_metadata,
    normalize_cik,
    validate_artifact,
)


ACCESSION = "0000320193-23-000106"


def submission_txt() -> bytes:
    return f"""<SEC-DOCUMENT>{ACCESSION}.txt : 20231103
<SEC-HEADER>
ACCESSION NUMBER: {ACCESSION}
CONFORMED PERIOD OF REPORT: 20230930
</SEC-HEADER>
<DOCUMENT>
<TYPE>10-K
<SEQUENCE>1
<FILENAME>aapl-20230930.htm
<TEXT>
<html><body>Annual report</body></html>
</TEXT>
</DOCUMENT>
""".encode()


def test_storage_paths_and_submission_parsing(tmp_path) -> None:
    storage = LocalStorageBackend(tmp_path)
    storage.ensure_layout()
    path = storage.artifact_path("10-K", "0000320193", 2023, ACCESSION, ArtifactKind.TXT)
    stored = storage.write_atomic(path, (submission_txt(),), "sha256")

    assert path == PurePosixPath("raw/10-K/0000320193/2023/0000320193-23-000106.txt")
    assert stored.file_size > 0
    assert validate_artifact(storage, path, ArtifactKind.TXT, ACCESSION).valid
    primary = extract_submission_metadata(storage, path, "10-K")
    assert primary.filename == "aapl-20230930.htm"
    assert primary.report_date == "2023-09-30"
    assert primary.is_html


def test_validator_rejects_sec_block_page(tmp_path) -> None:
    storage = LocalStorageBackend(tmp_path)
    path = PurePosixPath("raw/10-K/0000320193/2023/example.htm")
    storage.write_atomic(
        path,
        (b"<html>Your request originates from an undeclared automated tool</html>",),
        "sha256",
    )

    result = validate_artifact(storage, path, ArtifactKind.HTML, ACCESSION)

    assert not result.valid
    assert "block" in result.error.lower()


def test_validator_rejects_wrapped_html(tmp_path) -> None:
    storage = LocalStorageBackend(tmp_path)
    path = PurePosixPath("raw/10-K/0000320193/2023/example.htm")
    storage.write_atomic(
        path,
        (b"<DOCUMENT><TEXT><html>Annual report</html></TEXT></DOCUMENT>",),
        "sha256",
    )

    result = validate_artifact(storage, path, ArtifactKind.HTML, ACCESSION)

    assert not result.valid
    assert "wrapper" in result.error.lower()


def test_validator_accepts_legacy_html_fragment(tmp_path) -> None:
    storage = LocalStorageBackend(tmp_path)
    path = PurePosixPath("raw/10-K/0000320193/2023/example.htm")
    storage.write_atomic(
        path,
        (b'<P STYLE="text-align:center">Legacy filing</P>',),
        "sha256",
    )

    result = validate_artifact(storage, path, ArtifactKind.HTML, ACCESSION)

    assert result.valid


def test_validator_accepts_html_after_leading_character_entity(tmp_path) -> None:
    storage = LocalStorageBackend(tmp_path)
    path = PurePosixPath("raw/10-K/0000320193/2023/example.htm")
    storage.write_atomic(
        path,
        (b"&#39;<HTML><BODY>Annual report</BODY></HTML>",),
        "sha256",
    )

    result = validate_artifact(storage, path, ArtifactKind.HTML, ACCESSION)

    assert result.valid


def test_normalize_cik() -> None:
    assert normalize_cik("320193") == "0000320193"

