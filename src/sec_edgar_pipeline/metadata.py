from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sec_edgar_pipeline.models import (
    ArtifactKind,
    DiscoverySelection,
    DiscoveredFiling,
    FilingDownloadResult,
    RunStatus,
    Status,
)


SCHEMA_VERSION = 1


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


class MetadataRepository:
    def __init__(self, database_path: Path) -> None:
        database_path.parent.mkdir(parents=True, exist_ok=True)
        self.database_path = database_path
        self.connection = sqlite3.connect(database_path, timeout=30)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.connection.execute("PRAGMA journal_mode = WAL")
        self.connection.execute("PRAGMA synchronous = NORMAL")
        self.connection.execute("PRAGMA busy_timeout = 30000")

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> MetadataRepository:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def initialize(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS companies (
                cik TEXT PRIMARY KEY,
                company_name TEXT NOT NULL,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS filings (
                accession_number TEXT PRIMARY KEY,
                cik TEXT NOT NULL REFERENCES companies(cik),
                company_name TEXT NOT NULL,
                base_form TEXT NOT NULL,
                filing_type TEXT NOT NULL,
                filing_date TEXT NOT NULL,
                report_date TEXT,
                filing_url TEXT NOT NULL,
                submission_txt_url TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'PENDING',
                retry_count INTEGER NOT NULL DEFAULT 0,
                downloaded_at TEXT,
                discovered_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_error TEXT
            );

            CREATE TABLE IF NOT EXISTS artifacts (
                accession_number TEXT NOT NULL REFERENCES filings(accession_number) ON DELETE CASCADE,
                kind TEXT NOT NULL,
                source_filename TEXT,
                url TEXT NOT NULL,
                local_path TEXT,
                file_size INTEGER,
                checksum TEXT,
                status TEXT NOT NULL DEFAULT 'PENDING',
                retry_count INTEGER NOT NULL DEFAULT 0,
                downloaded_at TEXT,
                updated_at TEXT NOT NULL,
                last_error TEXT,
                PRIMARY KEY (accession_number, kind)
            );

            CREATE TABLE IF NOT EXISTS runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                base_form TEXT NOT NULL,
                start_year INTEGER NOT NULL,
                end_year INTEGER NOT NULL,
                cik_mode TEXT NOT NULL,
                selection_fingerprint TEXT NOT NULL,
                config_json TEXT NOT NULL,
                batch_size INTEGER NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                started_at TEXT,
                completed_at TEXT,
                last_error TEXT
            );

            CREATE TABLE IF NOT EXISTS run_targets (
                run_id INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
                cik TEXT NOT NULL,
                PRIMARY KEY (run_id, cik)
            );

            CREATE TABLE IF NOT EXISTS run_filings (
                run_id INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
                accession_number TEXT NOT NULL REFERENCES filings(accession_number),
                ordinal INTEGER NOT NULL,
                batch_number INTEGER NOT NULL,
                PRIMARY KEY (run_id, accession_number),
                UNIQUE (run_id, ordinal)
            );

            CREATE INDEX IF NOT EXISTS idx_filings_cik ON filings(cik);
            CREATE INDEX IF NOT EXISTS idx_filings_base_form ON filings(base_form);
            CREATE INDEX IF NOT EXISTS idx_filings_type ON filings(filing_type);
            CREATE INDEX IF NOT EXISTS idx_filings_date ON filings(filing_date);
            CREATE INDEX IF NOT EXISTS idx_filings_report_date ON filings(report_date);
            CREATE INDEX IF NOT EXISTS idx_filings_status ON filings(status);
            CREATE INDEX IF NOT EXISTS idx_artifacts_status ON artifacts(status);
            CREATE INDEX IF NOT EXISTS idx_runs_fingerprint ON runs(selection_fingerprint, status);
            CREATE INDEX IF NOT EXISTS idx_run_filings_batch ON run_filings(run_id, batch_number, ordinal);
            """
        )
        row = self.connection.execute("SELECT version FROM schema_version").fetchone()
        if row is None:
            self.connection.execute(
                "INSERT INTO schema_version(version) VALUES (?)", (SCHEMA_VERSION,)
            )
        elif row["version"] != SCHEMA_VERSION:
            raise RuntimeError(
                f"Unsupported metadata schema {row['version']}; expected {SCHEMA_VERSION}"
            )
        self.connection.commit()

    def create_run(
        self,
        selection: DiscoverySelection,
        batch_size: int,
        config_snapshot: dict[str, Any],
    ) -> int:
        now = utc_now()
        cursor = self.connection.execute(
            """
            INSERT INTO runs(
                base_form, start_year, end_year, cik_mode, selection_fingerprint,
                config_json, batch_size, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                selection.base_form,
                selection.start_year,
                selection.end_year,
                selection.cik_mode,
                selection.fingerprint,
                json.dumps(config_snapshot, sort_keys=True, default=str),
                batch_size,
                RunStatus.DISCOVERING,
                now,
            ),
        )
        run_id = int(cursor.lastrowid)
        self.add_run_targets(run_id, selection.target_ciks)
        self.connection.commit()
        return run_id

    def find_resumable_run(self, fingerprint: str) -> sqlite3.Row | None:
        return self.connection.execute(
            """
            SELECT * FROM runs
            WHERE selection_fingerprint = ?
              AND status IN ('DISCOVERING', 'PENDING', 'RUNNING', 'PARTIAL', 'INTERRUPTED')
            ORDER BY id DESC LIMIT 1
            """,
            (fingerprint,),
        ).fetchone()

    def get_run(self, run_id: int) -> sqlite3.Row:
        row = self.connection.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
        if row is None:
            raise ValueError(f"Run does not exist: {run_id}")
        return row

    def add_run_targets(self, run_id: int, ciks: Iterable[str]) -> None:
        self.connection.executemany(
            "INSERT OR IGNORE INTO run_targets(run_id, cik) VALUES (?, ?)",
            ((run_id, cik) for cik in ciks),
        )

    def add_discovered_filing(
        self,
        run_id: int,
        filing: DiscoveredFiling,
        ordinal: int,
        batch_size: int,
    ) -> bool:
        now = utc_now()
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO companies(cik, company_name, first_seen_at, last_seen_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(cik) DO UPDATE SET
                    company_name = excluded.company_name,
                    last_seen_at = excluded.last_seen_at
                """,
                (filing.cik, filing.company_name, now, now),
            )
            self.connection.execute(
                """
                INSERT INTO filings(
                    accession_number, cik, company_name, base_form, filing_type,
                    filing_date, report_date, filing_url, submission_txt_url,
                    status, discovered_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(accession_number) DO UPDATE SET
                    company_name = excluded.company_name,
                    report_date = COALESCE(filings.report_date, excluded.report_date),
                    filing_url = excluded.filing_url,
                    submission_txt_url = excluded.submission_txt_url,
                    updated_at = excluded.updated_at
                """,
                (
                    filing.accession_number,
                    filing.cik,
                    filing.company_name,
                    filing.base_form,
                    filing.filing_type,
                    filing.filing_date,
                    filing.report_date,
                    filing.filing_url,
                    filing.submission_txt_url,
                    Status.PENDING,
                    now,
                    now,
                ),
            )
            cursor = self.connection.execute(
                """
                INSERT OR IGNORE INTO run_filings(
                    run_id, accession_number, ordinal, batch_number
                ) VALUES (?, ?, ?, ?)
                """,
                (run_id, filing.accession_number, ordinal, ordinal // batch_size),
            )
        return cursor.rowcount == 1

    def complete_discovery(self, run_id: int) -> None:
        self.connection.execute(
            "UPDATE runs SET status = ?, last_error = NULL WHERE id = ?",
            (RunStatus.PENDING, run_id),
        )
        self.connection.commit()

    def set_run_status(
        self,
        run_id: int,
        status: RunStatus,
        error: str | None = None,
    ) -> None:
        now = utc_now()
        started_at = now if status == RunStatus.RUNNING else None
        completed_at = now if status in {RunStatus.SUCCESS, RunStatus.PARTIAL, RunStatus.FAILED} else None
        self.connection.execute(
            """
            UPDATE runs SET
                status = ?,
                started_at = COALESCE(started_at, ?),
                completed_at = ?,
                last_error = ?
            WHERE id = ?
            """,
            (status, started_at, completed_at, error, run_id),
        )
        self.connection.commit()

    def recover_stale_running(self, run_id: int) -> int:
        now = utc_now()
        with self.connection:
            artifacts = self.connection.execute(
                """
                UPDATE artifacts SET status = ?, updated_at = ?
                WHERE status = ? AND accession_number IN (
                    SELECT accession_number FROM run_filings WHERE run_id = ?
                )
                """,
                (Status.PENDING, now, Status.RUNNING, run_id),
            ).rowcount
            filings = self.connection.execute(
                """
                UPDATE filings SET status = ?, updated_at = ?
                WHERE status = ? AND accession_number IN (
                    SELECT accession_number FROM run_filings WHERE run_id = ?
                )
                """,
                (Status.PENDING, now, Status.RUNNING, run_id),
            ).rowcount
        return artifacts + filings

    def batch_numbers(self, run_id: int) -> list[int]:
        rows = self.connection.execute(
            "SELECT DISTINCT batch_number FROM run_filings WHERE run_id = ? ORDER BY batch_number",
            (run_id,),
        ).fetchall()
        return [int(row["batch_number"]) for row in rows]

    def download_items(
        self,
        run_id: int,
        batch_number: int,
        failed_only: bool = False,
    ) -> list[dict[str, Any]]:
        status_clause = (
            "AND f.status = 'FAILED'"
            if failed_only
            else "AND f.status IN ('PENDING', 'RUNNING')"
        )
        rows = self.connection.execute(
            f"""
            SELECT
                f.*,
                rf.ordinal,
                rf.batch_number,
                txt.status AS txt_status,
                txt.local_path AS txt_local_path,
                txt.file_size AS txt_file_size,
                txt.checksum AS txt_checksum,
                txt.retry_count AS txt_retry_count,
                html.status AS html_status,
                html.local_path AS html_local_path,
                html.file_size AS html_file_size,
                html.checksum AS html_checksum,
                html.retry_count AS html_retry_count,
                html.source_filename AS html_source_filename,
                html.url AS html_url
            FROM run_filings rf
            JOIN filings f ON f.accession_number = rf.accession_number
            LEFT JOIN artifacts txt
                ON txt.accession_number = f.accession_number AND txt.kind = 'TXT'
            LEFT JOIN artifacts html
                ON html.accession_number = f.accession_number AND html.kind = 'HTML'
            WHERE rf.run_id = ? AND rf.batch_number = ? {status_clause}
            ORDER BY rf.ordinal
            """,
            (run_id, batch_number),
        ).fetchall()
        return [dict(row) for row in rows]

    def mark_running(self, accession_numbers: Sequence[str]) -> None:
        if not accession_numbers:
            return
        now = utc_now()
        with self.connection:
            self.connection.executemany(
                """
                UPDATE filings SET status = ?, updated_at = ?, last_error = NULL
                WHERE accession_number = ? AND status != ?
                """,
                (
                    (Status.RUNNING, now, accession_number, Status.SUCCESS)
                    for accession_number in accession_numbers
                ),
            )

    def save_download_result(self, result: FilingDownloadResult) -> None:
        now = utc_now()
        artifact_statuses = []
        retry_count = 0
        errors: list[str] = []
        with self.connection:
            for artifact in result.artifacts:
                artifact_statuses.append(artifact.status)
                retry_count += artifact.retry_count
                if artifact.error:
                    errors.append(f"{artifact.kind}: {artifact.error}")
                self.connection.execute(
                    """
                    INSERT INTO artifacts(
                        accession_number, kind, source_filename, url, local_path,
                        file_size, checksum, status, retry_count, downloaded_at,
                        updated_at, last_error
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(accession_number, kind) DO UPDATE SET
                        source_filename = excluded.source_filename,
                        url = excluded.url,
                        local_path = excluded.local_path,
                        file_size = excluded.file_size,
                        checksum = excluded.checksum,
                        status = excluded.status,
                        retry_count = artifacts.retry_count + excluded.retry_count,
                        downloaded_at = excluded.downloaded_at,
                        updated_at = excluded.updated_at,
                        last_error = excluded.last_error
                    """,
                    (
                        result.accession_number,
                        artifact.kind,
                        artifact.source_filename,
                        artifact.url,
                        artifact.local_path,
                        artifact.file_size,
                        artifact.checksum,
                        artifact.status,
                        artifact.retry_count,
                        now if artifact.status == Status.SUCCESS else None,
                        now,
                        artifact.error,
                    ),
                )

            filing_status = (
                Status.SUCCESS
                if len(artifact_statuses) == 2
                and all(status == Status.SUCCESS for status in artifact_statuses)
                else Status.FAILED
            )
            if result.error:
                errors.append(result.error)
            self.connection.execute(
                """
                UPDATE filings SET
                    report_date = COALESCE(?, report_date),
                    status = ?,
                    retry_count = retry_count + ?,
                    downloaded_at = ?,
                    updated_at = ?,
                    last_error = ?
                WHERE accession_number = ?
                """,
                (
                    result.report_date,
                    filing_status,
                    retry_count,
                    now if filing_status == Status.SUCCESS else None,
                    now,
                    "; ".join(errors) or None,
                    result.accession_number,
                ),
            )

    def finish_run_from_counts(self, run_id: int) -> RunStatus:
        counts = self.connection.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN f.status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful
            FROM run_filings rf
            JOIN filings f ON f.accession_number = rf.accession_number
            WHERE rf.run_id = ?
            """,
            (run_id,),
        ).fetchone()
        total = int(counts["total"] or 0)
        successful = int(counts["successful"] or 0)
        status = RunStatus.SUCCESS if total == successful else RunStatus.PARTIAL
        self.set_run_status(run_id, status)
        return status

    def reset_failed(self, run_id: int) -> int:
        now = utc_now()
        with self.connection:
            artifact_count = self.connection.execute(
                """
                UPDATE artifacts SET status = ?, updated_at = ?, last_error = NULL
                WHERE status = ? AND accession_number IN (
                    SELECT accession_number FROM run_filings WHERE run_id = ?
                )
                """,
                (Status.PENDING, now, Status.FAILED, run_id),
            ).rowcount
            filing_count = self.connection.execute(
                """
                UPDATE filings SET status = ?, updated_at = ?, last_error = NULL
                WHERE status = ? AND accession_number IN (
                    SELECT accession_number FROM run_filings WHERE run_id = ?
                )
                """,
                (Status.PENDING, now, Status.FAILED, run_id),
            ).rowcount
            self.connection.execute(
                "UPDATE runs SET status = ?, completed_at = NULL, last_error = NULL WHERE id = ?",
                (RunStatus.PENDING, run_id),
            )
        return max(artifact_count, filing_count)

    def run_summary(self, run_id: int) -> dict[str, Any]:
        run = dict(self.get_run(run_id))
        counts = self.connection.execute(
            """
            SELECT
                COUNT(DISTINCT f.accession_number) AS discovered,
                COUNT(DISTINCT f.cik) AS companies_with_filings,
                COUNT(DISTINCT CASE WHEN f.status = 'PENDING' THEN f.accession_number END) AS pending,
                COUNT(DISTINCT CASE WHEN f.status = 'RUNNING' THEN f.accession_number END) AS running,
                COUNT(DISTINCT CASE WHEN f.status = 'SUCCESS' THEN f.accession_number END) AS successful,
                COUNT(DISTINCT CASE WHEN f.status = 'FAILED' THEN f.accession_number END) AS failed,
                COALESCE(SUM(a.file_size), 0) AS bytes
            FROM run_filings rf
            JOIN filings f ON f.accession_number = rf.accession_number
            LEFT JOIN artifacts a ON a.accession_number = f.accession_number
            WHERE rf.run_id = ?
            """,
            (run_id,),
        ).fetchone()
        targets = self.connection.execute(
            "SELECT COUNT(*) AS count FROM run_targets WHERE run_id = ?", (run_id,)
        ).fetchone()
        run.update(dict(counts))
        run["target_companies"] = int(targets["count"])
        return run

    def latest_run_id(self) -> int:
        row = self.connection.execute("SELECT id FROM runs ORDER BY id DESC LIMIT 1").fetchone()
        if row is None:
            raise ValueError("No ingestion runs exist")
        return int(row["id"])

    def detailed_stats(self, run_id: int) -> dict[str, Any]:
        def rows(query: str) -> list[dict[str, Any]]:
            return [dict(row) for row in self.connection.execute(query, (run_id,)).fetchall()]

        return {
            "summary": self.run_summary(run_id),
            "by_status": rows(
                """
                SELECT f.status, COUNT(*) AS filings
                FROM run_filings rf
                JOIN filings f ON f.accession_number = rf.accession_number
                WHERE rf.run_id = ? GROUP BY f.status ORDER BY f.status
                """
            ),
            "by_form": rows(
                """
                SELECT f.filing_type, COUNT(*) AS filings
                FROM run_filings rf
                JOIN filings f ON f.accession_number = rf.accession_number
                WHERE rf.run_id = ? GROUP BY f.filing_type ORDER BY f.filing_type
                """
            ),
            "by_year": rows(
                """
                SELECT substr(f.filing_date, 1, 4) AS year, COUNT(*) AS filings,
                       SUM(CASE WHEN f.status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful
                FROM run_filings rf
                JOIN filings f ON f.accession_number = rf.accession_number
                WHERE rf.run_id = ? GROUP BY year ORDER BY year
                """
            ),
            "by_batch": rows(
                """
                SELECT rf.batch_number, COUNT(*) AS filings,
                       SUM(CASE WHEN f.status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful,
                       SUM(CASE WHEN f.status = 'FAILED' THEN 1 ELSE 0 END) AS failed
                FROM run_filings rf
                JOIN filings f ON f.accession_number = rf.accession_number
                WHERE rf.run_id = ? GROUP BY rf.batch_number ORDER BY rf.batch_number
                """
            ),
        }

    def verification_rows(self, run_id: int) -> list[sqlite3.Row]:
        return self.connection.execute(
            """
            SELECT
                f.accession_number, f.cik, f.base_form, f.filing_type,
                f.filing_date, f.status AS filing_status, rf.batch_number,
                a.kind, a.local_path, a.file_size, a.checksum,
                a.status AS artifact_status
            FROM run_filings rf
            JOIN filings f ON f.accession_number = rf.accession_number
            LEFT JOIN artifacts a ON a.accession_number = f.accession_number
            WHERE rf.run_id = ?
            ORDER BY rf.ordinal, a.kind
            """,
            (run_id,),
        ).fetchall()

    def target_ciks_without_filings(self, run_id: int) -> list[str]:
        rows = self.connection.execute(
            """
            SELECT rt.cik
            FROM run_targets rt
            WHERE rt.run_id = ?
              AND NOT EXISTS (
                  SELECT 1
                  FROM run_filings rf
                  JOIN filings f ON f.accession_number = rf.accession_number
                  WHERE rf.run_id = rt.run_id AND f.cik = rt.cik
              )
            ORDER BY rt.cik
            """,
            (run_id,),
        ).fetchall()
        return [str(row["cik"]) for row in rows]

    def mark_verification_failures(
        self,
        run_id: int,
        accessions: Iterable[str],
        reason: str,
    ) -> int:
        unique_accessions = sorted(set(accessions))
        if not unique_accessions:
            return 0
        now = utc_now()
        with self.connection:
            self.connection.executemany(
                """
                UPDATE filings SET status = ?, updated_at = ?, last_error = ?
                WHERE accession_number = ?
                """,
                ((Status.FAILED, now, reason, accession) for accession in unique_accessions),
            )
            self.connection.execute(
                """
                UPDATE runs SET status = ?, completed_at = ?, last_error = ?
                WHERE id = ?
                """,
                (RunStatus.PARTIAL, now, reason, run_id),
            )
        return len(unique_accessions)
