from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from vn_report_pipeline.models import CatalogDocument, RunStatus, Stage, StageStatus


SCHEMA_VERSION = 2


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


class MetadataRepository:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.connection = sqlite3.connect(path, timeout=30)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.connection.execute("PRAGMA journal_mode = WAL")
        self.connection.execute("PRAGMA synchronous = NORMAL")
        self.connection.execute("PRAGMA busy_timeout = 30000")

    def __enter__(self) -> MetadataRepository:
        return self

    def __exit__(self, *_: object) -> None:
        self.connection.close()

    def initialize(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS schema_version(version INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS datasets(
                id INTEGER PRIMARY KEY,
                provider TEXT NOT NULL,
                record_id TEXT NOT NULL,
                version TEXT NOT NULL,
                doi TEXT,
                title TEXT NOT NULL,
                license TEXT,
                publication_date TEXT,
                metadata_json TEXT NOT NULL,
                retrieved_at TEXT NOT NULL,
                UNIQUE(provider, record_id, version)
            );
            CREATE TABLE IF NOT EXISTS source_files(
                dataset_id INTEGER NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
                name TEXT NOT NULL,
                kind TEXT NOT NULL,
                url TEXT NOT NULL,
                expected_size INTEGER NOT NULL,
                expected_md5 TEXT,
                expected_sha256 TEXT,
                local_path TEXT,
                file_size INTEGER,
                checksum TEXT,
                status TEXT NOT NULL DEFAULT 'PENDING',
                retry_count INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                last_error TEXT,
                PRIMARY KEY(dataset_id, name)
            );
            CREATE TABLE IF NOT EXISTS companies(
                ticker TEXT PRIMARY KEY,
                source_ticker TEXT NOT NULL,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS documents(
                document_id TEXT PRIMARY KEY,
                dataset_id INTEGER NOT NULL REFERENCES datasets(id),
                source_record_id TEXT NOT NULL,
                ticker TEXT NOT NULL REFERENCES companies(ticker),
                source_ticker TEXT NOT NULL,
                report_year INTEGER NOT NULL,
                archive_name TEXT NOT NULL,
                archive_member TEXT NOT NULL,
                source_filename TEXT NOT NULL,
                expected_size INTEGER NOT NULL,
                expected_sha256 TEXT NOT NULL,
                source_status TEXT NOT NULL,
                source_notes TEXT,
                download_status TEXT NOT NULL DEFAULT 'PENDING',
                extraction_status TEXT NOT NULL DEFAULT 'PENDING',
                ocr_status TEXT NOT NULL DEFAULT 'NOT_REQUIRED',
                normalization_status TEXT NOT NULL DEFAULT 'PENDING',
                page_count INTEGER,
                low_quality_pages INTEGER,
                updated_at TEXT NOT NULL,
                last_error TEXT,
                UNIQUE(dataset_id, source_record_id)
            );
            CREATE TABLE IF NOT EXISTS runs(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id INTEGER NOT NULL REFERENCES datasets(id),
                start_year INTEGER NOT NULL,
                end_year INTEGER NOT NULL,
                selection_fingerprint TEXT NOT NULL,
                config_json TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                started_at TEXT,
                completed_at TEXT,
                last_error TEXT
            );
            CREATE TABLE IF NOT EXISTS run_documents(
                run_id INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
                document_id TEXT NOT NULL REFERENCES documents(document_id),
                ordinal INTEGER NOT NULL,
                batch_number INTEGER NOT NULL,
                PRIMARY KEY(run_id, document_id),
                UNIQUE(run_id, ordinal)
            );
            CREATE TABLE IF NOT EXISTS artifacts(
                document_id TEXT NOT NULL REFERENCES documents(document_id) ON DELETE CASCADE,
                kind TEXT NOT NULL,
                profile TEXT NOT NULL DEFAULT '',
                local_path TEXT NOT NULL,
                file_size INTEGER NOT NULL,
                checksum TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_error TEXT,
                PRIMARY KEY(document_id, kind, profile)
            );
            CREATE TABLE IF NOT EXISTS page_quality(
                document_id TEXT NOT NULL REFERENCES documents(document_id) ON DELETE CASCADE,
                profile TEXT NOT NULL,
                page_number INTEGER NOT NULL,
                method TEXT NOT NULL,
                character_count INTEGER NOT NULL,
                printable_ratio REAL NOT NULL,
                replacement_ratio REAL NOT NULL,
                quality_score REAL NOT NULL,
                needs_ocr INTEGER NOT NULL,
                error TEXT,
                PRIMARY KEY(document_id, profile, page_number)
            );
            CREATE TABLE IF NOT EXISTS stage_executions(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
                document_id TEXT REFERENCES documents(document_id) ON DELETE CASCADE,
                stage TEXT NOT NULL,
                profile TEXT NOT NULL DEFAULT '',
                config_json TEXT NOT NULL DEFAULT '{}',
                status TEXT NOT NULL,
                attempt INTEGER NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                error TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_documents_year ON documents(report_year);
            CREATE INDEX IF NOT EXISTS idx_documents_archive ON documents(archive_name);
            CREATE INDEX IF NOT EXISTS idx_documents_download ON documents(download_status);
            CREATE INDEX IF NOT EXISTS idx_documents_extraction ON documents(extraction_status);
            CREATE INDEX IF NOT EXISTS idx_run_documents ON run_documents(run_id, batch_number, ordinal);
            CREATE INDEX IF NOT EXISTS idx_runs_fingerprint ON runs(selection_fingerprint, status);
            """
        )
        row = self.connection.execute("SELECT version FROM schema_version").fetchone()
        if row is None:
            self.connection.execute(
                "INSERT INTO schema_version VALUES (?)", (SCHEMA_VERSION,)
            )
        elif row["version"] == 1:
            self.connection.execute(
                "ALTER TABLE stage_executions ADD COLUMN config_json TEXT NOT NULL DEFAULT '{}'"
            )
            self.connection.execute(
                "UPDATE schema_version SET version=?", (SCHEMA_VERSION,)
            )
        elif row["version"] != SCHEMA_VERSION:
            raise RuntimeError(f"Unsupported Vietnam schema {row['version']}")
        self.connection.commit()

    def upsert_dataset(
        self, metadata: dict[str, Any], provider: str, record_id: str, version: str
    ) -> int:
        now = utc_now()
        values = (
            provider,
            record_id,
            version,
            str(metadata.get("doi", "")),
            str(metadata.get("title", "")),
            _license_id(metadata.get("license")),
            metadata.get("publication_date"),
            json.dumps(metadata, sort_keys=True, ensure_ascii=False),
            now,
        )
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO datasets(provider, record_id, version, doi, title, license,
                    publication_date, metadata_json, retrieved_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(provider, record_id, version) DO UPDATE SET
                    doi=excluded.doi, title=excluded.title, license=excluded.license,
                    publication_date=excluded.publication_date,
                    metadata_json=excluded.metadata_json, retrieved_at=excluded.retrieved_at
                """,
                values,
            )
        row = self.connection.execute(
            "SELECT id FROM datasets WHERE provider=? AND record_id=? AND version=?",
            (provider, record_id, version),
        ).fetchone()
        assert row is not None
        return int(row["id"])

    def upsert_source_file(
        self,
        dataset_id: int,
        name: str,
        kind: str,
        url: str,
        size: int,
        md5: str | None,
    ) -> None:
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO source_files(dataset_id,name,kind,url,expected_size,expected_md5,updated_at)
                VALUES(?,?,?,?,?,?,?)
                ON CONFLICT(dataset_id,name) DO UPDATE SET kind=excluded.kind,url=excluded.url,
                    expected_size=excluded.expected_size,expected_md5=excluded.expected_md5,
                    updated_at=excluded.updated_at
                """,
                (dataset_id, name, kind, url, size, md5, utc_now()),
            )

    def set_source_sha256(self, dataset_id: int, name: str, sha256: str) -> None:
        self.connection.execute(
            "UPDATE source_files SET expected_sha256=?,updated_at=? WHERE dataset_id=? AND name=?",
            (sha256, utc_now(), dataset_id, name),
        )
        self.connection.commit()

    def save_source_result(
        self,
        dataset_id: int,
        name: str,
        status: StageStatus,
        local_path: str | None,
        size: int | None,
        checksum: str | None,
        attempts: int = 0,
        error: str | None = None,
    ) -> None:
        self.connection.execute(
            """
            UPDATE source_files SET status=?,local_path=?,file_size=?,checksum=?,
                retry_count=retry_count+?,updated_at=?,last_error=?
            WHERE dataset_id=? AND name=?
            """,
            (
                status,
                local_path,
                size,
                checksum,
                attempts,
                utc_now(),
                error,
                dataset_id,
                name,
            ),
        )
        self.connection.commit()

    def create_or_resume_run(
        self,
        dataset_id: int,
        start_year: int,
        end_year: int,
        fingerprint: str,
        config: dict[str, Any],
        force_new: bool = False,
    ) -> tuple[int, bool]:
        if not force_new:
            row = self.connection.execute(
                """SELECT id FROM runs WHERE selection_fingerprint=? AND
                status IN ('CATALOGING','PENDING','RUNNING','PARTIAL','INTERRUPTED')
                ORDER BY id DESC LIMIT 1""",
                (fingerprint,),
            ).fetchone()
            if row is not None:
                return int(row["id"]), True
        cursor = self.connection.execute(
            """INSERT INTO runs(dataset_id,start_year,end_year,selection_fingerprint,
            config_json,status,created_at) VALUES(?,?,?,?,?,?,?)""",
            (
                dataset_id,
                start_year,
                end_year,
                fingerprint,
                json.dumps(config, sort_keys=True, default=str),
                RunStatus.CATALOGING,
                utc_now(),
            ),
        )
        self.connection.commit()
        return int(cursor.lastrowid), False

    def add_document(
        self, dataset_id: int, run_id: int, item: CatalogDocument, ordinal: int
    ) -> bool:
        now = utc_now()
        with self.connection:
            self.connection.execute(
                """INSERT INTO companies(ticker,source_ticker,first_seen_at,last_seen_at)
                VALUES(?,?,?,?) ON CONFLICT(ticker) DO UPDATE SET
                source_ticker=excluded.source_ticker,last_seen_at=excluded.last_seen_at""",
                (item.ticker, item.source_ticker, now, now),
            )
            self.connection.execute(
                """INSERT INTO documents(document_id,dataset_id,source_record_id,ticker,
                source_ticker,report_year,archive_name,archive_member,source_filename,
                expected_size,expected_sha256,source_status,source_notes,updated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(document_id) DO UPDATE SET ticker=excluded.ticker,
                source_ticker=excluded.source_ticker,report_year=excluded.report_year,
                archive_name=excluded.archive_name,archive_member=excluded.archive_member,
                source_filename=excluded.source_filename,expected_size=excluded.expected_size,
                expected_sha256=excluded.expected_sha256,source_status=excluded.source_status,
                source_notes=excluded.source_notes,updated_at=excluded.updated_at""",
                (
                    item.document_id,
                    dataset_id,
                    item.source_record_id,
                    item.ticker,
                    item.source_ticker,
                    item.report_year,
                    item.archive_name,
                    item.archive_member,
                    item.source_filename,
                    item.expected_size,
                    item.expected_sha256,
                    item.source_status,
                    item.source_notes,
                    now,
                ),
            )
            cursor = self.connection.execute(
                "INSERT OR IGNORE INTO run_documents VALUES(?,?,?,?)",
                (run_id, item.document_id, ordinal, ordinal // 1000),
            )
        return cursor.rowcount == 1

    def complete_catalog(self, run_id: int) -> None:
        self.set_run_status(run_id, RunStatus.PENDING)

    def set_run_status(
        self, run_id: int, status: RunStatus, error: str | None = None
    ) -> None:
        now = utc_now()
        self.connection.execute(
            """UPDATE runs SET status=?,started_at=COALESCE(started_at,?),
            completed_at=?,last_error=? WHERE id=?""",
            (
                status,
                now if status == RunStatus.RUNNING else None,
                now
                if status in {RunStatus.SUCCESS, RunStatus.PARTIAL, RunStatus.FAILED}
                else None,
                error,
                run_id,
            ),
        )
        self.connection.commit()

    def get_run(self, run_id: int) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM runs WHERE id=?", (run_id,)
        ).fetchone()
        if row is None:
            raise ValueError(f"Run does not exist: {run_id}")
        return row

    def latest_run_id(self) -> int:
        row = self.connection.execute(
            "SELECT id FROM runs ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if row is None:
            raise ValueError("No Vietnam runs exist")
        return int(row["id"])

    def source_files(
        self, dataset_id: int, kind: str | None = None
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM source_files WHERE dataset_id=?"
        params: tuple[Any, ...] = (dataset_id,)
        if kind:
            sql += " AND kind=?"
            params += (kind,)
        return [
            dict(row) for row in self.connection.execute(sql + " ORDER BY name", params)
        ]

    def documents(
        self, run_id: int, stage: Stage | None = None
    ) -> list[dict[str, Any]]:
        clause = ""
        if stage is not None:
            column = {
                Stage.DOWNLOAD: "download_status",
                Stage.EXTRACTION: "extraction_status",
                Stage.OCR: "ocr_status",
                Stage.NORMALIZATION: "normalization_status",
            }[stage]
            clause = f" AND d.{column} IN ('PENDING','RUNNING','NEEDS_OCR')"
        rows = self.connection.execute(
            f"""SELECT d.*,rd.ordinal,rd.batch_number FROM run_documents rd
            JOIN documents d ON d.document_id=rd.document_id
            WHERE rd.run_id=? {clause} ORDER BY rd.ordinal""",
            (run_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    def documents_for_archive(
        self, run_id: int, archive_name: str
    ) -> list[dict[str, Any]]:
        return [
            row for row in self.documents(run_id) if row["archive_name"] == archive_name
        ]

    def record_stage_start(
        self,
        run_id: int,
        document_id: str,
        stage: Stage,
        profile: str = "",
        config_snapshot: dict[str, Any] | None = None,
    ) -> int:
        attempt = (
            int(
                self.connection.execute(
                    "SELECT COUNT(*) AS n FROM stage_executions WHERE run_id=? AND document_id=? AND stage=?",
                    (run_id, document_id, stage),
                ).fetchone()["n"]
            )
            + 1
        )
        cursor = self.connection.execute(
            """INSERT INTO stage_executions(run_id,document_id,stage,profile,config_json,
            status,attempt,started_at) VALUES(?,?,?,?,?,?,?,?)""",
            (
                run_id,
                document_id,
                stage,
                profile,
                json.dumps(config_snapshot or {}, sort_keys=True, default=str),
                StageStatus.RUNNING,
                attempt,
                utc_now(),
            ),
        )
        column = _stage_column(stage)
        self.connection.execute(
            f"UPDATE documents SET {column}=?,updated_at=?,last_error=NULL WHERE document_id=?",
            (StageStatus.RUNNING, utc_now(), document_id),
        )
        self.connection.commit()
        return int(cursor.lastrowid)

    def record_stage_result(
        self,
        execution_id: int,
        document_id: str,
        stage: Stage,
        status: StageStatus,
        error: str | None = None,
        page_count: int | None = None,
        low_quality_pages: int | None = None,
    ) -> None:
        column = _stage_column(stage)
        with self.connection:
            self.connection.execute(
                "UPDATE stage_executions SET status=?,completed_at=?,error=? WHERE id=?",
                (status, utc_now(), error, execution_id),
            )
            extras = ""
            values: list[Any] = [status, utc_now(), error]
            if page_count is not None:
                extras += ",page_count=?"
                values.append(page_count)
            if low_quality_pages is not None:
                extras += ",low_quality_pages=?,ocr_status=?"
                values.extend(
                    [
                        low_quality_pages,
                        StageStatus.PENDING
                        if low_quality_pages
                        else StageStatus.NOT_REQUIRED,
                    ]
                )
            values.append(document_id)
            self.connection.execute(
                f"UPDATE documents SET {column}=?,updated_at=?,last_error=?{extras} WHERE document_id=?",
                values,
            )

    def save_artifact(
        self,
        document_id: str,
        kind: str,
        profile: str,
        local_path: str,
        size: int,
        checksum: str,
    ) -> None:
        now = utc_now()
        self.connection.execute(
            """INSERT INTO artifacts(document_id,kind,profile,local_path,file_size,checksum,
            status,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)
            ON CONFLICT(document_id,kind,profile) DO UPDATE SET local_path=excluded.local_path,
            file_size=excluded.file_size,checksum=excluded.checksum,status=excluded.status,
            updated_at=excluded.updated_at,last_error=NULL""",
            (
                document_id,
                kind,
                profile,
                local_path,
                size,
                checksum,
                StageStatus.SUCCESS,
                now,
                now,
            ),
        )
        self.connection.commit()

    def artifact(
        self, document_id: str, kind: str, profile: str = ""
    ) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT * FROM artifacts WHERE document_id=? AND kind=? AND profile=?",
            (document_id, kind, profile),
        ).fetchone()
        return dict(row) if row else None

    def artifacts_for_run(self, run_id: int) -> list[dict[str, Any]]:
        return [
            dict(row)
            for row in self.connection.execute(
                """SELECT a.*,d.ticker,d.report_year,d.expected_size,d.expected_sha256
            FROM run_documents rd JOIN documents d USING(document_id)
            JOIN artifacts a USING(document_id) WHERE rd.run_id=?
            ORDER BY rd.ordinal,a.kind,a.profile""",
                (run_id,),
            )
        ]

    def mark_document_stage_failed(
        self, run_id: int, document_id: str, stage: Stage, error: str
    ) -> None:
        column = _stage_column(stage)
        cascade = {
            Stage.DOWNLOAD: ",extraction_status='PENDING',ocr_status='NOT_REQUIRED',normalization_status='PENDING'",
            Stage.EXTRACTION: ",ocr_status='NOT_REQUIRED',normalization_status='PENDING'",
            Stage.OCR: ",normalization_status='PENDING'",
            Stage.NORMALIZATION: "",
        }[stage]
        with self.connection:
            self.connection.execute(
                f"UPDATE documents SET {column}='FAILED'{cascade},last_error=?,updated_at=? WHERE document_id=?",
                (error, utc_now(), document_id),
            )
            self.connection.execute(
                "UPDATE runs SET status='PARTIAL',last_error=? WHERE id=?",
                (error, run_id),
            )

    def finish_run(self, run_id: int) -> RunStatus:
        row = self.connection.execute(
            """SELECT COUNT(*) total,SUM(normalization_status='SUCCESS') successful,
            SUM(download_status='FAILED' OR extraction_status='FAILED' OR
                ocr_status='FAILED' OR normalization_status='FAILED') failed
            FROM run_documents rd JOIN documents d USING(document_id) WHERE run_id=?""",
            (run_id,),
        ).fetchone()
        total = int(row["total"] or 0)
        successful = int(row["successful"] or 0)
        status = (
            RunStatus.SUCCESS
            if total > 0 and total == successful
            else RunStatus.PARTIAL
        )
        self.set_run_status(
            run_id,
            status,
            None if status == RunStatus.SUCCESS else f"{successful}/{total} normalized",
        )
        return status

    def save_pages(
        self, document_id: str, profile: str, pages: Iterable[dict[str, Any]]
    ) -> None:
        with self.connection:
            self.connection.execute(
                "DELETE FROM page_quality WHERE document_id=? AND profile=?",
                (document_id, profile),
            )
            self.connection.executemany(
                """INSERT INTO page_quality(document_id,profile,page_number,method,
                character_count,printable_ratio,replacement_ratio,quality_score,needs_ocr,error)
                VALUES(?,?,?,?,?,?,?,?,?,?)""",
                (
                    (
                        document_id,
                        profile,
                        p["page_number"],
                        p["method"],
                        p["character_count"],
                        p["printable_ratio"],
                        p["replacement_ratio"],
                        p["quality_score"],
                        int(p["needs_ocr"]),
                        p.get("error"),
                    )
                    for p in pages
                ),
            )

    def recover_stale(self, run_id: int) -> int:
        changed = 0
        with self.connection:
            for stage in Stage:
                column = _stage_column(stage)
                changed += self.connection.execute(
                    f"""UPDATE documents SET {column}='PENDING',updated_at=?
                    WHERE {column}='RUNNING' AND document_id IN
                    (SELECT document_id FROM run_documents WHERE run_id=?)""",
                    (utc_now(), run_id),
                ).rowcount
            self.connection.execute(
                "UPDATE stage_executions SET status='FAILED',completed_at=?,error='Interrupted' WHERE run_id=? AND status='RUNNING'",
                (utc_now(), run_id),
            )
        return changed

    def reset_failed(self, run_id: int, stage: Stage) -> int:
        column = _stage_column(stage)
        with self.connection:
            count = self.connection.execute(
                f"""UPDATE documents SET {column}='PENDING',last_error=NULL,updated_at=?
                WHERE {column} IN ('FAILED','NEEDS_OCR') AND document_id IN
                (SELECT document_id FROM run_documents WHERE run_id=?)""",
                (utc_now(), run_id),
            ).rowcount
            self.connection.execute(
                "UPDATE runs SET status='PENDING',completed_at=NULL,last_error=NULL WHERE id=?",
                (run_id,),
            )
        return count

    def stats(self, run_id: int) -> dict[str, Any]:
        run = dict(self.get_run(run_id))
        summary = dict(
            self.connection.execute(
                """SELECT COUNT(*) documents,COUNT(DISTINCT d.ticker) tickers,
            SUM(d.expected_size) expected_pdf_bytes,
            SUM(d.download_status='SUCCESS') downloaded,
            SUM(d.extraction_status IN ('SUCCESS','NEEDS_OCR')) extracted,
            SUM(d.ocr_status='SUCCESS') ocr_success,
            SUM(d.normalization_status='SUCCESS') normalized,
            SUM(d.download_status='FAILED' OR d.extraction_status='FAILED' OR
                d.ocr_status='FAILED' OR d.normalization_status='FAILED') failed
            FROM run_documents rd JOIN documents d USING(document_id) WHERE rd.run_id=?""",
                (run_id,),
            ).fetchone()
        )
        run.update(summary)
        by_year = [
            dict(r)
            for r in self.connection.execute(
                """SELECT report_year,COUNT(*) documents,SUM(download_status='SUCCESS') downloaded,
            SUM(normalization_status='SUCCESS') normalized FROM run_documents rd
            JOIN documents d USING(document_id) WHERE run_id=? GROUP BY report_year ORDER BY report_year""",
                (run_id,),
            )
        ]
        by_stage = [
            {"stage": stage, "status": status, "documents": count}
            for stage in ("download", "extraction", "ocr", "normalization")
            for status, count in self.connection.execute(
                f"""SELECT d.{stage}_status,COUNT(*) FROM run_documents rd JOIN documents d
                USING(document_id) WHERE rd.run_id=? GROUP BY d.{stage}_status ORDER BY d.{stage}_status""",
                (run_id,),
            )
        ]
        return {"summary": run, "by_year": by_year, "by_stage": by_stage}


def _stage_column(stage: Stage) -> str:
    return {
        Stage.DOWNLOAD: "download_status",
        Stage.EXTRACTION: "extraction_status",
        Stage.OCR: "ocr_status",
        Stage.NORMALIZATION: "normalization_status",
    }[stage]


def _license_id(value: Any) -> str | None:
    if isinstance(value, dict):
        return str(value.get("id", "")) or None
    return str(value) if value else None
