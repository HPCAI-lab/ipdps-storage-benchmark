"""SQLite database backend — mirrors the original app.py schema exactly."""

import sqlite3
from pathlib import Path
from .base import DatabaseBackend


class SQLiteBackend(DatabaseBackend):
    def __init__(
        self,
        db_path: str = "./data/storage.db",
        journal_mode: str = "WAL",
        synchronous: str = "NORMAL",
    ):
        self.db_path = db_path
        self.journal_mode = journal_mode.upper()
        self.synchronous = synchronous.upper()
        self.conn = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def connect(self):
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self.journal_mode = self.conn.execute(
            f"PRAGMA journal_mode={self.journal_mode}"
        ).fetchone()[0].upper()
        self.conn.execute(f"PRAGMA synchronous={self.synchronous}")

    def checkpoint(self):
        if self.conn and self.journal_mode == "WAL":
            self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    def initialize_schema(self):
        cursor = self.conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS xyz_files (
                file_id   INTEGER PRIMARY KEY AUTOINCREMENT,
                filename  TEXT    NOT NULL,
                num_atoms INTEGER NOT NULL,
                num_frames INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS coordinates (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                file_id    INTEGER NOT NULL,
                frame      INTEGER NOT NULL,
                atom_index INTEGER NOT NULL,
                x REAL NOT NULL,
                y REAL NOT NULL,
                z REAL NOT NULL,
                FOREIGN KEY (file_id) REFERENCES xyz_files(file_id)
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_file_frame
            ON coordinates(file_id, frame)
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS telemetry (
                id     INTEGER PRIMARY KEY AUTOINCREMENT,
                ts     TIMESTAMP NOT NULL,
                metric TEXT      NOT NULL,
                value  REAL      NOT NULL,
                tags   TEXT
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_telemetry_ts
            ON telemetry(ts)
        """)
        self.conn.commit()

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def get_backend_name(self) -> str:
        return "sqlite"

    # ------------------------------------------------------------------
    # Coordinate workload
    # ------------------------------------------------------------------

    def insert_file_metadata(self, filename: str, num_atoms: int, num_frames: int) -> int:
        """Insert xyz_files row and return the new file_id."""
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT INTO xyz_files (filename, num_atoms, num_frames) VALUES (?, ?, ?)",
            (filename, num_atoms, num_frames),
        )
        self.conn.commit()
        return cursor.lastrowid

    def insert_coordinates_batch(self, records: list[dict]):
        """Insert coordinate records in a single executemany call (batch of ~1000)."""
        rows = [(r["file_id"], r["frame"], r["atom_index"], r["x"], r["y"], r["z"]) for r in records]
        self.conn.executemany(
            "INSERT INTO coordinates (file_id, frame, atom_index, x, y, z) VALUES (?, ?, ?, ?, ?, ?)",
            rows,
        )
        self.conn.commit()

    def query_coordinates(self, file_id: int) -> list:
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT frame, atom_index, x, y, z FROM coordinates WHERE file_id = ? ORDER BY frame, atom_index",
            (file_id,),
        )
        return cursor.fetchall()

    # ------------------------------------------------------------------
    # Telemetry workload
    # ------------------------------------------------------------------

    def insert_telemetry_batch(self, records: list[dict]):
        """Insert telemetry records; tags dict is serialised as JSON string."""
        import json
        rows = [(r["ts"], r["metric"], r["value"], json.dumps(r.get("tags", {}))) for r in records]
        self.conn.executemany(
            "INSERT INTO telemetry (ts, metric, value, tags) VALUES (?, ?, ?, ?)",
            rows,
        )
        self.conn.commit()

    def query_telemetry_range(self, start, end) -> list:
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT ts, metric, value, tags FROM telemetry WHERE ts >= ? AND ts <= ? ORDER BY ts",
            (start, end),
        )
        return cursor.fetchall()
