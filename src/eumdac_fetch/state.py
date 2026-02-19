"""SQLite state database for tracking per-product download status."""

from __future__ import annotations

import sqlite3
import threading
from datetime import UTC, datetime
from pathlib import Path

from eumdac_fetch.models import ProductRecord, ProductStatus


class StateDB:
    """Thread-safe SQLite state tracker for product processing status."""

    def __init__(self, db_path: str | Path):
        self.db_path = str(db_path)
        self._local = threading.local()
        self._init_db()

    @property
    def _conn(self) -> sqlite3.Connection:
        """Get a thread-local connection."""
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = sqlite3.connect(self.db_path)
            self._local.conn.row_factory = sqlite3.Row
            self._local.conn.execute("PRAGMA journal_mode=WAL")
        return self._local.conn

    def _init_db(self) -> None:
        """Create the products and search_results tables if they don't exist."""
        # noinspection SqlNoDataSourceInspection,SqlDialectInspection
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS products (
                product_id TEXT NOT NULL,
                job_name TEXT NOT NULL,
                collection TEXT NOT NULL DEFAULT '',
                size_kb REAL NOT NULL DEFAULT 0,
                md5 TEXT NOT NULL DEFAULT '',
                bytes_downloaded INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'pending',
                download_path TEXT NOT NULL DEFAULT '',
                error_message TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (product_id, job_name)
            )
        """)
        # noinspection SqlNoDataSourceInspection,SqlDialectInspection
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS search_results (
                product_id TEXT PRIMARY KEY,
                collection TEXT NOT NULL DEFAULT '',
                size_kb REAL NOT NULL DEFAULT 0,
                sensing_start TEXT NOT NULL DEFAULT '',
                sensing_end TEXT NOT NULL DEFAULT '',
                cached_at TEXT NOT NULL DEFAULT ''
            )
        """)
        self._conn.commit()

    def get(self, product_id: str, job_name: str) -> ProductRecord | None:
        """Get a product record by ID and job name."""
        # noinspection SqlNoDataSourceInspection,SqlDialectInspection
        row = self._conn.execute(
            "SELECT * FROM products WHERE product_id = ? AND job_name = ?",
            (product_id, job_name),
        ).fetchone()
        if row is None:
            return None
        return self._row_to_record(row)

    def upsert(self, record: ProductRecord) -> None:
        """Insert or update a product record."""
        now = datetime.now(UTC).isoformat()
        if not record.created_at:
            record.created_at = now
        record.updated_at = now

        # noinspection SqlNoDataSourceInspection,SqlDialectInspection
        self._conn.execute(
            """
            INSERT INTO products (
                product_id, job_name, collection, size_kb, md5,
                bytes_downloaded, status, download_path,
                error_message, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(product_id, job_name) DO UPDATE SET
                size_kb = excluded.size_kb,
                md5 = excluded.md5,
                bytes_downloaded = excluded.bytes_downloaded,
                status = excluded.status,
                download_path = excluded.download_path,
                error_message = excluded.error_message,
                updated_at = excluded.updated_at
            """,
            (
                record.product_id,
                record.job_name,
                record.collection,
                record.size_kb,
                record.md5,
                record.bytes_downloaded,
                record.status.value,
                record.download_path,
                record.error_message,
                record.created_at,
                record.updated_at,
            ),
        )
        self._conn.commit()

    def update_status(self, product_id: str, job_name: str, status: ProductStatus, **kwargs: object) -> None:
        """Update status and optional fields for a product."""
        now = datetime.now(UTC).isoformat()
        sets = ["status = ?", "updated_at = ?"]
        params: list = [status.value, now]

        for key, value in kwargs.items():
            sets.append(f"{key} = ?")
            params.append(value)

        params.extend([product_id, job_name])
        # noinspection SqlNoDataSourceInspection,SqlDialectInspection
        self._conn.execute(
            f"UPDATE products SET {', '.join(sets)} WHERE product_id = ? AND job_name = ?",
            params,
        )
        self._conn.commit()

    def get_by_status(self, job_name: str, status: ProductStatus) -> list[ProductRecord]:
        """Get all products with a given status for a job."""
        # noinspection SqlNoDataSourceInspection,SqlDialectInspection
        rows = self._conn.execute(
            "SELECT * FROM products WHERE job_name = ? AND status = ?",
            (job_name, status.value),
        ).fetchall()
        return [self._row_to_record(row) for row in rows]

    def get_all(self, job_name: str) -> list[ProductRecord]:
        """Get all product records for a job."""
        # noinspection SqlNoDataSourceInspection,SqlDialectInspection
        rows = self._conn.execute(
            "SELECT * FROM products WHERE job_name = ?",
            (job_name,),
        ).fetchall()
        return [self._row_to_record(row) for row in rows]

    def get_resumable(self, job_name: str) -> list[ProductRecord]:
        """Get products that need downloading (PENDING, DOWNLOADING, or FAILED).

        DOWNLOADING is included because a killed process may leave products
        in that state — they need to be retried on the next run.
        """
        # noinspection SqlNoDataSourceInspection,SqlDialectInspection
        rows = self._conn.execute(
            "SELECT * FROM products WHERE job_name = ? AND status IN (?, ?, ?)",
            (job_name, ProductStatus.PENDING.value, ProductStatus.DOWNLOADING.value, ProductStatus.FAILED.value),
        ).fetchall()
        return [self._row_to_record(row) for row in rows]

    def reset_stale_downloads(self, job_name: str) -> int:
        """Reset DOWNLOADING products to PENDING (stale from a killed process).

        Returns the number of products reset.
        """
        now = datetime.now(UTC).isoformat()
        # noinspection SqlNoDataSourceInspection,SqlDialectInspection
        cursor = self._conn.execute(
            "UPDATE products SET status = ?, updated_at = ? WHERE job_name = ? AND status = ?",
            (ProductStatus.PENDING.value, now, job_name, ProductStatus.DOWNLOADING.value),
        )
        self._conn.commit()
        return cursor.rowcount

    @staticmethod
    def _row_to_record(row: sqlite3.Row) -> ProductRecord:
        """Convert a database row to a ProductRecord."""
        return ProductRecord(
            product_id=row["product_id"],
            job_name=row["job_name"],
            collection=row["collection"],
            size_kb=row["size_kb"],
            md5=row["md5"],
            bytes_downloaded=row["bytes_downloaded"],
            status=ProductStatus(row["status"]),
            download_path=row["download_path"],
            error_message=row["error_message"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def cache_search_results(self, products: list, collection: str) -> None:
        """Bulk insert product metadata from eumdac product objects into search_results cache."""
        now = datetime.now(UTC).isoformat()
        rows = []
        for product in products:
            product_id = str(product)
            size_kb = getattr(product, "size", 0) or 0
            sensing_start = str(getattr(product, "sensing_start", ""))
            sensing_end = str(getattr(product, "sensing_end", ""))
            rows.append((product_id, collection, size_kb, sensing_start, sensing_end, now))
        self._conn.executemany(
            """
            INSERT OR REPLACE INTO search_results
                (product_id, collection, size_kb, sensing_start, sensing_end, cached_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        self._conn.commit()

    def get_cached_search_results(self) -> list[dict]:
        """Return all cached search result metadata."""
        # noinspection SqlNoDataSourceInspection,SqlDialectInspection
        rows = self._conn.execute("SELECT * FROM search_results").fetchall()
        return [dict(row) for row in rows]

    def has_cached_search(self) -> bool:
        """Check if the search_results cache has any rows."""
        # noinspection SqlNoDataSourceInspection,SqlDialectInspection
        row = self._conn.execute("SELECT COUNT(*) AS cnt FROM search_results").fetchone()
        return row["cnt"] > 0

    def close(self) -> None:
        """Close the thread-local connection."""
        if hasattr(self._local, "conn") and self._local.conn:
            self._local.conn.close()
            self._local.conn = None
