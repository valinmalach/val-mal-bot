import asyncio
import logging
from collections import defaultdict
from threading import Lock
from typing import Any, Dict, Optional, Set

import polars as pl

from constants import LiveAlert, UserRecord

logger = logging.getLogger(__name__)


class ParquetCache:
    def __init__(self, flush_interval: int = 30):
        self._cache: Dict[str, pl.DataFrame] = {}
        self._dirty_files: Set[str] = set()
        self._pending_writes: Dict[str, Dict[str, Any]] = defaultdict(dict)
        self._pending_deletes: Dict[str, Dict[str, Set[Any]]] = defaultdict(
            lambda: defaultdict(set)
        )
        self._id_columns: Dict[str, str] = {}
        self._lock = Lock()
        self._flush_interval = flush_interval
        self._flush_task: Optional[asyncio.Task] = None

    def start(self):
        """Start the periodic flush task"""
        if self._flush_task is None:
            self._flush_task = asyncio.create_task(self._periodic_flush())

    async def stop(self):
        """Stop and flush any remaining data"""
        if self._flush_task:
            self._flush_task.cancel()
            await self._force_flush()

    async def _periodic_flush(self):
        """Periodically flush dirty data to files"""
        while True:
            try:
                await asyncio.sleep(self._flush_interval)
                await self._force_flush()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Error in periodic flush: {e}")

    async def _force_flush(self):
        """Force flush all dirty data"""
        with self._lock:
            dirty_files = self._dirty_files.copy()
            self._dirty_files.clear()

        for filepath in dirty_files:
            try:
                await self._flush_file(filepath)
            except Exception as e:
                logger.error(f"Error flushing {filepath}: {e}")

    async def _flush_file(self, filepath: str):
        """Flush a specific file's changes"""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._flush_file_sync, filepath)

    def _flush_file_sync(self, filepath: str):
        """Synchronously flush file changes"""
        with self._lock:
            df = self._ensure_dataframe_loaded(filepath)
            df = self._apply_pending_changes(filepath, df)
            self._save_dataframe(filepath, df)

    def _ensure_dataframe_loaded(self, filepath: str) -> pl.DataFrame:
        """Ensure DataFrame is loaded into cache"""
        if filepath not in self._cache:
            try:
                self._cache[filepath] = pl.read_parquet(filepath)
            except FileNotFoundError:
                # Create empty DataFrame with basic structure
                self._cache[filepath] = pl.DataFrame()
        return self._cache[filepath]

    def _apply_pending_changes(self, filepath: str, df: pl.DataFrame) -> pl.DataFrame:
        """Apply all pending changes to the DataFrame"""
        df = self._apply_pending_deletes(filepath, df)
        df = self._apply_pending_writes(filepath, df)
        return df

    def _apply_pending_deletes(self, filepath: str, df: pl.DataFrame) -> pl.DataFrame:
        """Apply pending delete operations"""
        if filepath not in self._pending_deletes:
            return df

        for column_name, id_values in self._pending_deletes[filepath].items():
            for id_value in id_values:
                df = df.filter(pl.col(column_name) != id_value)
            id_values.clear()
        return df

    def _apply_pending_writes(self, filepath: str, df: pl.DataFrame) -> pl.DataFrame:
        """Apply pending write operations"""
        if filepath not in self._pending_writes:
            return df

        new_rows = list(self._pending_writes[filepath].values())
        if not new_rows:
            return df

        new_df = pl.DataFrame(new_rows)
        id_column = self._id_columns.get(filepath, "id")

        # Remove existing rows with same IDs
        if not df.is_empty() and id_column in df.columns:
            existing_ids = set(new_df[id_column].to_list())
            df = df.filter(~pl.col(id_column).is_in(existing_ids))

        # Concat new data
        df = new_df if df.is_empty() else pl.concat([df, new_df])
        self._pending_writes[filepath].clear()
        return df

    def _save_dataframe(self, filepath: str, df: pl.DataFrame):
        """Save DataFrame to cache and file"""
        self._cache[filepath] = df
        df.write_parquet(filepath)

    def upsert_row(
        self,
        row_data: dict | UserRecord | LiveAlert,
        filepath: str,
        id_column: str = "id",
    ) -> None:
        """Queue a row for upserting"""
        with self._lock:
            self._id_columns[filepath] = id_column
            id_value = row_data[id_column]
            self._pending_writes[filepath][id_value] = row_data
            self._dirty_files.add(filepath)

    def delete_row(self, id_value: Any, filepath: str, id_column: str = "id") -> None:
        """Queue a row for deletion"""
        with self._lock:
            self._id_columns[filepath] = id_column
            self._pending_deletes[filepath][id_column].add(id_value)
            # Remove from pending writes if it exists
            if id_value in self._pending_writes[filepath]:
                del self._pending_writes[filepath][id_value]
            self._dirty_files.add(filepath)

    async def read_df(self, filepath: str) -> pl.DataFrame:
        """Read DataFrame with cache"""
        with self._lock:
            if filepath in self._cache:
                return self._cache[filepath].clone()

        # Load from file if not in cache
        loop = asyncio.get_event_loop()
        df = await loop.run_in_executor(None, self._load_file, filepath)

        with self._lock:
            self._cache[filepath] = df

        return df.clone()

    def _load_file(self, filepath: str) -> pl.DataFrame:
        """Load file from disk"""
        try:
            return pl.read_parquet(filepath)
        except FileNotFoundError:
            return pl.DataFrame()


# Global cache instance
parquet_cache = ParquetCache(flush_interval=30)  # Flush every 30 seconds
