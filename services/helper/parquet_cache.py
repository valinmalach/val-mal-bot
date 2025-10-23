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
        self._pending_deletes: Dict[str, Set[Any]] = defaultdict(set)
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
                break
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
            if filepath not in self._cache:
                try:
                    self._cache[filepath] = pl.read_parquet(filepath)
                except FileNotFoundError:
                    # Create empty DataFrame with basic structure
                    self._cache[filepath] = pl.DataFrame()

            df = self._cache[filepath]

            # Apply pending deletes
            if filepath in self._pending_deletes:
                for id_value in self._pending_deletes[filepath]:
                    df = df.filter(pl.col("id") != id_value)
                self._pending_deletes[filepath].clear()

            # Apply pending writes
            if filepath in self._pending_writes:
                if new_rows := list(self._pending_writes[filepath].values()):
                    new_df = pl.DataFrame(new_rows)

                    # Remove existing rows with same IDs
                    if not df.is_empty() and "id" in df.columns:
                        existing_ids = set(new_df["id"].to_list())
                        df = df.filter(~pl.col("id").is_in(existing_ids))

                    # Concat new data
                    df = new_df if df.is_empty() else pl.concat([df, new_df])
                self._pending_writes[filepath].clear()

            # Update cache and write to file
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
            id_value = row_data[id_column]
            self._pending_writes[filepath][id_value] = row_data
            self._dirty_files.add(filepath)

    def delete_row(
        self, id_value: Any, filepath: str, id_column: str = "id"
    ) -> None:
        """Queue a row for deletion"""
        with self._lock:
            self._pending_deletes[filepath].add(id_value)
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
