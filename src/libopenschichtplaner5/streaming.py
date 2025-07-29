# src/libopenschichtplaner5/streaming.py
"""
Streaming and pagination support for large DBF files.
Provides memory-efficient data access.
"""

import logging
from pathlib import Path
from typing import Iterator, Dict, Any, List, Optional, Callable, TypeVar
from dataclasses import dataclass
import time
from collections import deque

from .db.reader import DBFTable, ENCODINGS
from .exceptions import DBFLoadError

logger = logging.getLogger(__name__)

T = TypeVar('T')


@dataclass
class StreamConfig:
    """Configuration for streaming operations."""
    chunk_size: int = 1000
    buffer_size: int = 5000
    prefetch: bool = True
    cache_chunks: bool = True
    max_cache_size: int = 10


class StreamingDBFReader:
    """
    Memory-efficient DBF reader with streaming support.
    Reads data in chunks instead of loading everything into memory.
    """

    def __init__(self, path: Path, config: Optional[StreamConfig] = None):
        self.path = path
        self.config = config or StreamConfig()
        self._dbf = None
        self._total_records = None
        self._chunk_cache = deque(maxlen=self.config.max_cache_size)
        self._cache_index = {}

    def __enter__(self):
        """Context manager entry."""
        self._open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self._close()

    def _open(self):
        """Open the DBF file."""
        if not self.path.exists():
            raise FileNotFoundError(f"DBF file not found: {self.path}")

        # Try different encodings
        last_error = None
        for encoding in ENCODINGS:
            try:
                from dbfread import DBF
                self._dbf = DBF(
                    self.path,
                    load=False,  # Important: Don't load all records
                    ignore_missing_memofile=True,
                    encoding=encoding,
                    char_decode_errors='replace'
                )
                # Get total record count
                self._total_records = len(self._dbf)
                logger.info(f"Opened {self.path.name} for streaming ({self._total_records} records)")
                return
            except Exception as e:
                last_error = e
                continue

        raise DBFLoadError(f"Could not open DBF file: {last_error}")

    def _close(self):
        """Close the DBF file."""
        if self._dbf:
            self._dbf = None
            self._chunk_cache.clear()
            self._cache_index.clear()

    def stream_records(self,
                       start: int = 0,
                       end: Optional[int] = None,
                       filter_func: Optional[Callable[[Dict], bool]] = None) -> Iterator[Dict[str, Any]]:
        """
        Stream records from the DBF file.

        Args:
            start: Starting record index
            end: Ending record index (None for all)
            filter_func: Optional filter function

        Yields:
            Dict containing record data
        """
        if not self._dbf:
            self._open()

        # Determine range
        end = min(end or self._total_records, self._total_records)

        # Stream in chunks
        for chunk_start in range(start, end, self.config.chunk_size):
            chunk_end = min(chunk_start + self.config.chunk_size, end)

            # Check cache first
            cache_key = (chunk_start, chunk_end)
            if self.config.cache_chunks and cache_key in self._cache_index:
                logger.debug(f"Using cached chunk {cache_key}")
                chunk = self._chunk_cache[self._cache_index[cache_key]]
            else:
                # Load chunk
                chunk = self._load_chunk(chunk_start, chunk_end)

                # Cache if enabled
                if self.config.cache_chunks:
                    self._chunk_cache.append(chunk)
                    self._cache_index[cache_key] = len(self._chunk_cache) - 1

            # Yield records
            for record in chunk:
                if filter_func is None or filter_func(record):
                    yield record

    def _load_chunk(self, start: int, end: int) -> List[Dict[str, Any]]:
        """Load a chunk of records."""
        logger.debug(f"Loading chunk [{start}:{end}]")
        chunk = []

        # Skip to start position
        record_iter = iter(self._dbf)
        for _ in range(start):
            next(record_iter, None)

        # Read chunk
        for i in range(end - start):
            try:
                record = next(record_iter)
                # Clean record data
                cleaned = self._clean_record(record)
                chunk.append(cleaned)
            except StopIteration:
                break

        return chunk

    def _clean_record(self, record: Dict) -> Dict[str, Any]:
        """Clean and normalize record data."""
        cleaned = {}
        for field, value in record.items():
            # Apply same cleaning as DBFTable
            if isinstance(value, str):
                value = value.replace("\x00", "").strip()
            cleaned[field] = value
        return cleaned

    def count_records(self, filter_func: Optional[Callable[[Dict], bool]] = None) -> int:
        """Count records matching filter."""
        if filter_func is None:
            return self._total_records

        count = 0
        for record in self.stream_records(filter_func=filter_func):
            count += 1
        return count

    def get_page(self, page: int, page_size: int,
                 filter_func: Optional[Callable[[Dict], bool]] = None) -> Tuple[List[Dict], int]:
        """
        Get a page of records.

        Returns:
            Tuple of (records, total_pages)
        """
        start = page * page_size
        end = start + page_size

        records = list(self.stream_records(start, end, filter_func))

        # Calculate total pages
        total_records = self.count_records(filter_func)
        total_pages = (total_records + page_size - 1) // page_size

        return records, total_pages

    def get_statistics(self) -> Dict[str, Any]:
        """Get reader statistics."""
        return {
            "file": str(self.path),
            "total_records": self._total_records,
            "chunk_size": self.config.chunk_size,
            "cache_size": len(self._chunk_cache),
            "cached_chunks": len(self._cache_index)
        }


class BatchProcessor:
    """Process DBF records in batches for better performance."""

    def __init__(self, batch_size: int = 1000):
        self.batch_size = batch_size
        self.stats = {
            "batches_processed": 0,
            "records_processed": 0,
            "errors": 0,
            "processing_time": 0.0
        }

    def process_table(self,
                      table_path: Path,
                      processor_func: Callable[[List[Dict]], Any],
                      filter_func: Optional[Callable[[Dict], bool]] = None,
                      progress_callback: Optional[Callable[[int, int], None]] = None) -> List[Any]:
        """
        Process a table in batches.

        Args:
            table_path: Path to DBF file
            processor_func: Function to process each batch
            filter_func: Optional filter for records
            progress_callback: Optional callback(current, total)

        Returns:
            List of results from processor_func
        """
        results = []
        start_time = time.time()

        with StreamingDBFReader(table_path) as reader:
            total = reader._total_records
            batch = []
            current = 0

            for record in reader.stream_records(filter_func=filter_func):
                batch.append(record)
                current += 1

                if len(batch) >= self.batch_size:
                    # Process batch
                    try:
                        result = processor_func(batch)
                        if result is not None:
                            results.append(result)
                        self.stats["batches_processed"] += 1
                    except Exception as e:
                        logger.error(f"Error processing batch: {e}")
                        self.stats["errors"] += 1

                    self.stats["records_processed"] += len(batch)

                    # Progress callback
                    if progress_callback:
                        progress_callback(current, total)

                    # Clear batch
                    batch = []

            # Process remaining records
            if batch:
                try:
                    result = processor_func(batch)
                    if result is not None:
                        results.append(result)
                    self.stats["batches_processed"] += 1
                    self.stats["records_processed"] += len(batch)
                except Exception as e:
                    logger.error(f"Error processing final batch: {e}")
                    self.stats["errors"] += 1

        self.stats["processing_time"] = time.time() - start_time
        return results

    def get_statistics(self) -> Dict[str, Any]:
        """Get processor statistics."""
        stats = self.stats.copy()
        if stats["processing_time"] > 0:
            stats["records_per_second"] = stats["records_processed"] / stats["processing_time"]
        return stats


class StreamingQueryEngine:
    """Query engine with streaming support for large datasets."""

    def __init__(self, dbf_dir: Path):
        self.dbf_dir = dbf_dir
        self.stream_config = StreamConfig()

    def stream_query(self,
                     table: str,
                     filters: Optional[List[Tuple[str, str, Any]]] = None,
                     chunk_size: int = 1000) -> Iterator[List[Dict]]:
        """
        Execute a streaming query.

        Args:
            table: Table name
            filters: List of (field, operator, value) tuples
            chunk_size: Size of each chunk

        Yields:
            Chunks of matching records
        """
        # Find table file
        table_path = None
        for ext in [".DBF", ".dbf", ".txt", ".TXT"]:
            path = self.dbf_dir / f"{table}{ext}"
            if path.exists():
                table_path = path
                break

        if not table_path:
            raise FileNotFoundError(f"Table {table} not found")

        # Create filter function
        def filter_func(record: Dict) -> bool:
            if not filters:
                return True

            for field, op, value in filters:
                record_value = record.get(field)

                if op == "=" and record_value != value:
                    return False
                elif op == "!=" and record_value == value:
                    return False
                elif op == ">" and not (record_value and record_value > value):
                    return False
                elif op == "<" and not (record_value and record_value < value):
                    return False
                elif op == "in" and record_value not in value:
                    return False
                elif op == "contains" and value not in str(record_value):
                    return False

            return True

        # Stream with chunking
        self.stream_config.chunk_size = chunk_size
        chunk = []

        with StreamingDBFReader(table_path, self.stream_config) as reader:
            for record in reader.stream_records(filter_func=filter_func):
                chunk.append(record)

                if len(chunk) >= chunk_size:
                    yield chunk
                    chunk = []

            # Yield remaining records
            if chunk:
                yield chunk

    def count_streaming(self,
                        table: str,
                        filters: Optional[List[Tuple[str, str, Any]]] = None) -> int:
        """Count records using streaming."""
        count = 0
        for chunk in self.stream_query(table, filters, chunk_size=10000):
            count += len(chunk)
        return count


# Example usage functions
def process_large_employee_table(dbf_path: Path) -> Dict[str, Any]:
    """Example: Process a large employee table efficiently."""
    processor = BatchProcessor(batch_size=5000)

    def analyze_batch(records: List[Dict]) -> Dict[str, int]:
        """Analyze a batch of employee records."""
        stats = {
            "active": 0,
            "inactive": 0,
            "positions": set()
        }

        for record in records:
            if record.get("EMPEND") is None:
                stats["active"] += 1
            else:
                stats["inactive"] += 1

            position = record.get("POSITION")
            if position:
                stats["positions"].add(position)

        return {
            "active": stats["active"],
            "inactive": stats["inactive"],
            "unique_positions": len(stats["positions"])
        }

    results = processor.process_table(
        dbf_path,
        analyze_batch,
        progress_callback=lambda current, total: logger.info(f"Progress: {current}/{total}")
    )

    # Aggregate results
    total_stats = {
        "active": sum(r["active"] for r in results),
        "inactive": sum(r["inactive"] for r in results),
        "unique_positions": len(set().union(*[r.get("positions", set()) for r in results]))
    }

    return total_stats