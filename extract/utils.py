"""
England Hockey Analytics - Utilities
Logging and CSV writing utilities for the scraper.
"""

import csv
import logging
import sys
from dataclasses import fields, asdict
from pathlib import Path
from typing import TypeVar, Generic, Type, Optional, List, Dict, Tuple

from extract.config import (
    LOG_CONFIG,
    LOG_DIR,
    SAMPLE_DIR,
    StandingsRow,
    MatchRow,
    MatchEventRow,
)


# =============================================================================
# LOGGING
# =============================================================================

def setup_logger(
    name: str = "extract",
    log_file: Optional[str] = None,
    log_level: Optional[str] = None,
    log_dir: Optional[Path] = None,
) -> logging.Logger:
    """
    Configure and return a logger with dual output (file + console).

    Args:
        name: Logger name (default: "extract")
        log_file: Log file name (default: from LOG_CONFIG)
        log_level: Log level string (default: from LOG_CONFIG)
        log_dir: Directory for log file (default: LOG_DIR)

    Returns:
        Configured logging.Logger instance

    Example:
        logger = setup_logger()
        logger.info("Starting scrape...")
    """
    log_file = log_file or LOG_CONFIG.log_file
    log_level = log_level or LOG_CONFIG.log_level
    log_dir = log_dir or LOG_DIR

    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, log_level.upper()))

    # Avoid duplicate handlers if logger already configured
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        fmt=LOG_CONFIG.format,
        datefmt=LOG_CONFIG.date_format,
    )

    # File handler
    log_dir.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(log_dir / log_file, encoding="utf-8")
    file_handler.setLevel(getattr(logging, log_level.upper()))
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, log_level.upper()))
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


def get_logger(name: str = "extract") -> logging.Logger:
    """
    Get an existing logger or create one with default settings.

    Args:
        name: Logger name to retrieve

    Returns:
        logging.Logger instance
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        return setup_logger(name)
    return logger


# =============================================================================
# CSV WRITER
# =============================================================================

T = TypeVar("T", StandingsRow, MatchRow, MatchEventRow)


class CSVWriter(Generic[T]):
    """
    Incremental CSV writer that writes dataclass rows to a file.

    Writes header on first row, then appends subsequent rows.
    Designed for streaming/incremental writes during scraping.

    Example:
        with CSVWriter(StandingsRow, "standings.csv") as writer:
            writer.write_row(StandingsRow(...))
    """

    def __init__(
        self,
        row_type: Type[T],
        filename: str,
        output_dir: Optional[Path] = None,
        overwrite: bool = True,
    ) -> None:
        """
        Initialize the CSV writer.

        Args:
            row_type: Dataclass type for rows (e.g., StandingsRow)
            filename: Output CSV filename
            output_dir: Directory for output (default: SAMPLE_DIR)
            overwrite: If True, overwrite existing file; if False, append
        """
        self.row_type = row_type
        self.output_dir = output_dir or SAMPLE_DIR
        self.filepath = self.output_dir / filename
        self.overwrite = overwrite
        self.fieldnames = [f.name for f in fields(row_type)]

        self._file = None
        self._writer = None
        self._header_written = False
        self._row_count = 0

    def _ensure_open(self) -> None:
        """Ensure file is open and header is written."""
        if self._file is None:
            self.output_dir.mkdir(parents=True, exist_ok=True)

            file_exists = self.filepath.exists()
            if self.overwrite or not file_exists:
                mode = "w"
                write_header = True
            else:
                mode = "a"
                write_header = False

            self._file = open(
                self.filepath,
                mode=mode,
                newline="",
                encoding="utf-8",
            )
            self._writer = csv.DictWriter(self._file, fieldnames=self.fieldnames)

            if write_header:
                self._writer.writeheader()
            self._header_written = True

    def write_row(self, row: T) -> None:
        """Write a single row to the CSV file."""
        self._ensure_open()
        self._writer.writerow(asdict(row))
        self._row_count += 1
        self._file.flush()

    def write_rows(self, rows: list[T]) -> None:
        """Write multiple rows to the CSV file."""
        for row in rows:
            self.write_row(row)

    @property
    def row_count(self) -> int:
        """Return the number of rows written."""
        return self._row_count

    def close(self) -> None:
        """Close the file handle."""
        if self._file is not None:
            self._file.close()
            self._file = None
            self._writer = None

    def __enter__(self) -> "CSVWriter[T]":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - close file."""
        self.close()


# =============================================================================
# CSV UPSERT WRITER (Smart Merge)
# =============================================================================


class CSVUpsertWriter(Generic[T]):
    """
    CSV writer with upsert (update or insert) logic.

    Loads existing CSV data, merges with new data based on key fields,
    then writes back. Prevents data loss when running with different parameters.

    Example:
        writer = CSVUpsertWriter(
            StandingsRow,
            Path("standings.csv"),
            key_fields=["season", "competition", "team", "snapshot_date"]
        )
        inserted, updated = writer.upsert_rows(new_standings)
        writer.save()
    """

    def __init__(
        self,
        row_type: Type[T],
        output_path: Path,
        key_fields: List[str],
    ) -> None:
        """
        Initialize the upsert writer.

        Args:
            row_type: Dataclass type for rows (e.g., StandingsRow)
            output_path: Full path to output CSV file
            key_fields: List of field names that form the unique key
        """
        self.row_type = row_type
        self.output_path = output_path
        self.key_fields = key_fields
        self.fieldnames = [f.name for f in fields(row_type)]
        self.existing_data: Dict[tuple, dict] = self._load_existing()

    def _load_existing(self) -> Dict[tuple, dict]:
        """Load existing CSV into dict keyed by unique key."""
        data = {}
        if self.output_path.exists():
            with open(self.output_path, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    key = tuple(row.get(k, "") for k in self.key_fields)
                    data[key] = row
        return data

    def _get_key(self, row: T) -> tuple:
        """Extract key fields from row object."""
        return tuple(getattr(row, k) for k in self.key_fields)

    def upsert_rows(self, rows: List[T]) -> Tuple[int, int]:
        """
        Upsert rows into existing data.

        Args:
            rows: List of dataclass row objects

        Returns:
            Tuple of (inserted_count, updated_count)
        """
        inserted = 0
        updated = 0

        for row in rows:
            key = self._get_key(row)
            row_dict = asdict(row)

            if key in self.existing_data:
                self.existing_data[key] = row_dict
                updated += 1
            else:
                self.existing_data[key] = row_dict
                inserted += 1

        return inserted, updated

    def save(self) -> None:
        """Write all data back to CSV."""
        self.output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(self.output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.fieldnames)
            writer.writeheader()
            for row in self.existing_data.values():
                writer.writerow(row)

    @property
    def total_rows(self) -> int:
        """Return total number of rows in data."""
        return len(self.existing_data)
