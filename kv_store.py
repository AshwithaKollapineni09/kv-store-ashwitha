#!/usr/bin/env python3
"""
Project 1 — Persistent Key–Value Store (Append-only)
Author: Ashwitha Kollapineni

A minimal persistent key–value store backed by an append-only log file.
- Rebuilds state by replaying the log on startup
- In-memory index implemented without using dict/map
- CLI:  SET <key> <value> | GET <key> | EXIT
"""

from __future__ import annotations

import os
import sys
from typing import List, Optional, Tuple

DATA_FILE = "data.db"


# ---------------------------------------------------------------------------
# Exceptions (more specific than generic OSError)
# ---------------------------------------------------------------------------
class KVError(Exception):
    """Base exception for the key–value store."""


class DataFileOpenError(KVError):
    """Raised when opening the data file fails."""


class DataFileWriteError(KVError):
    """Raised when appending a record to the data file fails."""


class DataFileCloseError(KVError):
    """Raised when flushing/closing the data file fails."""


def valid_token(tok: str) -> bool:
    """Return True if token is non-empty and contains no whitespace."""
    return bool(tok) and not any(c.isspace() for c in tok)


# ---------------------------------------------------------------------------
# Lightweight hash table (no built-in dict)
# ---------------------------------------------------------------------------
class SimpleHashMap:
    """Minimal str→str hash map with separate chaining (no dict)."""

    __slots__ = ("_buckets", "_size")

    def __init__(self, initial_capacity: int = 1024) -> None:
        """
        Initialize the bucket array.

        Args:
            initial_capacity: Suggested number of buckets (will be rounded to a power of two).
        """
        cap = 1
        while cap < initial_capacity:
            cap <<= 1  # keep capacity as a power of two for fast masking
        self._buckets: List[List[Tuple[str, str]]] = [[] for _ in range(cap)]
        self._size = 0

    def _index(self, key: str) -> int:
        """Compute bucket index for the given key via bitmasking."""
        return hash(key) & (len(self._buckets) - 1)

    def get(self, key: str) -> Optional[str]:
        """
        Retrieve the value for a key.

        Returns:
            The value if present, otherwise None.
        """
        bucket = self._buckets[self._index(key)]
        for k, v in bucket:
            if k == key:
                return v
        return None

    def set(self, key: str, value: str) -> None:
        """
        Insert or update a key–value pair.

        Args:
            key: Key to insert/update.
            value: Value to store.
        """
        idx = self._index(key)
        bucket = self._buckets[idx]
        for i, (k, _) in enumerate(bucket):
            if k == key:
                bucket[i] = (key, value)
                return
        bucket.append((key, value))
        self._size += 1


# ---------------------------------------------------------------------------
# Append-only persistent store
# ---------------------------------------------------------------------------
class AppendOnlyKV:
    """Persistent key–value store using an append-only log file."""

    def __init__(self, path: str) -> None:
        """
        Initialize store, open the log file, and rebuild the in-memory index.

        Args:
            path: Path to the log file (e.g., 'data.db').
        """
        self.path = path
        self._fh = None
        self._index = SimpleHashMap()
        self._open_and_replay()

    def __enter__(self) -> "AppendOnlyKV":
        """Enable use as a context manager."""
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        """Ensure file is closed on context manager exit."""
        self.close()

    def _open_and_replay(self) -> None:
        """
        Open the log file and rebuild the latest values by replaying all SET lines.

        Raises:
            DataFileOpenError: If the file cannot be opened.
        """
        try:
            fh = open(self.path, "a+", encoding="utf-8")
        except OSError as e:
            raise DataFileOpenError(f"Failed to open {self.path}: {e}") from e

        fh.seek(0)
        for line in fh:
            parts = line.strip().split()
            if len(parts) == 3 and parts[0] == "SET":
                key, val = parts[1], parts[2]
                if valid_token(key) and valid_token(val):
                    self._index.set(key, val)

        fh.seek(0, os.SEEK_END)
        self._fh = fh

    def set(self, key: str, val: str) -> None:
        """
        Append a new SET record and update the in-memory index.

        Args:
            key: Key to store.
            val: Value to store.

        Raises:
            DataFileWriteError: If writing/flush/fsync fails.
            RuntimeError: If called after the file handle becomes unavailable.
        """
        if not self._fh:
            raise RuntimeError("data file is not open")

        try:
            self._fh.write(f"SET {key} {val}\n")
            self._fh.flush()
            os.fsync(self._fh.fileno())
        except (OSError, ValueError) as e:
            raise DataFileWriteError(f"Failed to write record: {e}") from e

        self._index.set(key, val)

    def get(self, key: str) -> Optional[str]:
        """
        Look up the value for a key.

        Returns:
            The value if present, otherwise None.
        """
        return self._index.get(key)

    def close(self) -> None:
        """
        Flush and close the log file.

        Raises:
            DataFileCloseError: If flushing/closing fails.
        """
        if not self._fh:
            return
        try:
            self._fh.flush()
            os.fsync(self._fh.fileno())
            self._fh.close()
        except (OSError, ValueError) as e:
            raise DataFileCloseError(f"Failed to close file: {e}") from e
        finally:
            self._fh = None


# ---------------------------------------------------------------------------
# CLI interface
# ---------------------------------------------------------------------------
def _err() -> None:
    """Print the standardized error token used by the grader."""
    print("ERR")
    sys.stdout.flush()


def main() -> None:
    """Read commands from STDIN and execute them against the store."""
    try:
        with AppendOnlyKV(DATA_FILE) as db:
            for raw in sys.stdin:
                line = raw.strip()
                if not line:
                    continue

                parts = line.split()
                cmd = parts[0].upper()

                if cmd == "EXIT":
                    break

                if cmd == "SET":
                    if len(parts) == 3 and valid_token(parts[1]) and valid_token(parts[2]):
                        try:
                            db.set(parts[1], parts[2])
                        except (DataFileWriteError, RuntimeError):
                            _err()
                    else:
                        _err()
                    continue

                if cmd == "GET":
                    if len(parts) == 2 and valid_token(parts[1]):
                        val = db.get(parts[1])
                        print("" if val is None else val)
                        sys.stdout.flush()
                    else:
                        _err()
                    continue

                _err()  # unknown command

    except (DataFileOpenError, DataFileCloseError, KVError, RuntimeError, ValueError):
        # Catch explicit, known errors – no bare 'except' used.
        _err()


if __name__ == "__main__":
    main()
