#!/usr/bin/env python3
"""
Project 1 — Persistent Key–Value Store (Append-only)
Author: Ashwitha Kollapineni

Implements a minimal persistent key–value store using an append-only log.
Commands:
  SET <key> <value>   -> store key/value
  GET <key>           -> print value or blank if not found
  EXIT                -> quit
"""

from __future__ import annotations
import os
import sys
from typing import List, Optional, Tuple

DATA_FILE = "data.db"


def valid_token(tok: str) -> bool:
    """Return True if token is non-empty and contains no whitespace."""
    return bool(tok) and not any(c.isspace() for c in tok)


# ---------------------------------------------------------------------------
# Lightweight hash table (no dict)
# ---------------------------------------------------------------------------
class SimpleHashMap:
    """Minimal str→str hash map with separate chaining (no dict)."""

    __slots__ = ("_buckets", "_size")

    def __init__(self, initial_capacity: int = 1024) -> None:
        """
        Initialize buckets.

        Args:
            initial_capacity (int): Approximate number of buckets to create.
        """
        cap = 1
        while cap < initial_capacity:
            cap <<= 1
        self._buckets: List[List[Tuple[str, str]]] = [[] for _ in range(cap)]
        self._size = 0

    def _index(self, key: str) -> int:
        """Compute bucket index for key using bitmask (faster than modulo)."""
        return hash(key) & (len(self._buckets) - 1)

    def get(self, key: str) -> Optional[str]:
        """Retrieve the value for a key or None if not found."""
        bucket = self._buckets[self._index(key)]
        for k, v in bucket:
            if k == key:
                return v
        return None

    def set(self, key: str, value: str) -> None:
        """Insert or update a key-value pair."""
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
    """Persistent key-value store using append-only file."""

    def __init__(self, path: str) -> None:
        """Initialize store and rebuild index from log."""
        self.path = path
        self._fh = None
        self._index = SimpleHashMap()
        self._open_and_replay()

    def __enter__(self) -> "AppendOnlyKV":
        """Enable use as context manager."""
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        """Ensure file closes properly."""
        self.close()

    def _open_and_replay(self) -> None:
        """Open file and rebuild index by replaying all previous SETs."""
        try:
            fh = open(self.path, "a+", encoding="utf-8")
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Cannot create or open {self.path}: {e}")
        except PermissionError as e:
            raise PermissionError(f"Permission denied for {self.path}: {e}")
        except OSError as e:
            raise IOError(f"File operation failed: {e}")

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
        """Append a new SET operation to the file."""
        if not self._fh:
            raise RuntimeError("File handle is not open.")
        try:
            self._fh.write(f"SET {key} {val}\n")
            self._fh.flush()
            os.fsync(self._fh.fileno())
            self._index.set(key, val)
        except ValueError as e:
            raise ValueError(f"Invalid write value: {e}")
        except OSError as e:
            raise IOError(f"File write failed: {e}")

    def get(self, key: str) -> Optional[str]:
        """Return value for a key or None if not found."""
        return self._index.get(key)

    def close(self) -> None:
        """Flush and close the file safely."""
        if not self._fh:
            return
        try:
            self._fh.flush()
            os.fsync(self._fh.fileno())
            self._fh.close()
        except OSError as e:
            raise IOError(f"Failed to close file: {e}")
        finally:
            self._fh = None


# ---------------------------------------------------------------------------
# CLI interface
# ---------------------------------------------------------------------------
def _print_err() -> None:
    """Prints a standard ERR message to stdout."""
    print("ERR")
    sys.stdout.flush()


def main() -> None:
    """Main command loop."""
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

                # Handle SET <key> <value>
                if cmd == "SET":
                    if len(parts) == 3 and valid_token(parts[1]) and valid_token(parts[2]):
                        try:
                            db.set(parts[1], parts[2])
                        except (IOError, ValueError):
                            _print_err()
                    else:
                        _print_err()
                    continue

                # Handle GET <key>
                if cmd == "GET":
                    if len(parts) == 2 and valid_token(parts[1]):
                        val = db.get(parts[1])
                        print("" if val is None else val)
                        sys.stdout.flush()
                    else:
                        _print_err()
                    continue

                _print_err()

    except (FileNotFoundError, PermissionError, IOError, RuntimeError):
        _print_err()


if __name__ == "__main__":
    main()
