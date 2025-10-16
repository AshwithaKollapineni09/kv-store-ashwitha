#!/usr/bin/env python3
"""
Project 1 — Persistent Key–Value Store (Append-only)
Author: Ashwitha

This program implements a minimal persistent key–value store using an
append-only log file for durability. Keys and values are strings with no
whitespace. Commands are read from STDIN and results printed to STDOUT.

Supported commands:
    SET <key> <value>   - Store a key/value pair (persistent)
    GET <key>           - Retrieve value, prints blank if not found
    EXIT                - Gracefully exit
"""

from __future__ import annotations
import os
import sys
from typing import List, Optional, Iterable, Tuple

DATA_FILE = "data.db"


def valid_token(tok: str) -> bool:
    """Check whether a token is non-empty and contains no whitespace."""
    return bool(tok) and not any(c.isspace() for c in tok)


# --------------------------------------------------------------------------- #
# Custom Hash Table (no Python dicts)
# --------------------------------------------------------------------------- #
class SimpleHashMap:
    """A lightweight hash map for string keys/values using separate chaining."""

    __slots__ = ("_buckets", "_size")

    def __init__(self, initial_capacity: int = 1024) -> None:
        """
        Initialize hash table with the given capacity (rounded to a power of 2).
        """
        cap = 1
        while cap < initial_capacity:
            cap <<= 1
        self._buckets: List[List[Tuple[str, str]]] = [[] for _ in range(cap)]
        self._size: int = 0

    def _index(self, key: str) -> int:
        """Compute hash bucket index for a key."""
        return hash(key) & (len(self._buckets) - 1)

    def get(self, key: str) -> Optional[str]:
        """Return value if key exists, else None."""
        bucket = self._buckets[self._index(key)]
        for k, v in bucket:
            if k == key:
                return v
        return None

    def set(self, key: str, value: str) -> None:
        """Insert or update key/value pair."""
        idx = self._index(key)
        bucket = self._buckets[idx]
        for i, (k, _) in enumerate(bucket):
            if k == key:
                bucket[i] = (key, value)
                return
        bucket.append((key, value))
        self._size += 1

    def __len__(self) -> int:
        """Return number of items stored."""
        return self._size


# --------------------------------------------------------------------------- #
# Append-only key-value store
# --------------------------------------------------------------------------- #
class AppendOnlyKV:
    """Append-only persistent key–value store backed by a single log file."""

    def __init__(self, path: str) -> None:
        """
        Initialize the store and rebuild index by replaying the append-only log.
        """
        self.path = path
        self._fh: Optional[object] = None
        self._index = SimpleHashMap()
        self._open_and_replay()

    def __enter__(self) -> "AppendOnlyKV":
        """Enable use as a context manager (with ... as ...)."""
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        """Ensure the file is safely closed upon exit."""
        self.close()

    def _open_and_replay(self) -> None:
        """Open file for read/append and replay existing log entries."""
        fh = open(self.path, "a+", encoding="utf-8")
        fh.flush()
        os.fsync(fh.fileno())

        fh.seek(0)
        for raw in fh:
            self._apply_log_line(raw)

        fh.seek(0, os.SEEK_END)
        self._fh = fh

    def _apply_log_line(self, raw: str) -> None:
        """Apply a single log line to the in-memory hash map."""
        parts = raw.strip().split()
        if len(parts) == 3 and parts[0] == "SET":
            key, val = parts[1], parts[2]
            if valid_token(key) and valid_token(val):
                self._index.set(key, val)

    def set(self, key: str, val: str) -> None:
        """Write a new SET record to disk and update the in-memory index."""
        if self._fh is None:
            raise RuntimeError("File not open")
        rec = f"SET {key} {val}\n"
        self._fh.write(rec)
        self._fh.flush()
        os.fsync(self._fh.fileno())
        self._index.set(key, val)

    def get(self, key: str) -> Optional[str]:
        """Retrieve latest value for key, or None if it does not exist."""
        return self._index.get(key)

    def close(self) -> None:
        """Flush and close file, printing error if it fails."""
        if not self._fh:
            return
        try:
            self._fh.flush()
            os.fsync(self._fh.fileno())
            self._fh.close()
        except Exception as e:
            print("ERR: close failed", file=sys.stderr)
            print(str(e), file=sys.stderr)
        finally:
            self._fh = None


# --------------------------------------------------------------------------- #
# Command-line interface
# --------------------------------------------------------------------------- #
def _print_err() -> None:
    """Print standardized error response."""
    print("ERR")
    sys.stdout.flush()


def main() -> None:
    """Main REPL: read commands from STDIN and process them."""
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
                    except Exception:
                        _print_err()
                else:
                    _print_err()
                continue

            if cmd == "GET":
                if len(parts) == 2 and valid_token(parts[1]):
                    val = db.get(parts[1])
                    print("" if val is None else val)
                    sys.stdout.flush()
                else:
                    _print_err()
                continue

            _print_err()


if __name__ == "__main__":
    main()
