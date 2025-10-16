#!/usr/bin/env python3
"""
Project 1 — Persistent Key–Value Store (Append-only)
Author: Ashwitha Kollapineni

This program implements a minimal persistent key–value store using an
append-only log file for durability.

Commands (from STDIN):
  SET <key> <value>   → store key/value pair persistently
  GET <key>           → retrieve value, print blank if missing
  EXIT                → gracefully exit
"""

from __future__ import annotations
import os
import sys
from typing import List, Optional, Iterable, Tuple

DATA_FILE = "data.db"


def valid_token(tok: str) -> bool:
    """Check if a token is non-empty and contains no whitespace."""
    return bool(tok) and not any(c.isspace() for c in tok)


# ---------------------------------------------------------------------------
# Lightweight Hash Map (no dicts)
# ---------------------------------------------------------------------------
class SimpleHashMap:
    """A minimal string-to-string hash map using separate chaining."""

    __slots__ = ("_buckets", "_size")

    def __init__(self, initial_capacity: int = 1024) -> None:
        """
        Initialize the hash map.

        Args:
            initial_capacity: approximate starting number of buckets.
        """
        cap = 1
        while cap < initial_capacity:
            cap <<= 1
        self._buckets: List[List[Tuple[str, str]]] = [[] for _ in range(cap)]
        self._size = 0

    def _index(self, key: str) -> int:
        """Return the index bucket for the given key."""
        return hash(key) & (len(self._buckets) - 1)

    def get(self, key: str) -> Optional[str]:
        """
        Retrieve a value by key.

        Args:
            key: string key to retrieve.
        Returns:
            The corresponding value or None if not found.
        """
        bucket = self._buckets[self._index(key)]
        for k, v in bucket:
            if k == key:
                return v
        return None

    def set(self, key: str, value: str) -> None:
        """
        Insert or update a key-value pair.

        Args:
            key: key string.
            value: value string.
        """
        idx = self._index(key)
        bucket = self._buckets[idx]
        for i, (k, _) in enumerate(bucket):
            if k == key:
                bucket[i] = (key, value)
                return
        bucket.append((key, value))
        self._size += 1

    def __len__(self) -> int:
        """Return number of items in the map."""
        return self._size


# ---------------------------------------------------------------------------
# Append-only Persistent Store
# ---------------------------------------------------------------------------
class AppendOnlyKV:
    """Persistent key–value store backed by an append-only log file."""

    def __init__(self, path: str) -> None:
        """
        Initialize and rebuild index by replaying the log file.

        Args:
            path: path to the data file.
        """
        self.path = path
        self._fh: Optional[object] = None
        self._index = SimpleHashMap()
        self._open_and_replay()

    def __enter__(self) -> "AppendOnlyKV":
        """Enable context management (`with` syntax)."""
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        """Ensure file closure on exit."""
        self.close()

    def _open_and_replay(self) -> None:
        """Open file and rebuild in-memory index by replaying the log."""
        try:
            fh = open(self.path, "a+", encoding="utf-8")
        except OSError as e:
            print(f"ERR: failed to open {self.path} ({e})", file=sys.stderr)
            sys.exit(1)

        fh.flush()
        os.fsync(fh.fileno())

        fh.seek(0)
        for raw in fh:
            self._apply_log_line(raw)

        fh.seek(0, os.SEEK_END)
        self._fh = fh

    def _apply_log_line(self, raw: str) -> None:
        """Parse a single line and update index."""
        parts = raw.strip().split()
        if len(parts) == 3 and parts[0] == "SET":
            key, val = parts[1], parts[2]
            if valid_token(key) and valid_token(val):
                self._index.set(key, val)

    def set(self, key: str, val: str) -> None:
        """
        Store or overwrite a key-value pair.

        Args:
            key: key string.
            val: value string.

        Raises:
            OSError: if writing to the file fails.
        """
        if self._fh is None:
            raise RuntimeError("File not open")
        record = f"SET {key} {val}\n"
        try:
            self._fh.write(record)
            self._fh.flush()
            os.fsync(self._fh.fileno())
        except (OSError, ValueError) as e:
            print(f"ERR: failed to write record ({e})", file=sys.stderr)
            raise
        self._index.set(key, val)

    def get(self, key: str) -> Optional[str]:
        """
        Retrieve value for a key.

        Args:
            key: key string.
        Returns:
            The stored value, or None if not found.
        """
        return self._index.get(key)

    def close(self) -> None:
        """
        Flush and close the underlying file handle.

        Prints an error if closure fails (no silent exceptions).
        """
        if not self._fh:
            return
        try:
            self._fh.flush()
            os.fsync(self._fh.fileno())
            self._fh.close()
        except (OSError, ValueError) as e:
            print(f"ERR: close failed ({e})", file=sys.stderr)
        finally:
            self._fh = None


# ---------------------------------------------------------------------------
# CLI REPL
# ---------------------------------------------------------------------------
def _print_err() -> None:
    """Print standardized error message for invalid commands."""
    print("ERR")
    sys.stdout.flush()


def main() -> None:
    """Read commands from stdin and process them interactively."""
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
                    except OSError:
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
