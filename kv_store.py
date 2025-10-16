#!/usr/bin/env python3
"""
Persistent Key–Value Store (Project 1)
Author: Ashwitha Kollapineni

A simple append-only key–value store.
- Uses 'data.db' as a persistent log file.
- Replays the file on startup to rebuild the in-memory index.
- Supports SET and GET commands via standard input.
"""

import os
import sys
from typing import List, Optional, Tuple

DATA_FILE = "data.db"


def valid_token(tok: str) -> bool:
    """Return True if token is non-empty and contains no whitespace."""
    return bool(tok) and not any(c.isspace() for c in tok)


# --------------------------------------------------------------------
# Basic in-memory map (without using dict, per project constraints)
# --------------------------------------------------------------------
class SimpleHashMap:
    """A lightweight hash map using separate chaining (no built-in dict)."""

    __slots__ = ("_buckets", "_size")

    def __init__(self, capacity: int = 1024) -> None:
        """Initialize an empty map with fixed bucket capacity."""
        cap = 1
        while cap < capacity:
            cap <<= 1
        self._buckets: List[List[Tuple[str, str]]] = [[] for _ in range(cap)]
        self._size = 0

    def _idx(self, key: str) -> int:
        """Compute index of a key in the bucket array."""
        return hash(key) & (len(self._buckets) - 1)

    def get(self, key: str) -> Optional[str]:
        """Return value for key if exists, otherwise None."""
        for k, v in self._buckets[self._idx(key)]:
            if k == key:
                return v
        return None

    def set(self, key: str, val: str) -> None:
        """Insert or overwrite a key–value pair."""
        bucket = self._buckets[self._idx(key)]
        for i, (k, _) in enumerate(bucket):
            if k == key:
                bucket[i] = (key, val)
                return
        bucket.append((key, val))
        self._size += 1


# --------------------------------------------------------------------
# Persistent key-value store
# --------------------------------------------------------------------
class AppendOnlyKV:
    """Implements append-only persistence for the key–value store."""

    def __init__(self, path: str) -> None:
        """Open data file and rebuild index from previous entries."""
        self.path = path
        self._fh = None
        self._index = SimpleHashMap()
        self._open_and_replay()

    def _open_and_replay(self) -> None:
        """Open file, replay prior operations, and restore latest state."""
        try:
            fh = open(self.path, "a+", encoding="utf-8")
        except OSError as e:
            raise RuntimeError(f"Unable to open {self.path}: {e}")
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
        """Append a new SET operation and update in-memory index."""
        self._fh.write(f"SET {key} {val}\n")
        self._fh.flush()
        os.fsync(self._fh.fileno())
        self._index.set(key, val)

    def get(self, key: str) -> Optional[str]:
        """Retrieve the current value for a key, or None if absent."""
        return self._index.get(key)

    def close(self) -> None:
        """Safely close the data file."""
        try:
            if self._fh:
                self._fh.flush()
                os.fsync(self._fh.fileno())
                self._fh.close()
        except OSError:
            # Only log to stderr, don’t crash program
            print("ERR: file close failed", file=sys.stderr)
        finally:
            self._fh = None


# --------------------------------------------------------------------
# CLI interface
# --------------------------------------------------------------------
def _err() -> None:
    """Print a standardized error message."""
    print("ERR")
    sys.stdout.flush()


def main() -> None:
    """Main interactive loop for SET/GET/EXIT commands."""
    db = AppendOnlyKV(DATA_FILE)
    try:
        for raw in sys.stdin:
            line = raw.strip()
            if not line:
                continue
            parts = line.split()
            cmd = parts[0].upper()

            if cmd == "EXIT":
                break
            elif cmd == "SET":
                if len(parts) == 3 and valid_token(parts[1]) and valid_token(parts[2]):
                    try:
                        db.set(parts[1], parts[2])
                    except Exception:
                        _err()
                else:
                    _err()
            elif cmd == "GET":
                if len(parts) == 2 and valid_token(parts[1]):
                    val = db.get(parts[1])
                    print("" if val is None else val)
                    sys.stdout.flush()
                else:
                    _err()
            else:
                _err()
    finally:
        db.close()


if __name__ == "__main__":
    main()
