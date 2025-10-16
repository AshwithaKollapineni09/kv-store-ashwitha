#!/usr/bin/env python3
"""
Project 1 — Persistent Key–Value Store (Append-only)
Author: Ashwitha

Commands:
  SET <key> <value>
  GET <key>
  EXIT

Behavior:
- Append-only log written to data.db
- fsync after each write for durability
- On startup, replay the log to rebuild the in-memory index
- In-memory index uses a simple list-based structure (no dict)
- GET prints value or an empty line if not found
- Invalid commands print 'ERR'
"""

import os
import sys
from typing import List, Optional, Tuple

DATA_FILE = "data.db"


def valid_token(tok: str) -> bool:
    """Token must be non-empty and contain no whitespace."""
    return bool(tok) and not any(c.isspace() for c in tok)


# -------------------- minimal hash map (no dict) --------------------
class SimpleHashMap:
    """Very small str->str hash map with separate chaining."""

    __slots__ = ("_buckets", "_size")

    def __init__(self, capacity: int = 1024) -> None:
        cap = 1
        while cap < capacity:
            cap <<= 1
        self._buckets: List[List[Tuple[str, str]]] = [[] for _ in range(cap)]
        self._size = 0

    def _idx(self, key: str) -> int:
        return hash(key) & (len(self._buckets) - 1)

    def get(self, key: str) -> Optional[str]:
        bucket = self._buckets[self._idx(key)]
        for k, v in bucket:
            if k == key:
                return v
        return None

    def set(self, key: str, val: str) -> None:
        idx = self._idx(key)
        bucket = self._buckets[idx]
        for i, (k, _) in enumerate(bucket):
            if k == key:
                bucket[i] = (key, val)
                return
        bucket.append((key, val))
        self._size += 1


# -------------------- append-only KV store --------------------
class AppendOnlyKV:
    def __init__(self, path: str) -> None:
        self.path = path
        self._fh = None
        self._index = SimpleHashMap()
        self._open_and_replay()

    def _open_and_replay(self) -> None:
        # create if not exists; then replay
        fh = open(self.path, "a+", encoding="utf-8")
        fh.flush()
        os.fsync(fh.fileno())
        fh.seek(0)
        for line in fh:
            parts = line.strip().split()
            if len(parts) == 3 and parts[0] == "SET":
                k, v = parts[1], parts[2]
                if valid_token(k) and valid_token(v):
                    self._index.set(k, v)
        fh.seek(0, os.SEEK_END)
        self._fh = fh

    def set(self, key: str, val: str) -> None:
        rec = f"SET {key} {val}\n"
        self._fh.write(rec)
        self._fh.flush()
        os.fsync(self._fh.fileno())
        self._index.set(key, val)

    def get(self, key: str) -> Optional[str]:
        return self._index.get(key)

    def close(self) -> None:
        try:
            if self._fh:
                self._fh.flush()
                os.fsync(self._fh.fileno())
                self._fh.close()
        finally:
            self._fh = None


def _err() -> None:
    print("ERR")
    sys.stdout.flush()


def main() -> None:
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
                    db.set(parts[1], parts[2])
                else:
                    _err()

            elif cmd == "GET":
                if len(parts) == 2 and valid_token(parts[1]):
                    v = db.get(parts[1])
                    # print value if found, otherwise empty line
                    print("" if v is None else v)
                    sys.stdout.flush()
                else:
                    _err()
            else:
                _err()
    finally:
        db.close()


if __name__ == "__main__":
    main()
