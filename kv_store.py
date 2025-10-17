#!/usr/bin/env python3
# Minimal persistent key–value store (append-only, no built-in dicts/maps).

import os
import sys
from typing import List, Optional, Tuple

DATA_FILE = "data.db"


def valid_token(s: str) -> bool:
    """Keys/values are non-empty and contain no whitespace."""
    return bool(s) and not any(c.isspace() for c in s)


class SimpleHashMap:
    """Tiny str->str hash map using separate chaining (no dict)."""

    __slots__ = ("_b", "_n")

    def __init__(self, cap: int = 1024) -> None:
        # keep bucket count a power of two (fast masking)
        m = 1
        while m < cap:
            m <<= 1
        self._b: List[List[Tuple[str, str]]] = [[] for _ in range(m)]
        self._n = 0

    def _idx(self, k: str) -> int:
        return hash(k) & (len(self._b) - 1)

    def get(self, k: str) -> Optional[str]:
        bucket = self._b[self._idx(k)]
        for kk, vv in bucket:
            if kk == k:
                return vv
        return None

    def set(self, k: str, v: str) -> None:
        bucket = self._b[self._idx(k)]
        for i, (kk, _) in enumerate(bucket):
            if kk == k:
                bucket[i] = (k, v)
                return
        bucket.append((k, v))
        self._n += 1


class AppendOnlyKV:
    """Append-only log with in-memory index; last write wins."""

    def __init__(self, path: str) -> None:
        self.path = path
        self._fh = None
        self._idx = SimpleHashMap()
        self._open_and_replay()

    def _open_and_replay(self) -> None:
        # Create if missing, then read all SET lines to rebuild state.
        try:
            fh = open(self.path, "a+", encoding="utf-8")
        except OSError as e:
            raise RuntimeError(f"open failed: {e}")
        fh.seek(0)
        for line in fh:
            parts = line.strip().split()
            if len(parts) == 3 and parts[0] == "SET":
                k, v = parts[1], parts[2]
                if valid_token(k) and valid_token(v):
                    self._idx.set(k, v)
        fh.seek(0, os.SEEK_END)
        self._fh = fh

    def set(self, k: str, v: str) -> None:
        if not self._fh:
            raise RuntimeError("not open")
        try:
            self._fh.write(f"SET {k} {v}\n")
            self._fh.flush()
            os.fsync(self._fh.fileno())
        except OSError as e:
            raise RuntimeError(f"write failed: {e}")
        self._idx.set(k, v)

    def get(self, k: str) -> Optional[str]:
        return self._idx.get(k)

    def close(self) -> None:
        if not self._fh:
            return
        try:
            self._fh.flush()
            os.fsync(self._fh.fileno())
            self._fh.close()
        finally:
            self._fh = None


def _err() -> None:
    print("ERR")
    sys.stdout.flush()


def main() -> None:
    try:
        db = AppendOnlyKV(DATA_FILE)
    except RuntimeError:
        _err()
        return

    try:
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
                    except RuntimeError:
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
    finally:
        try:
            db.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
