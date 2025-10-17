#!/usr/bin/env python3
"""
Project 1 — Persistent Key–Value Store (Append-only)
Author: Ashwitha Kollapineni

A minimal persistent key–value store backed by an append-only log file.

Design highlights
-----------------
* **Durability:** Each `SET` is appended to `data.db`, flushed, and fsynced.
* **Recovery:** On startup, the log is replayed to rebuild in-memory state.
* **Index:** A tiny hash map is implemented *without* using Python dicts/maps.
* **CLI:** Reads commands from STDIN: `SET <key> <value>`, `GET <key>`, `EXIT`.

This module is intentionally small and self-contained to make the data-path and
persistence story easy to follow for a first project.
"""

from __future__ import annotations

import os
import sys
from typing import List, Optional, Tuple, Type, Optional as Opt

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
    """
    Check whether a token is acceptable for keys/values in this project.

    Args:
        tok: A string to validate.

    Returns:
        True if `tok` is non-empty and contains **no whitespace**; False otherwise.

    Notes:
        This project purposely keeps the grammar simple: keys and values are
        single, space-free tokens. That matches the black-box tests’ expectations.
    """
    return bool(tok) and not any(c.isspace() for c in tok)


# ---------------------------------------------------------------------------
# Lightweight hash table (no built-in dict)
# ---------------------------------------------------------------------------
class SimpleHashMap:
    """
    Minimal `str -> str` hash map using separate chaining.

    We avoid Python’s built-in `dict` on purpose to meet the “no dict/map” rule.
    Buckets are lists of `(key, value)` pairs. The number of buckets is a power
    of two so we can use a bit mask instead of modulo for fast indexing.

    Invariants:
        * Keys are unique; calling `set()` with the same key overwrites the value.
        * All stored keys/values are strings.

    Complexity:
        * `get` — average O(1); worst-case O(n) in a single bucket
        * `set` — average O(1); replaces value if key exists, otherwise appends
    """

    __slots__ = ("_buckets", "_size")

    def __init__(self, initial_capacity: int = 1024) -> None:
        """
        Create an empty table with at least `initial_capacity` buckets.

        Args:
            initial_capacity: Suggested number of buckets. It will be rounded
                up to the next power of two for efficient masking.
        """
        cap = 1
        while cap < initial_capacity:
            cap <<= 1  # keep capacity as a power of two for fast masking
        self._buckets: List[List[Tuple[str, str]]] = [[] for _ in range(cap)]
        self._size = 0

    def _index(self, key: str) -> int:
        """
        Compute the bucket index for `key`.

        Args:
            key: Key to hash.

        Returns:
            An integer index in `[0, len(_buckets))`.

        Notes:
            Uses `hash(key) & (len(_buckets) - 1)` which is faster than modulo
            when the bucket count is a power of two.
        """
        return hash(key) & (len(self._buckets) - 1)

    def get(self, key: str) -> Optional[str]:
        """
        Retrieve the value for `key`.

        Args:
            key: Key to look up.

        Returns:
            The associated value if present; otherwise `None`.

        Examples:
            >>> m = SimpleHashMap(4)
            >>> m.set("x", "1")
            >>> m.get("x")
            '1'
            >>> m.get("y") is None
            True
        """
        bucket = self._buckets[self._index(key)]
        for k, v in bucket:
            if k == key:
                return v
        return None

    def set(self, key: str, value: str) -> None:
        """
        Insert a new `(key, value)` or overwrite an existing key.

        Args:
            key: Key to insert/update.
            value: Value to store.

        Returns:
            None. The table is updated in place.

        Examples:
            >>> m = SimpleHashMap(4)
            >>> m.set("a", "1")
            >>> m.set("a", "2")
            >>> m.get("a")
            '2'
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
    """
    Persistent key–value store using an append-only log file.

    Data path:
        * `SET k v` is appended to the log, flushed, and fsynced, then the in-memory
          index is updated.
        * `GET k` returns the most recent value recorded for `k` or an empty line
          if not present (per the project spec).

    Error model:
        * We use explicit exceptions (`DataFileOpenError`, `DataFileWriteError`,
          `DataFileCloseError`) to avoid bare `OSError` in the public surface.
    """

    def __init__(self, path: str) -> None:
        """
        Initialize store, open the log file, and rebuild the in-memory index.

        Args:
            path: Path to the log file (e.g., 'data.db').

        Raises:
            DataFileOpenError: If the file cannot be created or opened.
        """
        self.path = path
        self._fh = None
        self._index = SimpleHashMap()
        self._open_and_replay()

    def __enter__(self) -> "AppendOnlyKV":
        """
        Enter the context manager.

        Returns:
            The store itself, so callers can write `with AppendOnlyKV(...) as db: ...`.

        Notes:
            Nothing special is required here besides returning `self`; file
            ownership is handled in `_open_and_replay` and `close`.
        """
        return self

    def __exit__(
        self,
        exc_type: Opt[Type[BaseException]],
        exc: Opt[BaseException],
        tb: Opt[object],
    ) -> None:
        """
        Exit the context manager, ensuring the file is safely closed.

        Args:
            exc_type: Exception type if one was raised inside the `with` block.
            exc: The exception instance (or None).
            tb: Traceback object (or None).

        Notes:
            We always attempt to close; any close failure is raised as
            `DataFileCloseError` to be caught by the CLI.
        """
        self.close()

    def _open_and_replay(self) -> None:
        """
        Open the log file and rebuild the latest values by replaying all `SET` lines.

        Raises:
            DataFileOpenError: If the file cannot be opened for read/append.
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
        Append a new `SET` record and update the in-memory index.

        Args:
            key: Key to store.
            val: Value to store.

        Raises:
            RuntimeError: If the data file is not open.
            DataFileWriteError: If writing/flush/fsync fails.

        Postconditions:
            On success, the record is durable on disk and visible via `get`.
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

        Args:
            key: Key to retrieve.

        Returns:
            The latest stored value for `key` if present; otherwise `None`.

        Notes:
            The CLI prints an **empty line** for `None` to match the black-box tests.
        """
        return self._index.get(key)

    def close(self) -> None:
        """
        Flush and close the log file.

        Raises:
            DataFileCloseError: If flushing or closing the file handle fails.
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
    """
    Print the standardized error token used by the grader.

    Notes:
        The grader expects the literal string `ERR` on invalid input or on
        unexpected I/O failures. Keep this output stable.
    """
    print("ERR")
    sys.stdout.flush()


def main() -> None:
    """
    Read commands from STDIN and execute them against the store.

    Commands:
        SET <key> <value>  -> persist key/value (last write wins)
        GET <key>          -> print value or **blank line** if not found
        EXIT               -> terminate the process

    Error handling:
        * Invalid arity or invalid tokens -> print `ERR`
        * I/O errors are converted to specific exceptions and handled centrally
    """
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
                        # Gradebot wants an empty line when not found
                        print("" if val is None else val)
                        sys.stdout.flush()
                    else:
                        _err()
                    continue

                _err()  # unknown command

    except (DataFileOpenError, DataFileCloseError, KVError, RuntimeError, ValueError):
        # Catch explicit, known errors – no bare 'except'.
        _err()


if __name__ == "__main__":
    main()
