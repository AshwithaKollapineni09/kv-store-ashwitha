#!/usr/bin/env python3
# Build Your Own Database - Project 1: Persistent Key-Value Store
# Author: Ashwitha Kollapineni
#
# Commands:
#   SET <key> <value>
#   GET <key>
#   EXIT
#
# Features:
# * Append-only log (data.db) and fsync after each write
# * Replays the log on startup to rebuild in-memory state
# * Custom in-memory index using arrays (no dict/map)
# * Last write wins semantics

import os
import sys

DATA_FILE = "data.db"


def valid_token(tok: str) -> bool:
    """Key/value must be non-empty and contain no whitespace."""
    return bool(tok) and not any(c.isspace() for c in tok)


class AppendOnlyKV:
    """Simple append-only persistent key–value store."""

    def __init__(self, path: str):
        self.path = path
        self.keys = []
        self.vals = []
        self._open_and_replay()

    def _open_and_replay(self):
        # Open for append, create if not exists, then replay
        self.fh = open(self.path, "a+", encoding="utf-8")
        self.fh.flush()
        os.fsync(self.fh.fileno())
        self.fh.seek(0)
        for line in self.fh:
            parts = line.strip().split()  # <-- handle arbitrary whitespace
            if len(parts) == 3 and parts[0] == "SET":
                key, val = parts[1], parts[2]
                if valid_token(key) and valid_token(val):
                    self.keys.append(key)
                    self.vals.append(val)
        self.fh.seek(0, os.SEEK_END)

    def set(self, key: str, val: str):
        record = f"SET {key} {val}\n"
        self.fh.write(record)
        self.fh.flush()
        os.fsync(self.fh.fileno())
        self.keys.append(key)
        self.vals.append(val)

    def get(self, key: str):
        # Scan backwards: last write wins
        for i in range(len(self.keys) - 1, -1, -1):
            if self.keys[i] == key:
                return self.vals[i]
        return None

    def close(self):
        try:
            self.fh.flush()
            os.fsync(self.fh.fileno())
        except Exception:
            pass
        self.fh.close()


def main():
    db = AppendOnlyKV(DATA_FILE)
    try:
        for raw in sys.stdin:
            line = raw.strip()
            if not line:
                continue
            parts = line.split()  # <-- handle arbitrary whitespace
            cmd = parts[0].upper()

            if cmd == "EXIT":
                break

            elif cmd == "SET":
                if len(parts) == 3 and valid_token(parts[1]) and valid_token(parts[2]):
                    db.set(parts[1], parts[2])
                else:
                    print("ERR", end="")  # no trailing newline to match "empty/err" style
                    sys.stdout.flush()

            elif cmd == "GET":
                if len(parts) == 2 and valid_token(parts[1]):
                    val = db.get(parts[1])
                    if val is not None:
                        print(val)  # found: print value + newline
                        sys.stdout.flush()
                    else:
                        # Not found: emit truly empty output (no newline)
                        sys.stdout.flush()
                else:
                    print("ERR", end="")
                    sys.stdout.flush()

            else:
                print("ERR", end="")
                sys.stdout.flush()
    finally:
        db.close()


if __name__ == "__main__":
    main()
