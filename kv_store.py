#!/usr/bin/env python3
# Build Your Own Database - Project 1: Persistent Key-Value Store
# Author: Ashwitha
#
# Implements a simple append-only key-value store.
# - SET <key> <value>: stores a key/value pair
# - GET <key>: retrieves the most recent value
# - EXIT: closes the program
#
# Features:
# * Append-only log (data.db) for durability
# * fsync() after each write for crash safety
# * On startup, replays the log to rebuild state
# * Custom in-memory index using arrays (no dict/map)
# * Last write wins

import os
import sys

DATA_FILE = "data.db"

def valid_token(tok: str) -> bool:
    """Check if token has no spaces and is non-empty."""
    return bool(tok) and not any(c.isspace() for c in tok)

class AppendOnlyKV:
    def __init__(self, path: str):
        self.path = path
        self.keys = []
        self.vals = []
        # open file for append and replay existing data
        self._open_and_replay()

    def _open_and_replay(self):
        # create file if not exists, then read all lines
        self.fh = open(self.path, "a+", encoding="utf-8")
        self.fh.flush()
        os.fsync(self.fh.fileno())
        self.fh.seek(0)
        for line in self.fh:
            parts = line.strip().split(" ")
            if len(parts) == 3 and parts[0] == "SET":
                key, val = parts[1], parts[2]
                if valid_token(key) and valid_token(val):
                    self.keys.append(key)
                    self.vals.append(val)
        # move to end for new appends
        self.fh.seek(0, os.SEEK_END)

    def set(self, key: str, val: str):
        """Append to log and update in-memory arrays."""
        record = f"SET {key} {val}\n"
        self.fh.write(record)
        self.fh.flush()
        os.fsync(self.fh.fileno())
        self.keys.append(key)
        self.vals.append(val)

    def get(self, key: str):
        """Return most recent value or None if not found."""
        for i in range(len(self.keys) - 1, -1, -1):
            if self.keys[i] == key:
                return self.vals[i]
        return None

    def close(self):
        """Close the log safely."""
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
            parts = line.split(" ")
            cmd = parts[0].upper()

            if cmd == "EXIT":
                break

            elif cmd == "SET":
                if len(parts) == 3 and valid_token(parts[1]) and valid_token(parts[2]):
                    db.set(parts[1], parts[2])
                else:
                    print("ERR")
                    sys.stdout.flush()
            elif cmd == "GET":
               if len(parts) == 2 and valid_token(parts[1]):
                   val = db.get(parts[1])
                if val is None:
                      print("")       # print blank line if key not found
           else:
            print(val)
        sys.stdout.flush()
    else:
        print("ERR")
        sys.stdout.flush()

    finally:
        db.close()

if __name__ == "__main__":
    main()
