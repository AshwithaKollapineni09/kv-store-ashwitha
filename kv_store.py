#!/usr/bin/env python3
"""
Persistent Key-Value Store (Project 1)
Author: Ashwitha
"""

import os
import sys
from typing import List, Optional

DATA_FILE = "data.db"


def valid_token(tok: str) -> bool:
    return bool(tok) and not any(c.isspace() for c in tok)


class AppendOnlyKV:
    def __init__(self, path: str):
        self.path = path
        self.keys: List[str] = []
        self.vals: List[str] = []
        self._open_and_replay()

    def _open_and_replay(self):
        # Ensure the file exists before reading
        open(self.path, "a", encoding="utf-8").close()

        with open(self.path, "r", encoding="utf-8") as f:
            for line in f:
                parts = line.strip().split(" ")
                if len(parts) == 3 and parts[0] == "SET":
                    key, val = parts[1], parts[2]
                    if valid_token(key) and valid_token(val):
                        self.keys.append(key)
                        self.vals.append(val)

        # open for appending
        self.fh = open(self.path, "a", encoding="utf-8")

    def set(self, key: str, val: str):
        record = f"SET {key} {val}\n"
        self.fh.write(record)
        self.fh.flush()
        os.fsync(self.fh.fileno())
        self.keys.append(key)
        self.vals.append(val)

    def get(self, key: str) -> Optional[str]:
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
            parts = line.split(" ")
            cmd = parts[0].upper()

            if cmd == "EXIT":
                break
            elif cmd == "SET":
                if len(parts) == 3 and valid_token(parts[1]) and valid_token(parts[2]):
                    db.set(parts[1], parts[2])
                else:
                    print("ERR", flush=True)
            elif cmd == "GET":
                if len(parts) == 2 and valid_token(parts[1]):
                    val = db.get(parts[1])
                    #  FIX: print blank line if key not found
                    if val is None:
                        print("", flush=True)
                    else:
                        print(val, flush=True)
                else:
                    print("ERR", flush=True)
            else:
                print("ERR", flush=True)
    finally:
        db.close()


if __name__ == "__main__":
    main()
