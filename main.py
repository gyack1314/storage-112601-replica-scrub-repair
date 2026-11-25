#!/usr/bin/env python3
"""Conventional root entry point; equivalent to ``python3 -m replica_store``."""

from replica_store.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
