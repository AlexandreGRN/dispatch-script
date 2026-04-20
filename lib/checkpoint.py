"""Checkpoint + incremental CSV writer — lets us resume after a crash."""

from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path

from .schema import CSV_COLUMNS

PROCESSED_HEADER = ["code", "status", "at"]


def load_processed(path: Path) -> dict[str, str]:
    """Return {code: status} for already-processed orders."""
    if not path.exists():
        return {}
    out: dict[str, str] = {}
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = (row.get("code") or "").strip()
            if code:
                out[code] = (row.get("status") or "").strip()
    return out


def mark_processed(path: Path, code: str, status: str) -> None:
    """Append one line to processed.csv (creates file with header if missing)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    new_file = not path.exists()
    with open(path, "a", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        if new_file:
            writer.writerow(PROCESSED_HEADER)
        writer.writerow([code, status, datetime.now().isoformat(timespec="seconds")])


def append_order_row(path: Path, row: dict[str, str]) -> None:
    """Append one extracted order to orders.csv (creates file with header if missing)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    new_file = not path.exists()
    with open(path, "a", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        if new_file:
            writer.writeheader()
        writer.writerow({k: row.get(k, "") for k in CSV_COLUMNS})
