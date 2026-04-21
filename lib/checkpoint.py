"""Checkpoint + incremental CSV writer — lets us resume after a crash."""

from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path

from .schema import CSV_COLUMNS

PROCESSED_HEADER = ["code", "status", "at"]


def migrate_csv_header(path: Path) -> bool:
    """If orders.csv header drifts from CSV_COLUMNS (e.g. new columns added),
    rewrite the file with the current header, filling empty strings for new columns.
    Returns True if a migration happened, False if no change needed or file absent."""
    if not path.exists():
        return False
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        existing_cols = list(reader.fieldnames or [])
        if existing_cols == CSV_COLUMNS:
            return False
        rows = list(reader)
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in CSV_COLUMNS})
    return True


def remove_codes(path: Path, codes: set[str], code_col: str) -> int:
    """Remove all rows in a CSV whose `code_col` value is in `codes`.
    Returns number of removed rows. Preserves the existing header."""
    if not path.exists() or not codes:
        return 0
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        kept = [r for r in reader if (r.get(code_col) or "").strip() not in codes]
        removed = 0  # computed below
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        total_before = sum(1 for _ in csv.DictReader(f))
    removed = total_before - len(kept)
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in kept:
            writer.writerow(r)
    return removed


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
