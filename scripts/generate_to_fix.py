"""Scan orders.csv and produce output/to_fix.txt with codes that need re-extraction.

Triggers for re-extraction:
  - claude_comment contains [ILLISIBLE] or [ATTENTION]
  - champs_manquants contains "poor_screenshot"
  - status != "ok"
  - empty claude_comment (shouldn't happen but safety net)

Usage:
    python scripts/generate_to_fix.py               # default paths
    python scripts/generate_to_fix.py --strict      # also include [DEVINÉ]
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ORDERS = ROOT / "output" / "orders.csv"
DEFAULT_OUT = ROOT / "output" / "to_fix.txt"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--orders", default=str(DEFAULT_ORDERS))
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    ap.add_argument("--strict", action="store_true",
                    help="Also flag [DEVINÉ] rows (more aggressive).")
    args = ap.parse_args()

    orders = Path(args.orders)
    if not orders.exists():
        print(f"ERROR: {orders} not found")
        return 1

    codes_by_reason: dict[str, list[str]] = {
        "illisible": [],
        "attention": [],
        "poor_screenshot": [],
        "status_not_ok": [],
        "empty_comment": [],
        "devine": [],
    }

    with orders.open(encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

    flagged: dict[str, set[str]] = {}  # code → set of reasons
    for r in rows:
        code = (r.get("code_ordre") or "").strip()
        if not code:
            continue
        reasons: set[str] = set()
        comment = r.get("claude_comment") or ""
        missing = r.get("champs_manquants") or ""
        status = r.get("status") or ""

        if "[ILLISIBLE]" in comment:
            reasons.add("illisible")
        if "[ATTENTION]" in comment:
            reasons.add("attention")
        if args.strict and "[DEVINÉ]" in comment:
            reasons.add("devine")
        if "poor_screenshot" in missing:
            reasons.add("poor_screenshot")
        if status and status != "ok" and status != "partial_quality":
            reasons.add("status_not_ok")
        if not comment.strip():
            reasons.add("empty_comment")

        if reasons:
            flagged[code] = reasons
            for r_ in reasons:
                codes_by_reason[r_].append(code)

    codes = sorted(flagged.keys())
    Path(args.out).write_text(",".join(codes), encoding="utf-8")

    print(f"→ {len(codes)} codes flagged → {args.out}")
    print(f"  [ILLISIBLE]        {len(codes_by_reason['illisible']):4d}")
    print(f"  [ATTENTION]        {len(codes_by_reason['attention']):4d}")
    if args.strict:
        print(f"  [DEVINÉ] (strict)  {len(codes_by_reason['devine']):4d}")
    print(f"  poor_screenshot    {len(codes_by_reason['poor_screenshot']):4d}")
    print(f"  status != ok       {len(codes_by_reason['status_not_ok']):4d}")
    print(f"  empty comment      {len(codes_by_reason['empty_comment']):4d}")
    print()
    print(f"Re-run with:")
    print(f"  python 02_extract.py --codes-file {Path(args.out).relative_to(ROOT)} --overwrite")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
