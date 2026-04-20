"""
Main extraction loop: iterate through every order in the list, capture 8 screenshots,
send them to Claude Vision, append structured data to output/orders.csv.

Usage:
    python 02_extract.py                  # full run until VIA33MON
    python 02_extract.py --dry-run 3      # stop after 3 orders (for testing)
    python 02_extract.py --stop-code XXX  # stop after processing this code
    python 02_extract.py --no-vision      # skip API calls (archive screenshots only)

Prereqs:
    - Dispatch INNOVIA open, "Liste des ordres réguliers" focused, 1st row selected (blue).
    - config.json present (run 01_calibrate.py first).
    - ANTHROPIC_API_KEY env var set.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path

from lib import navigation as nav
from lib.checkpoint import append_order_row, load_processed, mark_processed
from lib.schema import normalize_row
from lib.vision import VisionExtractor

ROOT = Path(__file__).parent
CONFIG_PATH = ROOT / "config.json"
OUTPUT_DIR = ROOT / "output"
ORDERS_CSV = OUTPUT_DIR / "orders.csv"
PROCESSED_CSV = OUTPUT_DIR / "processed.csv"
SCREENSHOTS_DIR = OUTPUT_DIR / "screenshots"
ERRORS_LOG = OUTPUT_DIR / "errors.log"

DEFAULT_STOP_CODE = "VIA33MON"


def log(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def log_error(msg: str) -> None:
    log(f"ERROR: {msg}")
    ERRORS_LOG.parent.mkdir(parents=True, exist_ok=True)
    with open(ERRORS_LOG, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now().isoformat(timespec='seconds')}\t{msg}\n")


def capture_all_tabs(cfg: dict, order_dir: Path) -> dict[str, Path]:
    """Click through every tab & sub-tab, take a screenshot for each view."""
    order_dir.mkdir(parents=True, exist_ok=True)
    shots: dict[str, Path] = {}

    # Général
    nav.click_at(cfg["tabs"]["general"], pause=0.7)
    shots["general"] = nav.screenshot(order_dir / "01_general.png")

    # Ordre → Enlèvement
    nav.click_at(cfg["tabs"]["ordre"], pause=0.7)
    nav.click_at(cfg["sub_tabs"]["enlevement"], pause=0.4)
    shots["ordre_enl"] = nav.screenshot(order_dir / "02_ordre_enlevement.png")

    # Ordre → Contact côté Enlèvement
    nav.click_at(cfg["sub_tabs"]["enlevement_contact"], pause=0.4)
    shots["ordre_enl_contact"] = nav.screenshot(order_dir / "03_ordre_enl_contact.png")

    # Ordre → Livraison
    nav.click_at(cfg["sub_tabs"]["livraison"], pause=0.4)
    shots["ordre_liv"] = nav.screenshot(order_dir / "04_ordre_livraison.png")

    # Ordre → Contact côté Livraison
    nav.click_at(cfg["sub_tabs"]["livraison_contact"], pause=0.4)
    shots["ordre_liv_contact"] = nav.screenshot(order_dir / "05_ordre_liv_contact.png")

    # Attribution
    nav.click_at(cfg["tabs"]["attribution"], pause=0.7)
    shots["attribution"] = nav.screenshot(order_dir / "06_attribution.png")

    # Tarification
    nav.click_at(cfg["tabs"]["tarification"], pause=0.7)
    shots["tarification"] = nav.screenshot(order_dir / "07_tarification.png")

    return shots


def process_one(cfg: dict, extractor: VisionExtractor | None) -> tuple[str | None, str]:
    """Process the currently-selected order. Returns (code, status)."""
    detail = nav.open_current_order()
    if detail is None:
        return None, "no_detail_window"

    code = nav.extract_code_from_title(detail) or "UNKNOWN"
    log(f"  order opened: {code}")

    order_dir = SCREENSHOTS_DIR / code
    shots = capture_all_tabs(cfg, order_dir)

    status = "ok"
    row = normalize_row({"code_ordre": code})

    if extractor is not None:
        data, raw = extractor.extract(shots)
        if data is None:
            status = "vision_failed"
            (order_dir / "vision_raw.txt").write_text(raw or "", encoding="utf-8")
            log(f"  vision extraction failed for {code}")
        else:
            row = normalize_row(data)
            # safety: make sure code_ordre is set even if vision missed it
            if not row.get("code_ordre"):
                row["code_ordre"] = code
            (order_dir / "vision_raw.json").write_text(
                json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8"
            )

    row["status"] = status
    row["screenshots_dir"] = str(order_dir.relative_to(ROOT))
    row["extracted_at"] = datetime.now().isoformat(timespec="seconds")

    append_order_row(ORDERS_CSV, row)

    closed = nav.close_detail(cfg.get("close_button"))
    if not closed:
        log_error(f"  could not close detail for {code}")
        status = status + "+close_stuck"

    return code, status


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", type=int, default=0, help="process only N orders then stop")
    parser.add_argument("--stop-code", type=str, default=DEFAULT_STOP_CODE,
                        help=f"stop after processing this code (default: {DEFAULT_STOP_CODE})")
    parser.add_argument("--no-vision", action="store_true",
                        help="skip Claude API calls (archive screenshots only)")
    args = parser.parse_args()

    if not CONFIG_PATH.exists():
        log(f"ERROR: {CONFIG_PATH} missing — run 01_calibrate.py first.")
        sys.exit(1)

    cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    OUTPUT_DIR.mkdir(exist_ok=True)

    extractor: VisionExtractor | None = None
    if not args.no_vision:
        try:
            extractor = VisionExtractor()
        except RuntimeError as e:
            log(f"ERROR: {e}")
            sys.exit(1)

    processed = load_processed(PROCESSED_CSV)
    log(f"Resuming with {len(processed)} orders already processed.")
    log(f"Stop code: {args.stop_code}")
    if args.dry_run:
        log(f"DRY RUN: limited to {args.dry_run} orders.")

    log("Starts in 5 seconds — focus Dispatch INNOVIA now.")
    time.sleep(5)

    count = 0
    while True:
        try:
            code, status = process_one(cfg, extractor)
        except Exception:
            tb = traceback.format_exc()
            log_error(f"unexpected exception: {tb}")
            # attempt to recover
            nav.close_detail(cfg.get("close_button"))
            nav.next_row()
            continue

        if code is None:
            log_error("no detail window opened; aborting")
            break

        mark_processed(PROCESSED_CSV, code, status)
        processed[code] = status
        count += 1
        log(f"  #{count} done: {code} [{status}]")

        if code == args.stop_code:
            log(f"Reached stop code {code}. Stopping.")
            break
        if args.dry_run and count >= args.dry_run:
            log(f"Reached dry-run limit ({args.dry_run}). Stopping.")
            break

        nav.next_row()

    log(f"=== Finished. Total processed this run: {count} ===")


if __name__ == "__main__":
    main()
