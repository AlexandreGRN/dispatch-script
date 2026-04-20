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

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import argparse
import json
import time
import traceback
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

from lib import navigation as nav
from lib.checkpoint import append_order_row, load_processed, mark_processed
from lib.schema import normalize_row
from lib.vision import VisionExtractor

MIN_NAV_WAIT = 4.0  # minimum seconds to let next window load while Claude runs

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


def capture_all_tabs(cfg: dict, order_dir: Path) -> tuple[dict[str, Path], list[str]]:
    """Click through every tab & sub-tab, take a screenshot for each view.
    Returns (shots dict, list of tab names with poor quality screenshots).
    """
    order_dir.mkdir(parents=True, exist_ok=True)
    shots: dict[str, Path] = {}
    poor_quality: list[str] = []

    def snap(name: str, path: Path, click_xy=None) -> None:
        p, ok = nav.screenshot_with_retry(path, click_xy=click_xy)
        shots[name] = p
        if not ok:
            poor_quality.append(name)
            log(f"  ⚠ poor quality screenshot: {name} (flagged)")

    # Général
    nav.click_at(cfg["tabs"]["general"], pause=1.0)
    snap("general", order_dir / "01_general.png", click_xy=cfg["tabs"]["general"])

    # Ordre → Enlèvement
    nav.click_at(cfg["tabs"]["ordre"], pause=1.0)
    nav.click_at(cfg["sub_tabs"]["enlevement"], pause=0.8)
    snap("ordre_enl", order_dir / "02_ordre_enlevement.png", click_xy=cfg["sub_tabs"]["enlevement"])

    # Ordre → Contact Enlèvement
    nav.click_at(cfg["sub_tabs"]["enlevement_contact"], pause=0.8)
    snap("ordre_enl_contact", order_dir / "03_ordre_enl_contact.png", click_xy=cfg["sub_tabs"]["enlevement_contact"])

    # Ordre → Livraison
    nav.click_at(cfg["sub_tabs"]["livraison"], pause=0.8)
    snap("ordre_liv", order_dir / "04_ordre_livraison.png", click_xy=cfg["sub_tabs"]["livraison"])

    # Ordre → Contact Livraison
    nav.click_at(cfg["sub_tabs"]["livraison_contact"], pause=0.8)
    snap("ordre_liv_contact", order_dir / "05_ordre_liv_contact.png", click_xy=cfg["sub_tabs"]["livraison_contact"])

    # Attribution
    nav.click_at(cfg["tabs"]["attribution"], pause=1.0)
    snap("attribution", order_dir / "06_attribution.png", click_xy=cfg["tabs"]["attribution"])

    # Tarification
    nav.click_at(cfg["tabs"]["tarification"], pause=1.0)
    snap("tarification", order_dir / "07_tarification.png", click_xy=cfg["tabs"]["tarification"])

    return shots, poor_quality


def _write_row(
    code: str,
    order_dir: Path,
    poor_quality: list[str],
    extractor: VisionExtractor | None,
    data: dict | None,
    raw: str | None,
) -> str:
    """Build and append the CSV row for one order. Returns the status string."""
    status = "ok"
    if poor_quality:
        status = "partial_quality"

    row = normalize_row({"code_ordre": code})
    if extractor is not None:
        if data is None:
            status = "vision_failed"
            (order_dir / "vision_raw.txt").write_text(raw or "", encoding="utf-8")
            log(f"  vision extraction failed for {code}")
        else:
            row = normalize_row(data)
            if not row.get("code_ordre"):
                row["code_ordre"] = code
            (order_dir / "vision_raw.json").write_text(
                json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8"
            )

    row["status"] = status
    if poor_quality:
        row["champs_manquants"] = "poor_screenshot:" + ",".join(poor_quality)
    row["screenshots_dir"] = str(order_dir.relative_to(ROOT))
    row["extracted_at"] = datetime.now().isoformat(timespec="seconds")

    append_order_row(ORDERS_CSV, row)
    return status


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

    with ThreadPoolExecutor(max_workers=1) as executor:
        # Open the first order before entering the loop.
        detail = nav.open_current_order()
        if detail is None:
            log_error("Could not open first order; aborting.")
            return

        count = 0
        while True:
            # ── CAPTURE: screenshots of the currently-open detail window ──────────
            try:
                code = nav.extract_code_from_title(detail) or "UNKNOWN"
                log(f"  order opened: {code}")
                order_dir = SCREENSHOTS_DIR / code
                shots, poor_quality = capture_all_tabs(cfg, order_dir)
            except Exception:
                tb = traceback.format_exc()
                log_error(f"capture exception: {tb}")
                nav.close_detail(cfg.get("close_button"))
                nav.next_row()
                detail = nav.open_current_order()
                if detail is None:
                    log_error("recovery failed; aborting.")
                    break
                continue

            # ── NAVIGATE: close current detail, advance list, fire F10 ────────────
            closed = nav.close_detail(cfg.get("close_button"))
            if not closed:
                log_error(f"close stuck for {code}; continuing anyway")

            nav.next_row()
            nav_t0 = time.time()
            nav.fire_open_next()  # F10 without waiting — next window loads in background

            # ── CLAUDE: extract in background while next window loads ─────────────
            future: Future | None = None
            if extractor is not None:
                future = executor.submit(extractor.extract, shots)

            # ── SYNC: wait for Claude AND at least MIN_NAV_WAIT total ────────────
            data: dict | None = None
            raw: str | None = None
            if future is not None:
                data, raw = future.result()  # blocks until Claude done

            elapsed = time.time() - nav_t0
            remaining = MIN_NAV_WAIT - elapsed
            if remaining > 0:
                time.sleep(remaining)

            # ── WRITE: CSV row for the order just captured ────────────────────────
            status = _write_row(code, order_dir, poor_quality, extractor, data, raw)
            mark_processed(PROCESSED_CSV, code, status)
            processed[code] = status
            count += 1
            log(f"  #{count} done: {code} [{status}]")

            # ── STOP: check conditions ────────────────────────────────────────────
            if code == args.stop_code:
                log(f"Reached stop code {code}. Stopping.")
                break
            if args.dry_run and count >= args.dry_run:
                log(f"Reached dry-run limit ({args.dry_run}). Stopping.")
                break

            # ── NEXT: the detail window was already fired; wait for it ────────────
            detail = nav.wait_detail_window(timeout=max(3.0, MIN_NAV_WAIT))
            if detail is None:
                log_error("Next detail window did not appear; aborting.")
                break

    log(f"=== Finished. Total processed this run: {count} ===")


if __name__ == "__main__":
    main()
