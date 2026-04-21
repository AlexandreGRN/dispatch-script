"""
Main extraction loop: iterate through every order in the list, capture 8 screenshots,
send them to Claude Vision, append structured data to output/orders.csv.

Usage:
    python 02_extract.py                           # full run until VIA33MON
    python 02_extract.py --dry-run 3               # stop after 3 orders (for testing)
    python 02_extract.py --stop-code XXX           # stop after processing this code
    python 02_extract.py --no-vision               # skip API calls (archive screenshots only)
    python 02_extract.py --codes 2BOL458,CEN11TOU  # process only these codes
    python 02_extract.py --codes-file path.txt     # process only codes listed (comma-or-newline-separated)
    python 02_extract.py --codes-file to_review.txt --overwrite
                                                   # re-extract listed codes, replacing existing data

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
import shutil
import time
import traceback
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

from lib import navigation as nav
from lib.checkpoint import (
    append_order_row,
    load_processed,
    mark_processed,
    migrate_csv_header,
    remove_codes,
)
from lib.schema import normalize_row
from lib.vision import VisionExtractor

MIN_NAV_WAIT = 8.0  # minimum seconds to let next window load while Claude runs

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

    Navigation order chosen so Général is captured LAST — it contains the
    Gantt calendar which takes the longest to render in Dispatch. By touching
    all other tabs first, Général has extra time to repaint between selection
    and screenshot.
    """
    order_dir.mkdir(parents=True, exist_ok=True)
    shots: dict[str, Path] = {}
    poor_quality: list[str] = []

    def snap(name: str, path: Path, click_xy=None, sweep: bool = False) -> None:
        # Sweep only on tabs that historically render badly (Gantt on Général).
        # Simple form tabs repaint fine with just the click hover.
        p, ok = nav.screenshot_with_retry(path, click_xy=click_xy, sweep=sweep)
        shots[name] = p
        if not ok:
            poor_quality.append(name)
            log(f"  ⚠ poor quality screenshot: {name} (flagged)")

    # Ordre → Enlèvement
    nav.click_at(cfg["tabs"]["ordre"], pause=0.5)
    nav.click_at(cfg["sub_tabs"]["enlevement"], pause=0.4)
    snap("ordre_enl", order_dir / "01_ordre_enlevement.png", click_xy=cfg["sub_tabs"]["enlevement"])

    # Ordre → Contact Enlèvement
    nav.click_at(cfg["sub_tabs"]["enlevement_contact"], pause=0.4)
    snap("ordre_enl_contact", order_dir / "02_ordre_enl_contact.png", click_xy=cfg["sub_tabs"]["enlevement_contact"])

    # Ordre → Livraison
    nav.click_at(cfg["sub_tabs"]["livraison"], pause=0.4)
    snap("ordre_liv", order_dir / "03_ordre_livraison.png", click_xy=cfg["sub_tabs"]["livraison"])

    # Ordre → Contact Livraison
    nav.click_at(cfg["sub_tabs"]["livraison_contact"], pause=0.4)
    snap("ordre_liv_contact", order_dir / "04_ordre_liv_contact.png", click_xy=cfg["sub_tabs"]["livraison_contact"])

    # Informations (commentaires + saisie) — skipped if not calibrated yet
    info_xy = cfg.get("tabs", {}).get("informations")
    if info_xy:
        nav.click_at(info_xy, pause=0.5)
        snap("informations", order_dir / "05_informations.png", click_xy=info_xy)
    else:
        log("  ⚠ tabs.informations not calibrated — skipping Informations tab")

    # Attribution
    nav.click_at(cfg["tabs"]["attribution"], pause=0.5)
    snap("attribution", order_dir / "06_attribution.png", click_xy=cfg["tabs"]["attribution"])

    # Tarification
    nav.click_at(cfg["tabs"]["tarification"], pause=0.5)
    snap("tarification", order_dir / "07_tarification.png", click_xy=cfg["tabs"]["tarification"])

    # Général — LAST, sweep enabled (Gantt is the slow-painting widget).
    nav.click_at(cfg["tabs"]["general"], pause=1.5)
    snap("general", order_dir / "08_general.png", click_xy=cfg["tabs"]["general"], sweep=True)

    # Général — dropdown "Le ..." (jours de semaine) déplié, pour lever
    # l'ambiguïté quand le texte inline est illisible. Dropdown reste ouvert :
    # close_detail() (Escape+Enter) qui suit ferme tout d'un coup.
    days_xy = cfg.get("tabs", {}).get("general_days_dropdown")
    if days_xy:
        nav.click_at(days_xy, pause=0.6)
        snap("general_days", order_dir / "09_general_days.png")
    else:
        log("  ⚠ tabs.general_days_dropdown not calibrated — skipping days dropdown")

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


def _load_target_codes(args) -> set[str] | None:
    """Parse --codes and --codes-file into a set of uppercase codes, or None if not set."""
    codes: set[str] = set()
    if args.codes:
        codes.update(c.strip().upper() for c in args.codes.split(",") if c.strip())
    if args.codes_file:
        p = Path(args.codes_file)
        if not p.exists():
            log(f"ERROR: --codes-file {p} not found")
            sys.exit(1)
        text = p.read_text(encoding="utf-8")
        # Accept either commas or newlines as separators.
        for tok in text.replace("\n", ",").split(","):
            tok = tok.strip().upper()
            if tok:
                codes.add(tok)
    return codes or None


def _purge_codes(codes: set[str]) -> None:
    """Delete screenshot dirs + remove rows from orders.csv and processed.csv for these codes."""
    migrate_csv_header(ORDERS_CSV)
    removed_orders = remove_codes(ORDERS_CSV, codes, code_col="code_ordre")
    removed_proc = remove_codes(PROCESSED_CSV, codes, code_col="code")
    deleted_dirs = 0
    for code in codes:
        d = SCREENSHOTS_DIR / code
        if d.exists():
            shutil.rmtree(d)
            deleted_dirs += 1
    log(f"Purged {removed_orders} row(s) from orders.csv, "
        f"{removed_proc} from processed.csv, {deleted_dirs} screenshot dir(s).")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", type=int, default=0, help="process only N orders then stop")
    parser.add_argument("--stop-code", type=str, default=DEFAULT_STOP_CODE,
                        help=f"stop after processing this code (default: {DEFAULT_STOP_CODE})")
    parser.add_argument("--no-vision", action="store_true",
                        help="skip Claude API calls (archive screenshots only)")
    parser.add_argument("--codes", type=str, default=None,
                        help="comma-separated list of codes to process (skip all others)")
    parser.add_argument("--codes-file", type=str, default=None,
                        help="path to a file containing codes (comma- or newline-separated)")
    parser.add_argument("--overwrite", action="store_true",
                        help="when used with --codes/--codes-file: delete existing screenshots + "
                             "remove rows from orders.csv + processed.csv for the listed codes "
                             "before processing")
    args = parser.parse_args()

    if not CONFIG_PATH.exists():
        log(f"ERROR: {CONFIG_PATH} missing — run 01_calibrate.py first.")
        sys.exit(1)

    cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    OUTPUT_DIR.mkdir(exist_ok=True)

    # Migrate orders.csv header if CSV_COLUMNS has drifted (e.g. new columns added).
    if migrate_csv_header(ORDERS_CSV):
        log(f"Migrated {ORDERS_CSV.name} header to current schema.")

    target_codes = _load_target_codes(args)
    if args.overwrite and not target_codes:
        log("ERROR: --overwrite requires --codes or --codes-file.")
        sys.exit(1)
    if args.overwrite and target_codes:
        log(f"--overwrite: purging {len(target_codes)} code(s) from existing outputs…")
        _purge_codes(target_codes)

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
    if target_codes:
        log(f"Target filter: {len(target_codes)} code(s) to process (all others skipped).")
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
        skipped = 0
        while True:
            # ── FILTER: if target_codes is set, skip non-matching orders ──────────
            code = nav.extract_code_from_title(detail) or "UNKNOWN"
            if target_codes is not None and code not in target_codes:
                skipped += 1
                if skipped % 20 == 0:
                    log(f"  … skipped {skipped} non-target orders so far (last: {code})")
                closed = nav.close_detail(cfg.get("close_button"))
                if not closed:
                    log_error(f"close stuck on non-target {code}; retrying once")
                    time.sleep(1.0)
                    closed = nav.close_detail(cfg.get("close_button"))
                    if not closed:
                        log_error("close still stuck; aborting cleanly.")
                        break
                # Let the list re-focus before moving selection
                time.sleep(0.6)
                nav.next_row(pause=0.5)
                nav_t0 = time.time()
                nav.fire_open_next()
                if code == args.stop_code:
                    log(f"Reached stop code {code} while skipping. Stopping.")
                    break
                # Replicate main-path timing: ensure at least MIN_NAV_WAIT elapses
                # between F10 and wait_detail_window (in the main path, Claude fills that gap).
                elapsed = time.time() - nav_t0
                remaining = MIN_NAV_WAIT - elapsed
                if remaining > 0:
                    time.sleep(remaining)
                detail = nav.wait_detail_window(timeout=8.0)
                if detail is None:
                    # One retry with a fresh F10 — Dispatch sometimes drops the first keystroke
                    log(f"  detail window missing after skip of {code}; retrying F10 once")
                    nav.fire_open_next()
                    detail = nav.wait_detail_window(timeout=6.0)
                if detail is None:
                    log_error("Next detail window did not appear after skip; aborting.")
                    break
                continue

            # ── CAPTURE: screenshots of the currently-open detail window ──────────
            try:
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

    log(f"=== Finished. Total processed this run: {count} (skipped {skipped}) ===")


if __name__ == "__main__":
    main()
