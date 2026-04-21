"""Create recurrences in Obsher API for each order created by 05_create_orders.py.

Reads:
  output/orders.csv                 (for periodicite, frequence, jours, dates)
  output/import/created_orders.csv  (for order_ids of status=ok)
Writes:
  output/import/created_recurrences.csv
  output/import/skipped_recurrences.csv
  output/import/recurrences_output.txt

Idempotence: skipped if code_ordre already in created_recurrences.csv with status=ok.

Mapping (CSV → API):
  periodicite "Hebdomadaire" → type=weekly
  periodicite "Mensuelle"    → type=monthly (skipped if no days_of_month)
  periodicite "Quotidienne"  → type=daily
  frequence_intervalle       → interval (default 1)
  jours_semaine "lun,mar,..."→ weekly.days=[{day:"monday"},...]
  days_of_month "1,15"       → monthly.days_of_month=[1,15]
  jours_feries "Jours fériés exclus" → include_holidays=false
  date_debut, date_fin       → begin_date, end_date (unix ts)

Flags:
  --dry-run            print payloads, no network
  --limit N            process N recurrences then stop
  --override-date YYYY-MM-DD   override begin_date (mirrors 05_create_orders.py)
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from lib.api_client import ApiClient, load_config  # noqa: E402


DAY_MAP = {
    "lun": "monday", "mar": "tuesday", "mer": "wednesday", "jeu": "thursday",
    "ven": "friday", "sam": "saturday", "dim": "sunday",
}

# Far-future fallback when the "Au" checkbox is unchecked in Dispatch
# (i.e. the recurrence has no explicit end date). Picked arbitrarily far
# out so the recurrence stays active for years.
DEFAULT_END_DATE = "01/01/2028"

TYPE_MAP = {
    "Hebdomadaire": "weekly",
    "Mensuelle": "monthly",
    "Quotidienne": "daily",
}


def to_unix_ts(date_ddmmyyyy: str) -> int | None:
    if not date_ddmmyyyy:
        return None
    try:
        d = dt.datetime.strptime(date_ddmmyyyy.strip(), "%d/%m/%Y")
    except ValueError:
        return None
    return int(d.replace(hour=0, minute=0).timestamp())


def parse_days_of_month(raw: str) -> list[int]:
    if not raw:
        return []
    out: list[int] = []
    for tok in raw.split(","):
        tok = tok.strip()
        if not tok:
            continue
        try:
            n = int(tok)
            if 1 <= n <= 31:
                out.append(n)
        except ValueError:
            continue
    return out


def parse_weekdays(raw: str) -> list[dict]:
    if not raw:
        return []
    out: list[dict] = []
    for tok in raw.split(","):
        tok = tok.strip().lower()[:3]
        day = DAY_MAP.get(tok)
        if day:
            out.append({"day": day})
    return out


def build_payload(row: dict, order_id: str, override_date: str | None) -> tuple[dict | None, str]:
    """Returns (payload, skip_reason). If payload is None, skip with reason."""
    perio = (row.get("periodicite") or "").strip()
    rtype = TYPE_MAP.get(perio)
    if not rtype:
        return None, f"no/unknown periodicite: {perio!r}"

    interval_raw = (row.get("frequence_intervalle") or "1").strip()
    try:
        interval = int(interval_raw) if interval_raw else 1
    except ValueError:
        interval = 1

    begin_src = override_date or row.get("date_debut", "")
    begin_ts = to_unix_ts(begin_src)
    if begin_ts is None:
        return None, f"invalid begin date: {begin_src!r}"
    end_ts = to_unix_ts(row.get("date_fin", "")) or to_unix_ts(DEFAULT_END_DATE)

    include_holidays = (row.get("jours_feries", "").strip() != "Jours fériés exclus")

    payload: dict = {
        "order_id": order_id,
        "type": rtype,
        "interval": max(1, interval),
        "shift_non_working": False,
        "include_holidays": include_holidays,
        "is_active": True,
        "begin_date": begin_ts,
    }
    if end_ts is not None:
        payload["end_date"] = end_ts

    if rtype == "weekly":
        days = parse_weekdays(row.get("jours_semaine", ""))
        if not days:
            return None, "weekly without jours_semaine"
        payload["weekly"] = {"days": days}
    elif rtype == "monthly":
        dom = parse_days_of_month(row.get("days_of_month", ""))
        if not dom:
            return None, "monthly without days_of_month (not re-extracted yet)"
        payload["monthly"] = {"days_of_month": dom, "weekdays": []}
    # daily needs no sub-struct

    return payload, ""


def load_created_orders(path: Path) -> dict[str, dict]:
    if not path.exists():
        return {}
    out = {}
    with path.open(encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r.get("status") == "ok":
                out[r["code_ordre"]] = r
    return out


def load_created_recurrences(path: Path) -> dict[str, dict]:
    if not path.exists():
        return {}
    out = {}
    with path.open(encoding="utf-8") as f:
        for r in csv.DictReader(f):
            out[r["code_ordre"]] = r
    return out


def append(path: Path, row: dict, fieldnames: list[str]) -> None:
    new = not path.exists()
    with path.open("a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if new:
            w.writeheader()
        w.writerow(row)


def main() -> int:
    ap = argparse.ArgumentParser(description="Create recurrences for imported orders")
    ap.add_argument("--config", default="import/config.json")
    ap.add_argument("--orders", default="output/orders.csv")
    ap.add_argument("--out-dir", default="output/import")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--override-date", default=None,
                    help="Override begin_date (ISO YYYY-MM-DD). "
                         "Use same value as 05_create_orders.py --override-date.")
    ap.add_argument("--codes", default=None,
                    help="Comma-separated list of code_ordre to process (others skipped).")
    args = ap.parse_args()
    code_filter: set[str] | None = (
        {c.strip() for c in args.codes.split(",") if c.strip()} if args.codes else None
    )

    override_date: str | None = None
    if args.override_date:
        try:
            iso = dt.datetime.strptime(args.override_date, "%Y-%m-%d").date()
        except ValueError:
            print(f"ERROR: --override-date must be YYYY-MM-DD, got: {args.override_date}", file=sys.stderr)
            return 2
        override_date = iso.strftime("%d/%m/%Y")
        print(f"→ override begin_date = {override_date}")

    script_root = HERE.parent
    cfg = load_config((script_root / args.config).resolve())
    orders_csv = (script_root / args.orders).resolve()
    out_dir = (script_root / args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    created_orders = load_created_orders(out_dir / "created_orders.csv")
    created_recurrences = load_created_recurrences(out_dir / "created_recurrences.csv")

    client = ApiClient(cfg["api"]["base_url"], cfg["api"]["token"],
                       log_path=out_dir / "api_calls.log")

    with orders_csv.open(encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

    rec_fields = ["code_ordre", "order_id", "recurrence_id", "type", "status", "error_msg"]
    skip_fields = ["code_ordre", "order_id", "reason"]
    processed = 0
    ok_count = skip_count = fail_count = 0

    for row in rows:
        code = row["code_ordre"]
        order_meta = created_orders.get(code)
        if not order_meta:
            continue  # order was never created, skip
        order_id = order_meta["order_id"]

        prior = created_recurrences.get(code)
        if prior and prior.get("status") == "ok":
            continue

        payload, skip_reason = build_payload(row, order_id, override_date)
        if payload is None:
            append(out_dir / "skipped_recurrences.csv",
                   {"code_ordre": code, "order_id": order_id, "reason": skip_reason},
                   skip_fields)
            skip_count += 1
            print(f"  SKIP {code:12s}  {skip_reason}")
            continue

        if args.dry_run:
            print(f"[DRY] {code:12s}  type={payload['type']:8s}  interval={payload['interval']}  "
                  f"begin={payload['begin_date']}")
            ok_count += 1
            processed += 1
            if args.limit and processed >= args.limit:
                break
            continue

        status, resp = client.post("/operator/recurrence", payload)
        result = {
            "code_ordre": code, "order_id": order_id, "recurrence_id": "",
            "type": payload["type"], "status": "", "error_msg": "",
        }
        if status in (200, 201) and "data" in resp:
            result["recurrence_id"] = (resp["data"].get("uuid") or "")
            result["status"] = "ok"
            ok_count += 1
        else:
            result["status"] = "failed"
            result["error_msg"] = f"recurrence POST {status}: {str(resp)[:200]}"
            fail_count += 1

        append(out_dir / "created_recurrences.csv", result, rec_fields)
        print(f"  {result['status']:7s} {code:12s}  type={payload['type']:8s}  "
              f"{result['recurrence_id'][:8] or '—'}  {result['error_msg'][:80]}")
        processed += 1
        if args.limit and processed >= args.limit:
            break

    # Final summary
    summary = out_dir / "recurrences_output.txt"
    total_orders_ok = len(created_orders)
    summary.write_text(
        f"Recurrence run — {dt.datetime.now().isoformat(timespec='seconds')}\n"
        f"Orders éligibles (created status=ok): {total_orders_ok}\n"
        f"Récurrences créées ok:   {ok_count}\n"
        f"Récurrences skippées:    {skip_count}  (cf. skipped_recurrences.csv)\n"
        f"Récurrences échouées:    {fail_count}\n",
        encoding="utf-8",
    )
    print(f"\n=== Done. ok={ok_count} skip={skip_count} fail={fail_count} ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
