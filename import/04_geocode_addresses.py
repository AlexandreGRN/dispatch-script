"""Geocode unique addresses from orders.csv via BAN (fallback Nominatim).

Outputs:
  output/import/address_map.json     {normalized_key: {lat, lng, ...}}
  output/import/missing_addresses.csv   rows that failed geocoding

Doesn't call the Obsher API — address_id creation happens in 05 (needs
company_id, which depends on resolved customer). This script is idempotent:
re-running merges into the existing address_map.json.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from lib.geocode import Geocoder  # noqa: E402


_GARBAGE_STREET_PATTERNS = (
    "CAISSE",
    "14M3",
    "20M3",
    "DEBACHABLE",
    "* PRENDRE",
    "RN 117",
)


def clean_street(street: str) -> str:
    """Blank out street names that are extraction artifacts (vehicle descs,
    driver notes) so we at least geocode at postal/city level."""
    up = street.upper()
    if any(pat in up for pat in _GARBAGE_STREET_PATTERNS):
        return ""
    return street


def normalize_key(
    street_no: str, street: str, postal: str, city: str, country: str
) -> str:
    return "|".join(
        part.strip().upper()
        for part in (street_no, street, postal, city, country)
    )


def extract_unique_addresses(orders_csv: Path) -> list[dict]:
    seen: dict[str, dict] = {}
    with orders_csv.open(encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            for prefix in ("enl", "liv"):
                entry = {
                    "street_number": row[f"{prefix}_no"].strip(),
                    "street_name": clean_street(row[f"{prefix}_rue"].strip()),
                    "postal_code": row[f"{prefix}_cp"].strip(),
                    "city": row[f"{prefix}_ville"].strip(),
                    "country": (row[f"{prefix}_pays"].strip() or "FR"),
                    "custom_name": row[f"{prefix}_nom"].strip(),
                    "example_order": row["code_ordre"],
                    "side": "pickup" if prefix == "enl" else "dropoff",
                }
                key = normalize_key(
                    entry["street_number"],
                    entry["street_name"],
                    entry["postal_code"],
                    entry["city"],
                    entry["country"],
                )
                if key and key not in seen:
                    entry["key"] = key
                    seen[key] = entry
    return list(seen.values())


def main() -> int:
    ap = argparse.ArgumentParser(description="Geocode addresses from orders.csv")
    ap.add_argument("--orders", default="output/orders.csv")
    ap.add_argument("--out-dir", default="output/import")
    ap.add_argument("--limit", type=int, default=0, help="0 = no limit")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    script_root = HERE.parent
    orders_csv = (script_root / args.orders).resolve()
    out_dir = (script_root / args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    if not orders_csv.exists():
        print(f"orders.csv not found: {orders_csv}", file=sys.stderr)
        return 2

    address_map_path = out_dir / "address_map.json"
    missing_path = out_dir / "missing_addresses.csv"
    existing: dict = {}
    if address_map_path.exists():
        with address_map_path.open(encoding="utf-8") as f:
            existing = json.load(f)

    addrs = extract_unique_addresses(orders_csv)
    print(f"unique addresses in orders.csv: {len(addrs)}")
    to_process = [a for a in addrs if a["key"] not in existing]
    print(f"already cached: {len(addrs) - len(to_process)} / to geocode: {len(to_process)}")
    if args.limit:
        to_process = to_process[: args.limit]
        print(f"--limit {args.limit} -> processing {len(to_process)}")

    if args.dry_run:
        for a in to_process[:10]:
            print("  DRY:", a["street_number"], a["street_name"], a["postal_code"], a["city"])
        return 0

    geocoder = Geocoder()
    missing_rows: list[dict] = []
    for i, a in enumerate(to_process, 1):
        hit = geocoder.search(
            a["street_number"],
            a["street_name"],
            a["postal_code"],
            a["city"],
            a["country"],
        )
        if hit is None:
            existing[a["key"]] = {
                "lat": 0.0,
                "lng": 0.0,
                "status": "failed",
                "street_number": a["street_number"],
                "street_name": a["street_name"],
                "postal_code": a["postal_code"],
                "city": a["city"],
                "country": a["country"],
            }
            missing_rows.append(
                {
                    "key": a["key"],
                    "street_number": a["street_number"],
                    "street_name": a["street_name"],
                    "postal_code": a["postal_code"],
                    "city": a["city"],
                    "country": a["country"],
                    "example_order": a["example_order"],
                    "reason": "geocode_failed",
                }
            )
            print(f"  [{i}/{len(to_process)}] FAIL  {a['street_name']} {a['postal_code']} {a['city']}")
        else:
            existing[a["key"]] = {
                **hit,
                "status": "ok",
                "street_number": a["street_number"],
                "street_name": a["street_name"],
                "postal_code": a["postal_code"],
                "city": a["city"],
                "country": a["country"],
            }
            print(
                f"  [{i}/{len(to_process)}] OK    {hit['source']:9s} "
                f"{hit['lat']:.4f},{hit['lng']:.4f}  {a['street_name']} {a['postal_code']} {a['city']}"
            )

        if i % 20 == 0:
            with address_map_path.open("w", encoding="utf-8") as f:
                json.dump(existing, f, ensure_ascii=False, indent=2)

    with address_map_path.open("w", encoding="utf-8") as f:
        json.dump(existing, f, ensure_ascii=False, indent=2)

    if missing_rows:
        fieldnames = list(missing_rows[0].keys())
        with missing_path.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(missing_rows)

    total = len(existing)
    ok = sum(1 for v in existing.values() if v.get("status") == "ok")
    failed = sum(1 for v in existing.values() if v.get("status") == "failed")
    print(f"\naddress_map.json: {total} entries ({ok} ok / {failed} failed)")
    if failed:
        print(f"missing_addresses.csv: {failed} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
