"""Create orders in Obsher API from orders.csv.

Two flows depending on code_prestation:
  - Transport (default): POSTs order → transport → dispatch/simulation → merchandises
  - Rental (code_prestation starts with "T"): POSTs order → order/rental → dispatch/simulation
    Meeting point = pickup address. Depot = config.default_depot_uuid. No dropoff.

Idempotence: skipped if code_ordre already in created_orders.csv with status=ok.

Writes:
  output/import/created_orders.csv
  output/import/address_map.json   (enriched with address_ids per company)
  output/import/output.txt         (final summary incl. transport/rental breakdown)
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from lib.api_client import ApiClient, load_config  # noqa: E402
from lib import db as dbmod  # noqa: E402


HORAIRE_MAP = {
    "à": "at",
    "avant": "before",
    "à partir de": "after",
    "": "at",
}

BILLING_EMAIL = "alexandre.guerin@obspher.com"
BILLING_PHONE = "+33600000000"

DEFAULT_MERCHANDISE = {
    "nature": "Générique (import Dispatch)",
    "size": {"weight": 1, "height": 1, "length": 1, "width": 1},
    "attributes": {
        "is_fragile": False,
        "is_dangerous": False,
        "is_stackable": True,
        "is_rotatable": True,
        "is_palletize": False,
    },
}

DEFAULT_ACCESSIBILITY = {
    "weight": 0,
    "height": 0,
    "length": 0,
    "width": 0,
    "has_pallett_truck": False,
    "has_elevator": False,
    "has_plateform": False,
    "has_guardhouse": False,
    "has_loading_dock": False,
    "guardhouse_delay": 0,
    "loading_dock_delay": 0,
}


def is_rental_prestation(code_prestation: str) -> bool:
    """T-prefixed prestations are rentals (vehicle loans)."""
    return code_prestation.strip().upper().startswith("T")


def normalize_fr_phone(raw: str) -> str | None:
    """Convert French 10-digit phone to E.164. Returns None if invalid."""
    if not raw:
        return None
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 10 and digits.startswith("0"):
        return "+33" + digits[1:]
    if len(digits) == 11 and digits.startswith("33"):
        return "+" + digits
    if raw.startswith("+") and len(digits) >= 10:
        return "+" + digits
    return None


def split_contact_name(full: str) -> tuple[str, str]:
    if not full:
        return "", ""
    # Take only the first name (before "/") if there are multiple
    first = full.split("/")[0].strip()
    parts = first.split()
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])


def to_unix_ts(date_ddmmyyyy: str, time_hhmm: str) -> int | None:
    """Combine '14/03/2022' + '05:30' into Unix timestamp (Europe/Paris naive → UTC)."""
    if not date_ddmmyyyy:
        return None
    try:
        d = dt.datetime.strptime(date_ddmmyyyy.strip(), "%d/%m/%Y")
    except ValueError:
        return None
    hh, mm = 0, 0
    if time_hhmm:
        try:
            hh, mm = map(int, time_hhmm.strip().split(":"))
        except ValueError:
            pass
    # Treat as UTC for simplicity (backend may apply tz)
    return int(d.replace(hour=hh, minute=mm).timestamp())


def compute_trip_type(enl_horaire: str, liv_horaire: str) -> str:
    """short_distance unless delivery-time is earlier than pickup-time (assumed next day)."""
    if not (enl_horaire and liv_horaire):
        return "short_distance"
    try:
        eh, em = map(int, enl_horaire.strip().split(":"))
        lh, lm = map(int, liv_horaire.strip().split(":"))
    except ValueError:
        return "short_distance"
    enl_min = eh * 60 + em
    liv_min = lh * 60 + lm
    return "long_distance" if liv_min < enl_min else "short_distance"


def build_comment(row: dict, resolution: dict) -> str:
    """Always include the Dispatch recurrence code. Only mention Dispatch values
    that were REPLACED by a default (so admin knows what to fix)."""
    lines = [f"Récurrence Dispatch: {row['code_ordre']}"]
    replaced = []
    c = resolution.get("customer", {})
    if c.get("match_method") in (None, "fallback_obspher") or not c.get("uuid") or c.get("match_method", "").startswith("fallback"):
        raw = f"{row.get('nom_client', '').strip()} ({row.get('code_client', '').strip()}) / {row.get('donneur_ordre', '').strip()}"
        replaced.append(f"  - Client: {raw} → user fallback Obspher")
    v = resolution.get("vehicle_skill")
    vcode = row.get("vehicule_code", "").strip()
    if not v and vcode:
        replaced.append(f"  - Véhicule: {vcode} — {row.get('vehicule_libelle', '').strip()} → vehicle_skills vides")
    d = resolution.get("driver_skill")
    dname = row.get("conducteur_nom", "").strip()
    if not d and dname:
        replaced.append(f"  - Conducteur: {dname} ({row.get('conducteur_code', '').strip()}) → driver_skills vides")
    if replaced:
        lines.append("Remplacé par défaut :")
        lines.extend(replaced)
    text = "\n".join(lines)
    if len(text) > 1990:
        text = text[:1987] + "..."
    return text


def build_references(row: dict) -> list[dict]:
    refs = []
    for key in ("reference_1", "reference_2", "reference_3"):
        val = row.get(key, "").strip()
        if val:
            refs.append({"label": val})
    return refs


def load_companies_by_user(conn) -> dict[str, str]:
    """user_uuid → company_uuid via professional_customers join."""
    sql = """
    SELECT u.uuid AS user_uuid, c.uuid AS company_uuid
    FROM users u
    JOIN registered_users ru ON ru.id = u.registered_id
    JOIN professional_users pu ON pu.id = ru.professional_id
    JOIN companies c ON c.id = pu.company_id
    """
    out = {}
    for r in dbmod.fetch_dict(conn, sql):
        out[r["user_uuid"]] = r["company_uuid"]
    return out


def customer_key(row: dict) -> str:
    return f"{row['nom_client'].strip()}|{row['donneur_ordre'].strip()}"


def vehicle_key(row: dict) -> str:
    return row["vehicule_code"].strip()


def driver_key(row: dict) -> str:
    return row["conducteur_nom"].strip()


def resolve_row(row: dict, entity_map: dict) -> dict:
    return {
        "customer": entity_map["customer"].get(customer_key(row), {}),
        "vehicle_skill": entity_map.get("vehicle_skill", {}).get(vehicle_key(row)),
        "driver_skill": entity_map.get("driver_skill", {}).get(driver_key(row)),
    }


def address_key(street_no: str, street: str, postal: str, city: str, country: str) -> str:
    return "|".join(p.strip().upper() for p in (street_no, street, postal, city, country))


def ensure_address(
    client: ApiClient,
    addr_entry: dict,
    company_uuid: str,
    row_side: dict,
    custom_name: str,
) -> tuple[str | None, str | None]:
    """POST /operator/company/address if not already cached for this company.
    Returns (address_id, error)."""
    cache = addr_entry.setdefault("address_ids", {})
    if company_uuid in cache:
        return cache[company_uuid], None

    payload = {
        "company_id": company_uuid,
        "billing": {
            "contact_email": BILLING_EMAIL,
            "contact_phone": BILLING_PHONE,
        },
        "address": {
            "custom_name": (custom_name or "Import Dispatch")[:100],
            "street_number": (row_side["street_number"] or "0")[:100],
            "street_name": (row_side["street_name"] or "N/A")[:100],
            "postal_code": (row_side["postal_code"] or "00000")[:100],
            "city": (row_side["city"] or "N/A")[:100],
            "country": row_side["country"] or "France",
            "floor_number": 0,
        },
    }
    status, resp = client.post("/operator/company/address", payload)
    if status not in (200, 201) or "data" not in resp:
        return None, f"address POST {status}: {resp}"
    addr_id = resp["data"].get("id")
    cache[company_uuid] = addr_id
    return addr_id, None


def build_waypoint(
    address_id: str,
    addr_entry: dict,
    row: dict,
    prefix: str,
) -> dict:
    phone = normalize_fr_phone(row.get(f"{prefix}_contact_tel", ""))
    email = row.get(f"{prefix}_contact_email", "").strip() or None
    fname, lname = split_contact_name(row.get(f"{prefix}_contact_nom", ""))
    wp: dict = {
        "address_id": address_id,
        "longitude": float(addr_entry.get("lng", 0.0)),
        "latitude": float(addr_entry.get("lat", 0.0)),
        "accessibility": DEFAULT_ACCESSIBILITY,
    }
    if fname:
        wp["contact_first_name"] = fname
    if lname:
        wp["contact_last_name"] = lname
    if phone:
        wp["contact_phone"] = phone
    if email:
        wp["contact_email"] = email
    return wp


def process_row(
    row: dict,
    entity_map: dict,
    address_map: dict,
    user_to_company: dict,
    fallback_company_uuid: str,
    default_depot_uuid: str,
    client: ApiClient,
    dry_run: bool,
    push_dispatch: bool,
    override_date: str | None = None,
) -> dict:
    """Returns result dict with status + ids."""
    result = {
        "code_ordre": row["code_ordre"],
        "order_id": "",
        "transport_id": "",
        "kind": "",
        "trip_type": "",
        "status": "",
        "error_msg": "",
    }
    result["kind"] = "rental" if is_rental_prestation(row["code_prestation"]) else "transport"
    resolution = resolve_row(row, entity_map)
    customer_uuid = resolution["customer"].get("uuid")
    if not customer_uuid:
        result["status"] = "failed"
        result["error_msg"] = f"no customer uuid for key {customer_key(row)}"
        return result

    company_uuid = user_to_company.get(customer_uuid, fallback_company_uuid)

    is_rental = result["kind"] == "rental"

    pickup_key = address_key(
        row["enl_no"], row["enl_rue"], row["enl_cp"], row["enl_ville"], row["enl_pays"] or "FR"
    )
    pickup_entry = address_map.get(pickup_key)
    if not pickup_entry:
        result["status"] = "failed"
        result["error_msg"] = "pickup address not geocoded"
        return result
    pickup_side = {
        "street_number": row["enl_no"],
        "street_name": row["enl_rue"],
        "postal_code": row["enl_cp"],
        "city": row["enl_ville"],
        "country": row["enl_pays"] or "FR",
    }

    dropoff_entry = None
    dropoff_side = None
    if not is_rental:
        dropoff_key = address_key(
            row["liv_no"], row["liv_rue"], row["liv_cp"], row["liv_ville"], row["liv_pays"] or "FR"
        )
        dropoff_entry = address_map.get(dropoff_key)
        if not dropoff_entry:
            result["status"] = "failed"
            result["error_msg"] = "dropoff address not geocoded"
            return result
        dropoff_side = {
            "street_number": row["liv_no"],
            "street_name": row["liv_rue"],
            "postal_code": row["liv_cp"],
            "city": row["liv_ville"],
            "country": row["liv_pays"] or "FR",
        }

    trip_type = "" if is_rental else compute_trip_type(row["enl_horaire"], row["liv_horaire"])
    result["trip_type"] = trip_type

    comment = build_comment(row, resolution)
    references = build_references(row)
    vehicle_skills = [resolution["vehicle_skill"]] if resolution["vehicle_skill"] else []
    driver_skills = [resolution["driver_skill"]] if resolution["driver_skill"] else []

    order_payload = {
        "customer_id": customer_uuid,
        "licenses": [],
        "services_id": [],
        "references": references,
        "vehicle_skills": vehicle_skills,
        "driver_skills": driver_skills,
        "comment": comment,
    }

    date_for_ts = override_date or row["date_debut"]
    pickup_ts = to_unix_ts(date_for_ts, row["enl_horaire"])
    dropoff_ts = to_unix_ts(date_for_ts, row["liv_horaire"])
    if pickup_ts is None or dropoff_ts is None:
        result["status"] = "failed"
        result["error_msg"] = f"invalid date/time: {row['date_debut']} / {row['enl_horaire']} / {row['liv_horaire']}"
        return result
    if not is_rental and trip_type == "long_distance" and dropoff_ts <= pickup_ts:
        dropoff_ts += 86400  # next day
    if is_rental and dropoff_ts <= pickup_ts:
        dropoff_ts += 86400  # overnight rental

    if dry_run:
        print(f"[DRY] {row['code_ordre']} ({result['kind']})  customer={customer_uuid[:8]}  "
              f"trip={trip_type}  comment:\n  " + comment.replace('\n', '\n  '))
        result["status"] = "dry_run"
        return result

    pickup_addr_id, err = ensure_address(client, pickup_entry, company_uuid, pickup_side, row.get("enl_nom", ""))
    if err:
        result["status"] = "failed"
        result["error_msg"] = err
        return result
    dropoff_addr_id = None
    if not is_rental:
        dropoff_addr_id, err = ensure_address(client, dropoff_entry, company_uuid, dropoff_side, row.get("liv_nom", ""))
        if err:
            result["status"] = "failed"
            result["error_msg"] = err
            return result

    status, resp = client.post("/operator/order", order_payload)
    if status not in (200, 201) or "data" not in resp:
        result["status"] = "failed"
        result["error_msg"] = f"order POST {status}: {resp}"
        return result
    order_id = resp["data"].get("id")
    result["order_id"] = order_id

    if is_rental:
        meeting_wp = build_waypoint(pickup_addr_id, pickup_entry, row, "enl")
        rental_payload = {
            "order_id": order_id,
            "departure_depot_id": default_depot_uuid,
            "return_depot_id": default_depot_uuid,
            "meeting_point": meeting_wp,
            "meeting_date": pickup_ts,
            "end_date": dropoff_ts,
            "meeting_research_range": 10,
            "end_research_range": 10,
            "requested_distance_km": 0.0,
        }
        status, resp = client.post("/operator/order/rental", rental_payload)
        if status not in (200, 201):
            result["status"] = "failed"
            result["error_msg"] = f"rental POST {status}: {resp}"
            return result
    else:
        pickup_wp = build_waypoint(pickup_addr_id, pickup_entry, row, "enl")
        dropoff_wp = build_waypoint(dropoff_addr_id, dropoff_entry, row, "liv")
        transport_payload = {
            "order_id": order_id,
            "pickup_waypoint": pickup_wp,
            "dropoff_waypoint": dropoff_wp,
            "pickup_condition": HORAIRE_MAP.get(row["enl_horaire_type"].strip(), "at"),
            "dropoff_condition": HORAIRE_MAP.get(row["liv_horaire_type"].strip(), "at"),
            "pickup_range_min": pickup_ts,
            "pickup_range_max": pickup_ts + 600,
            "dropoff_range_min": dropoff_ts,
            "dropoff_range_max": dropoff_ts + 600,
            "trip_type": trip_type,
        }
        status, resp = client.post("/operator/order/transport", transport_payload)
        if status not in (200, 201):
            result["status"] = "failed"
            result["error_msg"] = f"transport POST {status}: {resp}"
            return result
        result["transport_id"] = (resp.get("data") or {}).get("id", "")

    sub_codes = []
    for key_prefix in ("sp1", "sp2", "sp3", "sp4"):
        code = row.get(f"{key_prefix}_code", "").strip()
        if not code:
            continue
        entry = {"code": code}
        qte = row.get(f"{key_prefix}_qte", "").strip()
        if qte:
            try:
                entry["quantity"] = float(qte)
            except ValueError:
                pass
        sub_codes.append(entry)
    if row["code_prestation"].strip():
        simu_payload = {
            "order_id": order_id,
            "dispatch_service_code": row["code_prestation"].strip(),
            "dispatch_sub_service_code": sub_codes,
        }
        status, resp = client.post("/operator/order/dispatch/simulation", simu_payload)
        if status not in (200, 201):
            result["status"] = "partial"
            result["error_msg"] = f"dispatch_simulation POST {status}: {resp}"

    if not is_rental:
        merch_payload = {
            "order_id": order_id,
            "merchandises": [DEFAULT_MERCHANDISE],
        }
        status, resp = client.post("/operator/order/merchandises", merch_payload)
        if status not in (200, 201):
            result["status"] = "partial"
            result["error_msg"] = (result["error_msg"] + " | " if result["error_msg"] else "") + f"merchandises POST {status}: {resp}"

    if push_dispatch:
        status, resp = client.post("/operator/order/dispatch", {"order_id": order_id})
        if status not in (200, 201):
            result["status"] = "partial"
            result["error_msg"] = (result["error_msg"] + " | " if result["error_msg"] else "") + f"dispatch push POST {status}: {resp}"

    if not result["status"]:
        result["status"] = "ok"
    return result


def load_created(path: Path) -> dict[str, dict]:
    if not path.exists():
        return {}
    out = {}
    with path.open(encoding="utf-8") as f:
        for r in csv.DictReader(f):
            out[r["code_ordre"]] = r
    return out


def append_created(path: Path, row: dict, fieldnames: list[str]) -> None:
    new = not path.exists()
    with path.open("a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if new:
            w.writeheader()
        w.writerow(row)


def main() -> int:
    ap = argparse.ArgumentParser(description="Create orders in Obsher API from orders.csv")
    ap.add_argument("--config", default="import/config.json")
    ap.add_argument("--orders", default="output/orders.csv")
    ap.add_argument("--out-dir", default="output/import")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument(
        "--push-dispatch",
        action="store_true",
        help="After each order, POST /operator/order/dispatch to push it into the Dispatch TMS. "
             "Off by default — we don't want to pollute Dispatch while testing.",
    )
    ap.add_argument(
        "--override-date",
        default=None,
        help="Override date_debut for every order (ISO YYYY-MM-DD). "
             "Handy to make imported templates show up in the dashboard's default (today) window.",
    )
    ap.add_argument(
        "--codes",
        default=None,
        help="Comma-separated list of code_ordre to process (others skipped).",
    )
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
        print(f"→ override date_debut = {override_date} (from --override-date {args.override_date})")

    script_root = HERE.parent
    cfg = load_config((script_root / args.config).resolve())
    orders_csv = (script_root / args.orders).resolve()
    out_dir = (script_root / args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    entity_map = json.loads((out_dir / "entity_map.json").read_text(encoding="utf-8"))
    address_map_path = out_dir / "address_map.json"
    address_map = json.loads(address_map_path.read_text(encoding="utf-8"))
    created_path = out_dir / "created_orders.csv"
    created = load_created(created_path)

    fallback_user_uuid = cfg["fallback_user_uuid"]
    fallback_company_uuid = cfg.get("fallback_company_uuid", "db85f589-cb20-4650-ae81-e2f45a3eb245")
    default_depot_uuid = cfg["default_depot_uuid"]

    conn = dbmod.connect(**cfg["db"])
    try:
        user_to_company = load_companies_by_user(conn)
    finally:
        conn.close()
    if fallback_user_uuid not in user_to_company:
        user_to_company[fallback_user_uuid] = fallback_company_uuid

    log_path = out_dir / "api_calls.log"
    client = ApiClient(cfg["api"]["base_url"], cfg["api"]["token"], log_path=log_path)

    fieldnames = ["code_ordre", "order_id", "transport_id", "kind", "trip_type", "status", "error_msg"]
    results: list[dict] = []
    with orders_csv.open(encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

    processed = 0
    for i, row in enumerate(rows, 1):
        code = row["code_ordre"]
        if code_filter is not None and code not in code_filter:
            continue
        prior = created.get(code)
        if prior and prior.get("status") == "ok":
            continue
        result = process_row(
            row, entity_map, address_map, user_to_company,
            fallback_company_uuid, default_depot_uuid, client, args.dry_run,
            args.push_dispatch, override_date,
        )
        results.append(result)
        if not args.dry_run:
            append_created(created_path, result, fieldnames)
            # Flush address_map periodically (new address_ids appear inside it)
            if processed % 10 == 0:
                address_map_path.write_text(
                    json.dumps(address_map, ensure_ascii=False, indent=2), encoding="utf-8"
                )
        print(f"  [{i}/{len(rows)}] {code:10s}  {result['kind']:9s}  {result['status']:8s}  trip={result['trip_type']:14s}  {result['error_msg'][:100]}")
        processed += 1
        if args.limit and processed >= args.limit:
            break

    if not args.dry_run:
        address_map_path.write_text(
            json.dumps(address_map, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        write_summary(out_dir / "output.txt", results, len(rows))
    return 0


def write_summary(path: Path, results: list[dict], total_rows: int) -> None:
    ok = [r for r in results if r["status"] == "ok"]
    partial = [r for r in results if r["status"] == "partial"]
    failed = [r for r in results if r["status"] == "failed"]
    transports = [r for r in results if r["kind"] == "transport"]
    rentals = [r for r in results if r["kind"] == "rental"]
    short = [r for r in transports if r["trip_type"] == "short_distance"]
    long_ = [r for r in transports if r["trip_type"] == "long_distance"]

    lines: list[str] = []
    lines.append(f"Import Dispatch INNOVIA — {dt.datetime.now().isoformat(timespec='seconds')}")
    lines.append(f"Ordres traités ce run: {len(results)} / total CSV: {total_rows}")
    lines.append(f"  ok:      {len(ok)}")
    lines.append(f"  partial: {len(partial)}  (ordre créé mais simulation/merchandises KO)")
    lines.append(f"  failed:  {len(failed)}")
    lines.append("")
    lines.append(f"Kind — transport: {len(transports)} / rental: {len(rentals)}")
    lines.append(f"Trip type (transports) — short_distance: {len(short)} / long_distance: {len(long_)}")
    lines.append("")
    lines.append("=== transport: short_distance ===")
    for r in short:
        lines.append(f"  {r['code_ordre']}  {r['order_id']}")
    lines.append("")
    lines.append("=== transport: long_distance ===")
    for r in long_:
        lines.append(f"  {r['code_ordre']}  {r['order_id']}")
    lines.append("")
    lines.append("=== rental ===")
    for r in rentals:
        lines.append(f"  {r['code_ordre']}  {r['order_id']}")
    if failed or partial:
        lines.append("")
        lines.append("=== errors ===")
        for r in failed + partial:
            lines.append(f"  {r['code_ordre']}  [{r['status']}]  {r['error_msg']}")
    path.write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    sys.exit(main())
