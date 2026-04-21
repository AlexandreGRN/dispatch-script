"""
Resolve Dispatch codes/names from orders.csv into Obsher UUIDs.

Reads:
  - ../output/orders.csv  (each row has code_client/nom_client/donneur_ordre,
                           vehicule_code, conducteur_nom)
  - config.json           (DB creds + fallback UUID + thresholds)

DB lookups (direct MySQL, no API):
  - customers : user UUID for (donneur_ordre, nom_client)
                Step 1 find company by id_dispatch OR by fuzzy name.
                Step 2 find professional_customer user inside that company by fuzzy name.
                Fallback: user UUID db85f589-…e2f45a3eb245 (pc1.admin@obspher.com).
  - vehicle_skill : exact name match on vehicule_code (format X.XX.YY).
  - driver_skill  : fuzzy match on conducteur_nom (token-sort, both name orders).

Writes:
  - ../output/import/entity_map.json   (keyed by Dispatch identifier)
  - ../output/import/missing_entities.csv

Usage:
  python 03_resolve_entities.py [--config config.json] [--threshold 0.85]
  python 03_resolve_entities.py --source tsv --snapshot prod_snapshot/
                                               # read from 4 TSV files exported
                                               # via an SSH tunnel (no DB access
                                               # from this machine)
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from lib.db import connect, fetch_dict
from lib.fuzzy import best_match, normalize, similarity

UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE)

ROOT = Path(__file__).parent
CSV_IN = ROOT.parent / "output" / "orders.csv"
OUT_DIR = ROOT.parent / "output" / "import"
ENTITY_MAP = OUT_DIR / "entity_map.json"
MISSING_CSV = OUT_DIR / "missing_entities.csv"

DEFAULT_CONFIG = ROOT / "config.json"


def load_config(path: Path) -> dict:
    if not path.exists():
        print(f"ERROR: {path} not found. Copy config.example.json → config.json.",
              file=sys.stderr)
        sys.exit(1)
    return json.loads(path.read_text(encoding="utf-8"))


def load_orders(path: Path) -> list[dict]:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


# ─────────────────────────── DB fetch helpers ───────────────────────────

def fetch_companies(conn) -> list[dict]:
    return fetch_dict(conn, """
        SELECT id, uuid, name, id_dispatch
        FROM companies
    """)


def fetch_professional_customers(conn) -> list[dict]:
    return fetch_dict(conn, """
        SELECT u.uuid       AS user_uuid,
               u.first_name,
               u.last_name,
               pu.company_id AS company_id
        FROM users u
        JOIN registered_users ru ON ru.id = u.registered_id
        JOIN professional_users pu ON pu.id = ru.professional_id
        WHERE pu.professional_customer_id IS NOT NULL
    """)


def fetch_driver_skills(conn) -> list[dict]:
    return fetch_dict(conn, "SELECT uuid, name FROM driver_skills WHERE archived_at IS NULL")


def fetch_vehicle_skills(conn) -> list[dict]:
    return fetch_dict(conn, "SELECT uuid, name FROM vehicle_skills WHERE archived_at IS NULL")


# ─────────────────────────── TSV loaders ────────────────────────────────
# These mirror the DB fetch_* functions for when we only have a snapshot
# exported via `mysql -BN` through an SSH tunnel (prod use case).

def _read_tsv(path: Path) -> list[list[str]]:
    if not path.exists():
        print(f"ERROR: missing {path}", file=sys.stderr)
        sys.exit(1)
    rows: list[list[str]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n").rstrip("\r")
            if not line:
                continue
            rows.append(line.split("\t"))
    return rows


def load_companies_tsv(path: Path) -> list[dict]:
    """TSV export of: SELECT name, id_dispatch, uuid FROM companies …"""
    out: list[dict] = []
    for i, row in enumerate(_read_tsv(path)):
        if len(row) < 3:
            continue
        name, id_dispatch, uuid = row[0], row[1], row[2]
        if not UUID_RE.match(uuid):
            continue
        out.append({"id": i + 1, "uuid": uuid, "name": name, "id_dispatch": id_dispatch})
    return out


def load_professional_customers_tsv(path: Path, companies: list[dict]) -> list[dict]:
    """TSV export of: SELECT uuid, first_name, last_name, company_name, id_dispatch …
    Joins back to the synthetic company.id from load_companies_tsv by (name, id_dispatch)."""
    key_to_id: dict[tuple[str, str], int] = {
        (c["name"], c["id_dispatch"]): c["id"] for c in companies
    }
    out: list[dict] = []
    skipped = 0
    for row in _read_tsv(path):
        if len(row) < 5:
            continue
        uuid, first_name, last_name, company_name, id_dispatch = row[:5]
        if not UUID_RE.match(uuid):
            skipped += 1
            continue
        company_id = key_to_id.get((company_name, id_dispatch))
        if company_id is None:
            skipped += 1
            continue
        out.append({
            "user_uuid": uuid, "first_name": first_name, "last_name": last_name,
            "company_id": company_id,
        })
    if skipped:
        print(f"  (skipped {skipped} user row(s) with invalid UUID or orphan company)")
    return out


def load_skills_tsv(path: Path) -> list[dict]:
    """TSV export of: SELECT name, uuid FROM {driver,vehicle}_skills …"""
    out: list[dict] = []
    for row in _read_tsv(path):
        if len(row) < 2:
            continue
        name, uuid = row[0], row[1]
        if not UUID_RE.match(uuid):
            continue
        out.append({"name": name, "uuid": uuid})
    return out


# ─────────────────────────── Resolution ─────────────────────────────────

def resolve_customer(
    code_client: str,
    nom_client: str,
    donneur_ordre: str,
    companies_by_id_dispatch: dict[str, dict],
    companies: list[dict],
    users_by_company: dict[int, list[dict]],
    threshold: float,
    prefer_id_dispatch: bool,
) -> dict:
    """Return a dict describing how this customer was resolved (including fallback)."""
    result: dict = {
        "company_id": None, "company_uuid": None, "company_name_matched": None,
        "user_uuid": None, "user_name_matched": None,
        "match_method": None, "score": 0.0, "reason": None,
    }

    # Step 1 — find the company.
    company: dict | None = None
    if prefer_id_dispatch and code_client:
        company = companies_by_id_dispatch.get(code_client.strip())
        if company:
            result["match_method"] = "id_dispatch"
            result["score"] = 1.0

    if company is None and nom_client:
        # Exact uppercase match
        norm_q = normalize(nom_client)
        for c in companies:
            if normalize(c["name"]) == norm_q:
                company = c
                result["match_method"] = "exact_name"
                result["score"] = 1.0
                break

    if company is None and nom_client:
        # Fuzzy
        hit = best_match(
            nom_client,
            [(c["name"], c) for c in companies],
            threshold=threshold,
        )
        if hit:
            company, score, matched = hit
            result["match_method"] = "fuzzy_name"
            result["score"] = round(score, 3)

    if company is None:
        result["reason"] = "company_not_found"
        return result

    result["company_id"] = company["id"]
    result["company_uuid"] = company["uuid"]
    result["company_name_matched"] = company["name"]

    # Step 2 — find the user inside that company.
    users = users_by_company.get(company["id"], [])
    if not users:
        result["reason"] = "company_has_no_professional_customer"
        return result

    if not donneur_ordre:
        # Pick any user in the company as a placeholder, flag it.
        u = users[0]
        result["user_uuid"] = u["user_uuid"]
        result["user_name_matched"] = f'{u["first_name"]} {u["last_name"]}'
        result["match_method"] = (result["match_method"] or "") + "+first_user_fallback"
        result["reason"] = "donneur_ordre_empty"
        return result

    # Fuzzy match against "First Last" AND "Last First" — we normalize both sides.
    candidates: list[tuple[str, dict]] = []
    for u in users:
        fl = f'{u["first_name"]} {u["last_name"]}'
        candidates.append((fl, u))

    hit = best_match(donneur_ordre, candidates, threshold=threshold)
    if hit:
        u, score, matched = hit
        result["user_uuid"] = u["user_uuid"]
        result["user_name_matched"] = matched
        result["match_method"] = (result["match_method"] or "") + "+fuzzy_user"
        result["score"] = round(min(result["score"] or 1.0, score), 3)
        return result

    result["reason"] = "user_not_found_in_company"
    return result


def resolve_vehicle_skill(code: str, skills_by_name: dict[str, str]) -> str | None:
    return skills_by_name.get((code or "").strip()) if code else None


def resolve_driver_skill(
    name: str,
    driver_skills: list[dict],
    threshold: float,
) -> tuple[str, str, float] | None:
    if not name:
        return None
    hit = best_match(
        name,
        [(s["name"], s) for s in driver_skills],
        threshold=threshold,
    )
    if hit:
        s, score, matched = hit
        return s["uuid"], matched, round(score, 3)
    return None


# ─────────────────────────── Main ───────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--threshold", type=float, default=None,
                        help="override fuzzy threshold from config")
    parser.add_argument("--source", choices=["db", "tsv"], default="db",
                        help="db = live MySQL (default) ; tsv = read the 4 snapshot "
                             "files from --snapshot (for prod via SSH tunnel export)")
    parser.add_argument("--snapshot", type=Path, default=ROOT / "prod_snapshot",
                        help="directory containing {companies,users,vehicle_skills,"
                             "driver_skills}_prod.tsv (used with --source tsv)")
    parser.add_argument("--out-dir", type=Path, default=None,
                        help="output directory (default: ../output/import)")
    args = parser.parse_args()

    cfg = load_config(args.config)
    threshold = args.threshold if args.threshold is not None else cfg.get("fuzzy_threshold", 0.85)
    fallback_user = cfg.get("fallback_user_uuid")
    if not fallback_user:
        print("ERROR: config.fallback_user_uuid is required.", file=sys.stderr)
        sys.exit(1)
    prefer_id_dispatch = cfg.get("prefer_id_dispatch_match", False)

    print(f"Loading orders from {CSV_IN} …")
    orders = load_orders(CSV_IN)
    print(f"  {len(orders)} rows.")

    if args.source == "db":
        print("Connecting to DB …")
        conn = connect(**cfg["db"])
        try:
            companies = fetch_companies(conn)
            prof_customers = fetch_professional_customers(conn)
            driver_skills = fetch_driver_skills(conn)
            vehicle_skills = fetch_vehicle_skills(conn)
        finally:
            conn.close()
    else:
        snap = args.snapshot
        print(f"Loading snapshot TSVs from {snap} …")
        companies = load_companies_tsv(snap / "companies_prod.tsv")
        prof_customers = load_professional_customers_tsv(
            snap / "users_prod.tsv", companies,
        )
        driver_skills = load_skills_tsv(snap / "driver_skills_prod.tsv")
        vehicle_skills = load_skills_tsv(snap / "vehicle_skills_prod.tsv")

    print(f"  companies={len(companies)}  "
          f"professional_customers={len(prof_customers)}  "
          f"driver_skills={len(driver_skills)}  "
          f"vehicle_skills={len(vehicle_skills)}")

    companies_by_id_dispatch: dict[str, dict] = {}
    for c in companies:
        if c["id_dispatch"]:
            companies_by_id_dispatch.setdefault(c["id_dispatch"].strip(), c)
    users_by_company: dict[int, list[dict]] = defaultdict(list)
    for u in prof_customers:
        users_by_company[u["company_id"]].append(u)
    vehicle_skill_by_name: dict[str, str] = {s["name"]: s["uuid"] for s in vehicle_skills}

    # Accumulators
    entity_map: dict = {"customer": {}, "vehicle_skill": {}, "driver_skill": {}}
    # missing_by_key groups by (kind, entity_identifier) so all affected orders
    # collapse into one row with a comma-separated `order_codes` field.
    missing_by_key: dict[tuple[str, str], dict] = {}

    def log_missing(kind: str, entity_key: str, row: dict, order_code: str) -> None:
        k = (kind, entity_key)
        if k not in missing_by_key:
            missing_by_key[k] = {**row, "order_codes": [], "affected_count": 0}
        missing_by_key[k]["order_codes"].append(order_code)
        missing_by_key[k]["affected_count"] += 1

    seen_customer_keys: set[str] = set()
    seen_vehicle_codes: set[str] = set()
    seen_driver_names: set[str] = set()

    # ─── Resolve unique keys once, then iterate all orders to collect impact ──

    # Pass 1: populate entity_map for unique keys
    for row in orders:
        key = f"{row.get('nom_client','').strip()}|{row.get('donneur_ordre','').strip()}"
        if key in seen_customer_keys:
            continue
        seen_customer_keys.add(key)
        res = resolve_customer(
            (row.get("code_client") or "").strip(),
            (row.get("nom_client") or "").strip(),
            (row.get("donneur_ordre") or "").strip(),
            companies_by_id_dispatch, companies, users_by_company,
            threshold=threshold, prefer_id_dispatch=prefer_id_dispatch,
        )
        if res["user_uuid"]:
            entity_map["customer"][key] = {
                "uuid": res["user_uuid"],
                "company_uuid": res["company_uuid"],
                "company_name_matched": res["company_name_matched"],
                "user_name_matched": res["user_name_matched"],
                "match_method": res["match_method"],
                "score": res["score"],
            }
        else:
            entity_map["customer"][key] = {
                "uuid": fallback_user,
                "match_method": "fallback_obspher",
                "reason": res["reason"],
                "company_name_matched": res["company_name_matched"],
            }

    for row in orders:
        code = (row.get("vehicule_code") or "").strip()
        if not code or code in seen_vehicle_codes:
            continue
        seen_vehicle_codes.add(code)
        entity_map["vehicle_skill"][code] = vehicle_skill_by_name.get(code)

    for row in orders:
        name = (row.get("conducteur_nom") or "").strip()
        if not name or name in seen_driver_names:
            continue
        seen_driver_names.add(name)
        hit = resolve_driver_skill(name, driver_skills, threshold=threshold)
        if hit:
            uuid, matched, score = hit
            entity_map["driver_skill"][name] = {
                "uuid": uuid, "matched": matched, "score": score,
            }
        else:
            entity_map["driver_skill"][name] = None

    # Pass 2: walk every order to attribute fallbacks to their code_ordre(s)
    for row in orders:
        code_ordre = (row.get("code_ordre") or "").strip()
        nom = (row.get("nom_client") or "").strip()
        donneur = (row.get("donneur_ordre") or "").strip()
        ckey = f"{nom}|{donneur}"

        cust = entity_map["customer"].get(ckey, {})
        if cust.get("match_method") == "fallback_obspher":
            log_missing(
                "customer", ckey,
                {
                    "kind": "customer", "csv_name": nom, "csv_donneur": donneur,
                    "csv_code": (row.get("code_client") or "").strip(),
                    "reason": cust.get("reason") or "unknown",
                    "fallback_used": "obspher_user",
                    "company_name_matched": cust.get("company_name_matched") or "",
                    "match_score": 0.0,
                },
                code_ordre,
            )
        elif cust.get("match_method", "").endswith("first_user_fallback"):
            log_missing(
                "customer_donneur", ckey,
                {
                    "kind": "customer_donneur_empty", "csv_name": nom,
                    "csv_donneur": donneur,
                    "csv_code": (row.get("code_client") or "").strip(),
                    "reason": "donneur_ordre_empty_used_first_user",
                    "fallback_used": cust.get("user_name_matched") or "first_user_in_company",
                    "company_name_matched": cust.get("company_name_matched") or "",
                    "match_score": cust.get("score") or 0.0,
                },
                code_ordre,
            )

        vcode = (row.get("vehicule_code") or "").strip()
        if vcode and entity_map["vehicle_skill"].get(vcode) is None:
            log_missing(
                "vehicle_skill", vcode,
                {
                    "kind": "vehicle_skill",
                    "csv_name": (row.get("vehicule_libelle") or "").strip(),
                    "csv_donneur": "", "csv_code": vcode,
                    "reason": "skill_not_found",
                    "fallback_used": "empty_array",
                    "company_name_matched": "",
                    "match_score": 0.0,
                },
                code_ordre,
            )

        dname = (row.get("conducteur_nom") or "").strip()
        if dname and entity_map["driver_skill"].get(dname) is None:
            log_missing(
                "driver_skill", dname,
                {
                    "kind": "driver_skill", "csv_name": dname, "csv_donneur": "",
                    "csv_code": (row.get("conducteur_code") or "").strip(),
                    "reason": "skill_not_found",
                    "fallback_used": "empty_array",
                    "company_name_matched": "",
                    "match_score": 0.0,
                },
                code_ordre,
            )

    # ─── Write outputs ────────────────────────────────────────────────────
    out_dir = args.out_dir or OUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    entity_map_path = out_dir / "entity_map.json"
    missing_path = out_dir / "missing_entities.csv"
    entity_map_path.write_text(
        json.dumps(entity_map, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    miss_fields = [
        "kind", "csv_name", "csv_donneur", "csv_code",
        "company_name_matched", "reason", "fallback_used",
        "match_score", "affected_count", "order_codes",
    ]
    # Sort by affected_count desc so the admin tackles the biggest blockers first.
    missing_rows = sorted(
        missing_by_key.values(),
        key=lambda r: (-r["affected_count"], r["kind"], r.get("csv_name", "")),
    )
    with open(missing_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=miss_fields)
        w.writeheader()
        for row in missing_rows:
            out = {k: row.get(k, "") for k in miss_fields}
            out["order_codes"] = ",".join(row["order_codes"])
            w.writerow(out)

    # ─── Summary ──────────────────────────────────────────────────────────
    c_total = len(entity_map["customer"])
    c_fallback = sum(1 for v in entity_map["customer"].values()
                     if v.get("match_method") == "fallback_obspher")
    v_total = len(entity_map["vehicle_skill"])
    v_ok = sum(1 for v in entity_map["vehicle_skill"].values() if v)
    d_total = len(entity_map["driver_skill"])
    d_ok = sum(1 for v in entity_map["driver_skill"].values() if v)

    print()
    print("=" * 60)
    print(f"Customers:      {c_total - c_fallback}/{c_total} matched "
          f"({c_fallback} → fallback Obspher user)")
    print(f"Vehicle skills: {v_ok}/{v_total} matched")
    print(f"Driver skills:  {d_ok}/{d_total} matched")
    total_affected = sum(r["affected_count"] for r in missing_by_key.values())
    print(f"→ {entity_map_path}")
    print(f"→ {missing_path}  ({len(missing_by_key)} unique missing entities, "
          f"{total_affected} order references)")


if __name__ == "__main__":
    main()
