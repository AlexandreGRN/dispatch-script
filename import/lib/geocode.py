"""Geocoder using api-adresse.data.gouv.fr (BAN — Base Adresse Nationale).

Free, no auth, 50 req/s. Covers all of France (all our addresses are FR).
Falls back to Nominatim only if BAN has zero results for an address.

ORS dev can't geocode (ors/proxy → ors_service_error). Nominatim public would
rate-limit us at 1 req/s over 209 addresses, so BAN is the sane choice.
"""

from __future__ import annotations

import time
from typing import Any

import requests

BAN_URL = "https://api-adresse.data.gouv.fr/search/"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_UA = "ObspherDispatchImport/1.0 (alexandre.guerin@obspher.com)"


class Geocoder:
    def __init__(self) -> None:
        self._last_nominatim = 0.0

    def search(
        self, street_no: str, street: str, postal: str, city: str, country: str
    ) -> dict[str, Any] | None:
        country = (country or "FR").upper()
        query = _format_query(street_no, street, postal, city)
        if not query:
            return None
        if country == "FR":
            hit = self._ban(query, postal, city)
            if hit:
                return hit
            if (street or street_no) and postal and city:
                hit = self._ban(f"{postal} {city}", postal, city)
                if hit:
                    hit["source"] = "ban_city_fallback"
                    return hit
        return self._nominatim(f"{query}, {country}", country)

    def _ban(self, query: str, postal: str, city: str) -> dict[str, Any] | None:
        try:
            resp = requests.get(
                BAN_URL,
                params={"q": query, "limit": 1, "postcode": postal} if postal else {"q": query, "limit": 1},
                timeout=15,
            )
        except (requests.Timeout, requests.ConnectionError):
            return None
        if resp.status_code != 200:
            return None
        try:
            data = resp.json()
        except ValueError:
            return None
        feats = data.get("features") or []
        if not feats:
            return None
        f = feats[0]
        props = f.get("properties", {})
        lng, lat = f["geometry"]["coordinates"]
        return {
            "lat": float(lat),
            "lng": float(lng),
            "display_name": props.get("label", ""),
            "score": props.get("score"),
            "source": "ban",
            "postcode": props.get("postcode"),
            "city": props.get("city"),
        }

    def _nominatim(self, query: str, country: str) -> dict[str, Any] | None:
        elapsed = time.monotonic() - self._last_nominatim
        if elapsed < 1.1:
            time.sleep(1.1 - elapsed)
        self._last_nominatim = time.monotonic()
        try:
            resp = requests.get(
                NOMINATIM_URL,
                params={"q": query, "format": "json", "limit": 1, "countrycodes": country.lower()},
                headers={"User-Agent": NOMINATIM_UA},
                timeout=20,
            )
        except (requests.Timeout, requests.ConnectionError):
            return None
        if resp.status_code != 200:
            return None
        try:
            results = resp.json()
        except ValueError:
            return None
        if not results:
            return None
        r = results[0]
        try:
            return {
                "lat": float(r["lat"]),
                "lng": float(r["lon"]),
                "display_name": r.get("display_name", ""),
                "source": "nominatim",
            }
        except (KeyError, ValueError):
            return None


def _format_query(street_no: str, street: str, postal: str, city: str) -> str:
    parts = [p for p in [street_no, street] if p]
    first = " ".join(parts)
    return ", ".join(p for p in [first, f"{postal} {city}".strip()] if p)
