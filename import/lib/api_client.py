"""Thin HTTP client for the Obsher API with JWT bearer + retry + request log."""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

import requests


class ApiClient:
    def __init__(
        self,
        base_url: str,
        token: str,
        log_path: Path | None = None,
        timeout: int = 30,
        retries: int = 3,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.retries = retries
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )
        self.logger = logging.getLogger("api_client")
        if log_path and not self.logger.handlers:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            h = logging.FileHandler(log_path, encoding="utf-8")
            h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
            self.logger.addHandler(h)
            self.logger.setLevel(logging.INFO)

    def request(self, method: str, path: str, json_body: Any = None) -> tuple[int, Any]:
        url = f"{self.base_url}{path}"
        last_exc: Exception | None = None
        for attempt in range(1, self.retries + 1):
            try:
                resp = self.session.request(
                    method, url, json=json_body, timeout=self.timeout
                )
                status = resp.status_code
                try:
                    payload = resp.json()
                except ValueError:
                    payload = {"_raw": resp.text}
                self.logger.info(
                    "%s %s -> %d (attempt %d)", method, path, status, attempt
                )
                if status >= 500 and attempt < self.retries:
                    time.sleep(2**attempt)
                    continue
                return status, payload
            except (requests.Timeout, requests.ConnectionError) as e:
                last_exc = e
                self.logger.warning(
                    "%s %s network error on attempt %d: %s", method, path, attempt, e
                )
                if attempt < self.retries:
                    time.sleep(2**attempt)
                    continue
        raise RuntimeError(f"{method} {path} failed after {self.retries} retries") from last_exc

    def post(self, path: str, body: Any) -> tuple[int, Any]:
        return self.request("POST", path, body)

    def get(self, path: str) -> tuple[int, Any]:
        return self.request("GET", path, None)


def load_config(path: Path) -> dict:
    with path.open(encoding="utf-8") as f:
        return json.load(f)
