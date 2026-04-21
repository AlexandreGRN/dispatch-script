"""MySQL connection helper — direct TCP to the Obspher DB for bulk lookups.

Uses mysql-connector-python (already installed) rather than hitting the API
for reads, since the /operator/customer/companies route is not wired in
staging and we need names + UUIDs in the same round-trip.
"""

from __future__ import annotations

import mysql.connector
from mysql.connector.cursor import MySQLCursorDict


def connect(host: str, user: str, password: str, database: str, port: int = 3306):
    """Open a mysql-connector connection. Caller is responsible for closing."""
    return mysql.connector.connect(
        host=host, port=port, user=user, password=password, database=database,
        charset="utf8mb4", use_unicode=True,
    )


def fetch_dict(conn, query: str, params: tuple = ()) -> list[dict]:
    """Run SELECT, return rows as list of dicts."""
    cur: MySQLCursorDict = conn.cursor(dictionary=True)
    try:
        cur.execute(query, params)
        return cur.fetchall()
    finally:
        cur.close()
