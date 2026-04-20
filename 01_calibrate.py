"""
Interactive calibration: capture the fixed mouse coordinates of Dispatch INNOVIA tabs.

Usage:
    1. Open Dispatch INNOVIA, open the "Liste des ordres réguliers" window, click the 1st row.
    2. Open one order manually (F10) so the detail window is visible with all tabs.
    3. Run: python 01_calibrate.py
    4. Follow prompts: hover the mouse on each target, press Enter to capture.

Output: config.json at the repo root.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import pyautogui

CONFIG_PATH = Path(__file__).parent / "config.json"

TARGETS = [
    ("tabs.general",               "Onglet 'Général' (détail ordre)"),
    ("tabs.ordre",                 "Onglet 'Ordre'"),
    ("tabs.informations",          "Onglet 'Informations'"),
    ("tabs.attribution",           "Onglet 'Attribution'"),
    ("tabs.tarification",          "Onglet 'Tarification'"),
    ("sub_tabs.enlevement",        "Sous-onglet 'Enlèvement' (panneau gauche de l'onglet Ordre)"),
    ("sub_tabs.enlevement_contact","Sous-onglet 'Contact' côté Enlèvement"),
    ("sub_tabs.livraison",         "Sous-onglet 'Livraison' (panneau droit)"),
    ("sub_tabs.livraison_contact", "Sous-onglet 'Contact' côté Livraison"),
    ("close_button",               "Bouton fermer (flèche rouge en haut à droite du détail)"),
]


def capture_one(label: str) -> tuple[int, int]:
    print(f"\n>> {label}")
    print("   Positionne la souris dessus, puis appuie sur Entrée dans ce terminal...")
    input()
    x, y = pyautogui.position()
    print(f"   captured: ({x}, {y})")
    return x, y


def set_nested(d: dict, dotted_key: str, value):
    parts = dotted_key.split(".")
    cur = d
    for p in parts[:-1]:
        cur = cur.setdefault(p, {})
    cur[parts[-1]] = list(value)


def main() -> None:
    print("=" * 60)
    print("Calibration Dispatch INNOVIA — capture des coordonnées")
    print("=" * 60)
    print("Prérequis : une fenêtre 'Ordre régulier ...' ouverte avec tous les onglets visibles.")
    print()
    print("Petit délai de 3 secondes pour que tu puisses revenir sur la VM...")
    time.sleep(3)

    config: dict = {}
    for key, label in TARGETS:
        x, y = capture_one(label)
        set_nested(config, key, (x, y))

    CONFIG_PATH.write_text(json.dumps(config, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nConfig sauvegardée → {CONFIG_PATH}")
    print(json.dumps(config, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
