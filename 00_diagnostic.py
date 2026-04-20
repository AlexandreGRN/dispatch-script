"""
Diagnostic script for Dispatch INNOVIA extraction.

Prerequisites (manual):
- Dispatch INNOVIA open and maximized
- Window "Liste des ordres réguliers" visible
- One row selected (blue highlight)

What it does:
1. Takes a screenshot of the current state (list view)
2. Sends F10 to open the order detail
3. Waits for the detail window
4. Takes a screenshot of the initial tab
5. Dumps the UI Automation tree of the detail window
6. Closes with Escape + Enter
7. Saves everything in ./diagnostic/

After running, commit+push the ./diagnostic/ folder so we can analyze it.
"""

import time
import os
from pathlib import Path
from datetime import datetime

import pyautogui
from pywinauto import Desktop
from pywinauto.application import Application


OUT_DIR = Path(__file__).parent / "diagnostic"
OUT_DIR.mkdir(exist_ok=True)

WIN_TITLE_PATTERN = "Dispatch INNOVIA"
DETAIL_TITLE_PATTERN = "Ordre régulier"


def log(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")


def screenshot(name: str) -> Path:
    path = OUT_DIR / f"{name}.png"
    pyautogui.screenshot(str(path))
    log(f"  screenshot -> {path.name}")
    return path


def find_window(title_contains: str, timeout: float = 5.0):
    """Find a window whose title contains the given substring."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        for win in Desktop(backend="uia").windows():
            try:
                if title_contains.lower() in win.window_text().lower():
                    return win
            except Exception:
                continue
        time.sleep(0.2)
    return None


def dump_control_tree(window, filename: str) -> None:
    """Print all controls of a window to a text file."""
    path = OUT_DIR / filename
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"Window: {window.window_text()}\n")
        f.write(f"Rect: {window.rectangle()}\n\n")
        try:
            window.print_control_identifiers(depth=None, filename=str(path))
        except TypeError:
            # Fallback for older pywinauto versions
            import io
            import contextlib
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                window.print_control_identifiers(depth=None)
            f.write(buf.getvalue())
    log(f"  control tree -> {path.name}")


def main() -> None:
    log("=== Dispatch INNOVIA diagnostic ===")
    log(f"Output dir: {OUT_DIR}")

    # 1. Find main window
    log("Looking for Dispatch INNOVIA window...")
    main_win = find_window(WIN_TITLE_PATTERN, timeout=3)
    if not main_win:
        log("ERROR: Dispatch INNOVIA window not found. Is the app open?")
        return
    log(f"Found: {main_win.window_text()}")
    log(f"Rect: {main_win.rectangle()}")

    try:
        main_win.set_focus()
    except Exception as e:
        log(f"  warning: could not set focus: {e}")

    time.sleep(0.5)
    screenshot("01_list_initial")

    # 2. Dump tree of main window (to find the list grid control)
    log("Dumping control tree of main window...")
    try:
        dump_control_tree(main_win, "tree_main.txt")
    except Exception as e:
        log(f"  warning: tree dump failed: {e}")

    # 3. Press F10 to open detail
    log("Pressing F10 to open detail...")
    pyautogui.press("f10")
    time.sleep(3.0)  # detail window takes ~2s to open

    # 4. Find detail window
    detail = find_window(DETAIL_TITLE_PATTERN, timeout=5)
    if not detail:
        log("ERROR: detail window not found after F10. Check if F10 actually opens the order.")
        screenshot("02_after_f10_no_detail")
        return
    log(f"Detail window: {detail.window_text()}")
    log(f"Rect: {detail.rectangle()}")

    try:
        detail.set_focus()
    except Exception as e:
        log(f"  warning: could not set focus on detail: {e}")
    time.sleep(0.5)

    screenshot("02_detail_initial_tab")

    # 5. Dump tree of detail window
    log("Dumping control tree of detail window...")
    try:
        dump_control_tree(detail, "tree_detail.txt")
    except Exception as e:
        log(f"  warning: detail tree dump failed: {e}")

    # 6. Try to find tabs by name and click each, screenshotting
    tab_names = ["Général", "Ordre", "Informations", "Attribution", "Tarification", "Tracking"]
    for i, name in enumerate(tab_names, start=1):
        log(f"Looking for tab: {name}")
        try:
            tab = detail.child_window(title=name, control_type="TabItem")
            if tab.exists(timeout=0.5):
                tab.click_input()
                time.sleep(0.8)
                screenshot(f"03_tab_{i}_{name.lower().replace('é', 'e')}")
                log(f"  OK: tab '{name}' clicked")
            else:
                log(f"  SKIP: tab '{name}' not found as TabItem")
        except Exception as e:
            log(f"  ERROR clicking tab '{name}': {e}")

    # 7. Close detail: Escape + Enter
    log("Closing detail (Escape + Enter)...")
    pyautogui.press("escape")
    time.sleep(0.8)
    screenshot("04_after_escape")
    pyautogui.press("enter")
    time.sleep(1.5)
    screenshot("05_back_to_list")

    log("=== Diagnostic complete. ===")
    log(f"Check the folder: {OUT_DIR}")
    log("Commit + push the 'diagnostic/' folder to share the result.")


if __name__ == "__main__":
    main()
