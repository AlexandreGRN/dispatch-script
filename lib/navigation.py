"""UI navigation helpers for Dispatch INNOVIA."""

from __future__ import annotations

import re
import time
from pathlib import Path

import pyautogui
from pywinauto import Desktop

pyautogui.FAILSAFE = True
pyautogui.PAUSE = 0.05

DETAIL_TITLE_SUBSTR = "Ordre régulier"
LIST_TITLE_SUBSTR = "Dispatch INNOVIA"

# Regex to pull the order code from the detail window title.
# Example title: "Ordre régulier 2B1703M CENTRAVET BDX B33 04"
CODE_RE = re.compile(r"Ordre r[ée]gulier\s+([A-Z0-9_-]+)")


def find_window(title_substring: str, timeout: float = 5.0):
    """Return the first top-level window whose title contains the substring, or None."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        for win in Desktop(backend="uia").windows():
            try:
                if title_substring.lower() in win.window_text().lower():
                    return win
            except Exception:
                continue
        time.sleep(0.15)
    return None


def wait_detail_window(timeout: float = 6.0):
    """Wait for the 'Ordre régulier ...' window to appear."""
    return find_window(DETAIL_TITLE_SUBSTR, timeout=timeout)


def wait_detail_closed(timeout: float = 5.0) -> bool:
    """Wait until no 'Ordre régulier ...' window is visible."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if find_window(DETAIL_TITLE_SUBSTR, timeout=0.1) is None:
            return True
        time.sleep(0.15)
    return False


def extract_code_from_title(window) -> str | None:
    """Pull the order code (first token after 'Ordre régulier') from the window title."""
    if window is None:
        return None
    try:
        m = CODE_RE.search(window.window_text())
        return m.group(1) if m else None
    except Exception:
        return None


def click_at(xy: tuple[int, int] | list[int], pause: float = 0.4) -> None:
    x, y = xy
    pyautogui.click(int(x), int(y))
    time.sleep(pause)


def press(key: str, pause: float = 0.2) -> None:
    pyautogui.press(key)
    time.sleep(pause)


def screenshot(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    pyautogui.screenshot(str(path))
    return path


def _screenshot_quality(path: Path) -> float:
    """Return std dev of pixel values — low value = mostly uniform = likely bad render."""
    from PIL import Image, ImageStat
    import statistics
    img = Image.open(path).convert("L")
    stat = ImageStat.Stat(img)
    return stat.stddev[0]


def screenshot_with_retry(
    path: Path,
    click_xy: tuple[int, int] | list[int] | None = None,
    min_quality: float = 8.0,
    max_attempts: int = 3,
    retry_pause: float = 2.5,
) -> tuple[Path, bool]:
    """Take a screenshot, retry if quality is too low. Returns (path, is_ok)."""
    for attempt in range(max_attempts):
        if attempt > 0 and click_xy is not None:
            click_at(click_xy, pause=retry_pause)
        elif attempt > 0:
            time.sleep(retry_pause)
        screenshot(path)
        quality = _screenshot_quality(path)
        if quality >= min_quality:
            return path, True
    return path, False


def close_detail(close_button_xy: tuple[int, int] | list[int] | None = None) -> bool:
    """Close the detail window.

    Primary path: Escape → wait for 'Voulez-vous quitter la saisie ?' dialog → Enter to validate 'Oui'.
    Fallback: click the red close button at close_button_xy.
    """
    pyautogui.press("escape")
    time.sleep(0.6)
    pyautogui.press("enter")
    time.sleep(0.8)

    if wait_detail_closed(timeout=2.0):
        return True

    # Fallback: try the close button
    if close_button_xy is not None:
        click_at(close_button_xy, pause=0.4)
        time.sleep(0.4)
        pyautogui.press("enter")  # in case a confirm dialog still shows
        time.sleep(0.8)
        return wait_detail_closed(timeout=2.0)

    return False


def open_current_order() -> object | None:
    """Press F10 to open the currently selected order and return the detail window."""
    pyautogui.press("f10")
    return wait_detail_window(timeout=6.0)


def fire_open_next() -> None:
    """Press F10 without waiting — use when the next window can load in the background."""
    pyautogui.press("f10")
    time.sleep(0.1)


def next_row(pause: float = 0.3) -> None:
    """Move list selection to the next order (auto-scrolls in the Dispatch list)."""
    pyautogui.press("down")
    time.sleep(pause)


