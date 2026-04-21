"""UI navigation helpers for Dispatch INNOVIA."""

from __future__ import annotations

import re
import time
from pathlib import Path

import pyautogui
from pywinauto import Desktop

pyautogui.FAILSAFE = True
pyautogui.PAUSE = 0.05

# Sweep density: tune these if Dispatch still misses repaints between rows.
# Total points = SWEEP_ROWS * SWEEP_COLS. Each move takes ~SWEEP_MOVE_S seconds.
# Current: 20 rows × 10 cols = 200 points × 0.02s ≈ 4s per sweep.
SWEEP_ROWS = 20
SWEEP_COLS = 10
SWEEP_MOVE_S = 0.02
SWEEP_TAIL_PAUSE_S = 0.5

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


def force_redraw_hover(xy: tuple[int, int] | list[int]) -> None:
    """Move the cursor over a target before interacting with it.

    Workaround for Dispatch INNOVIA under RDP: the app only repaints its
    controls when the mouse hovers over them. Without this, pyautogui clicks
    a stale area and screenshots capture un-rendered widgets.

    Performs a small L-shape motion around the target so that Dispatch's
    hover events fire reliably (a single moveTo is sometimes optimized-out
    by the RDP client).
    """
    x, y = int(xy[0]), int(xy[1])
    pyautogui.moveTo(x - 30, y - 30, duration=0.15)
    pyautogui.moveTo(x + 10, y + 10, duration=0.15)
    pyautogui.moveTo(x, y, duration=0.10)
    time.sleep(0.3)  # let Dispatch process the hover and repaint


def hover_click(xy: tuple[int, int] | list[int], pause: float = 0.6) -> None:
    """Force redraw by hovering, then click. Use everywhere we need a freshly
    rendered view (tab switches, etc.)."""
    force_redraw_hover(xy)
    pyautogui.click(int(xy[0]), int(xy[1]))
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


def _sweep_content_area() -> None:
    """Drag the cursor across the entire screen to force Dispatch to repaint
    every control before the screenshot. Works around RDP rendering lag where
    widgets stay blank until hovered.

    Boustrophédon sweep covering the FULL screen (any resolution, detected via
    pyautogui.size()). Very dense grid — SWEEP_ROWS × SWEEP_COLS points — so
    every widget gets a hover event in both X and Y axes. Each row alternates
    direction so the cursor sweeps continuously.

    Tune SWEEP_ROWS / SWEEP_COLS / SWEEP_MOVE_S at top of this file if still
    not dense enough. Called before every tab screenshot, so the full UI gets
    hovered on every tab.
    """
    screen_w, screen_h = pyautogui.size()
    margin_x = max(20, screen_w // 40)
    margin_y = max(20, screen_h // 40)
    xs = [margin_x + i * (screen_w - 2 * margin_x) // (SWEEP_COLS - 1) for i in range(SWEEP_COLS)]
    ys = [margin_y + i * (screen_h - 2 * margin_y) // (SWEEP_ROWS - 1) for i in range(SWEEP_ROWS)]
    for i, y in enumerate(ys):
        row = xs if i % 2 == 0 else list(reversed(xs))
        for x in row:
            pyautogui.moveTo(x, y, duration=SWEEP_MOVE_S)
    time.sleep(SWEEP_TAIL_PAUSE_S)


def screenshot_with_retry(
    path: Path,
    click_xy: tuple[int, int] | list[int] | None = None,
    min_quality: float = 8.0,
    max_attempts: int = 3,
    retry_pause: float = 2.5,
    sweep: bool = True,
) -> tuple[Path, bool]:
    """Take a screenshot, retry if quality is too low. Returns (path, is_ok).

    Before each attempt we sweep the cursor across the detail content area to
    force Dispatch to repaint under RDP (see _sweep_content_area).
    """
    for attempt in range(max_attempts):
        if attempt > 0 and click_xy is not None:
            click_at(click_xy, pause=retry_pause)
        elif attempt > 0:
            time.sleep(retry_pause)
        if sweep:
            _sweep_content_area()
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


