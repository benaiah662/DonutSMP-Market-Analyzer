"""DMA website scraper and SQLite synchroniser.

Python 3.11+. Requires Playwright and its Chromium browser:
    py -m pip install -r requirements.txt
    py -m playwright install chromium
    py dma_sql_sync.py

AH (the entire market) is read from, in order: the official DonutSMP API (every listing; key from /api
in game), then donut.auction's public JSON API (one row per item, no key), then donut.auction in a browser.

The other sites: visits each in a headless Chromium session, captures JSON responses
(XHR/fetch and JSON embedded in the page), scrapes rendered rows as a fallback,
scrolls/paginates to load everything, and stores the result in dma_market.db.

If a site returns nothing, a screenshot, the page HTML and a list of every
network response are saved to the 'debug' folder so you can see why.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import shutil
import sqlite3
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

try:
    from playwright.async_api import async_playwright, Browser, Page, Response
except ImportError:
    print("Playwright is required. Run: py -m pip install -r requirements.txt")
    print("Then run: py -m playwright install chromium")
    raise SystemExit(1)

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "dma_market.db"
DEBUG_DIR = BASE_DIR / "debug"

CONFIG_FILE = BASE_DIR / "dma_config.json"
ITEMS_FILE = BASE_DIR / "items.txt"

# Search mode: type each item into the site's search box and read only the FIRST page of results.
SEARCH_MAX_RECORDS = 300       # a "first page" is small; if a search floods in more, keep only the first ones
TICKERS_URL = "https://api.donut.auction/v2/tickers/"   # donut.auction: live snapshot of the whole market
CATALOGUE_URL = "https://api.donut.auction/v2/items/search"   # item catalogue with market values
CATALOGUE_REQUESTS_PER_MINUTE = 300
CATALOGUE_MAX_DEPTH = 4        # query length limit when drilling down (a -> ab -> abc -> abcd)
CATALOGUE_ALPHABET = "abcdefghijklmnopqrstuvwxyz0123456789_"
SITE_HEADERS = {
    "Accept": "application/json",
    "Origin": "https://donut.auction",
    "Referer": "https://donut.auction/",
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
}
# A JSON response counts as "this page's data" only if its URL mentions the page's topic.
# Sites like donutmeta load ONE site-wide dataset on every page; without this, every command
# would claim the whole site's records.
URL_HINTS = {
    "AH": ("auction", "listing", "ticker"), "Orders": ("order",), "Craft": ("craft", "recipe"),
    "Smelt": ("smelt", "furnace"), "Villager": ("villager", "trade"), "Potions": ("potion", "brew"),
}
SEARCH_SETTLE_MS = 1500        # wait after pressing Enter for results to render
SEARCH_IDLE_MS = 6000          # ...and for the network to go quiet
SEARCH_SCROLL_STEPS = 3        # a few scroll steps so lazily rendered rows of that first page appear
SEARCH_SELECTORS = (
    'input[type="search"]', '[role="searchbox"]', 'input[placeholder*="search" i]',
    'input[aria-label*="search" i]', 'input[name*="search" i]', 'input[name="q"]',
    'input[id*="search" i]', 'input[placeholder*="filter" i]', 'input[type="text"]',
)
SEARCH_REVEAL_SELECTORS = ('[aria-label*="search" i]', 'button:has-text("Search")', '[class*="search" i] button')
ITEMS_TEMPLATE = """# Items to search for on Orders - one per line. Lines starting with # are ignored.
# Orders is searched for each item and only the FIRST page of results is read.
# (AH and the Craft/Smelt/Villager/Potions pages are always read in full.)
# Type  Sync Orders diamond sword  to search a single item without editing this file,
# or  Sync Orders All  to read everything.
Diamond Sword
Netherite Ingot
Enchanted Golden Apple
"""

# AH: official API (preferred). 44 listings per page, 250 requests/minute per key.
API_URL = "https://api.donutsmp.net/v1/auction/list"
API_PAGE_SIZE = 44
API_REQUESTS_PER_MINUTE = 240     # stay under the 250/min limit
API_WORKERS = 4
API_MAX_PAGES = 20000

SOURCES: dict[str, str] = {
    "AH": "https://donut.auction",                     # last-resort browser fallback for AH
    "Orders": "https://donutmeta.com/orders",
    "Craft": "https://donutmeta.com/craft",
    "Smelt": "https://donutmeta.com/smelt",
    "Villager": "https://donutmeta.com/villager",
    "Potions": "https://donutmeta.com/potions",
}
FULL_ORDER = ("AH", "Orders", "Craft", "Smelt", "Villager", "Potions")
COMMANDS = {name.lower(): name for name in FULL_ORDER}
# Orders has thousands of listings, so it is searched item by item (first page of results).
# AH (the whole market) and the small lists (Craft/Smelt/Villager/Potions) are always read in full.
SEARCHED = frozenset({"Orders"})

VERB = "Retrieving"            # text shown on each progress line
SCROLL_PAUSE_MS = 450          # wait between scroll steps
MAX_SCROLL_STEPS = 240         # per page
MAX_SECONDS_PER_PAGE = 180
MAX_PAGES = 500                # pagination safety limit
STALE_PAGES_LIMIT = 3          # stop paginating after this many pages that add nothing new
MIN_JSON_RECORDS = 3           # fewer JSON records than this -> use rendered rows instead
MAX_SECONDS_PER_SITE = 900     # hard stop for one site
EMPTY_WAIT_STEPS = 10          # x2s: how long to wait for the first data before giving up
EMPTY_SCROLL_STEPS = 15        # stop scrolling a page after this many steps if nothing was found
BLOCK_RE = re.compile(r"just a moment|verify you are (?:a )?human|checking (?:your|if you).{0,40}browser|"
                      r"attention required|enable javascript and cookies", re.I)

# Reserved for a future scheduler. Keep False: Auto is not an accepted mode yet.
SHOW_FUTURE_AUTO_OPTION = False
FUTURE_AUTO_LABEL = "Auto"

# Retain raw source fields rather than guess a site's internal schema.
# NOTE: compared against lower-cased keys, so everything here must be lower case.
RECORD_KEYS = {
    "id", "key", "item", "item_id", "item_key", "name", "product",
    "output", "recipe", "result", "display_name", "displayname", "itemid", "itemname",
}
VALUE_KEYS = {
    "price", "unitprice", "buyprice", "sellprice", "amount", "cost",
    "value", "orders", "inputs", "ingredients", "count", "quantity",
    "profit", "pricelevels", "levels", "trades", "brew", "smelt",
}
# Fields that change on every request for the same listing; ignored when de-duplicating.
VOLATILE_KEYS = {"time_left", "timeleft", "time_remaining", "timeremaining", "expires_in", "expiresin"}
TOTAL_KEYS = ("total", "totalcount", "total_count", "totalitems", "total_items", "recordcount")
MONEY_RE = re.compile(r"(?:\$\s*[\d,]+(?:\.\d+)?|[\d,]+\s*(?:coins?|each|per item))", re.I)
ROW_WORDS_RE = re.compile(r"\b(?:price|profit|order|recipe|output|input|cost)\b", re.I)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def digest_of(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def without_volatile(value: Any) -> Any:
    """Copy of `value` minus fields like time_left, so the same listing always hashes the same."""
    if isinstance(value, dict):
        return {k: without_volatile(v) for k, v in value.items() if str(k).lower() not in VOLATILE_KEYS}
    if isinstance(value, list):
        return [without_volatile(v) for v in value]
    return value


def load_items() -> list[str]:
    """Items to search for, from items.txt (created with examples if it does not exist)."""
    if not ITEMS_FILE.exists():
        ITEMS_FILE.write_text(ITEMS_TEMPLATE, encoding="utf-8")
        print(f"Created {ITEMS_FILE.name} with a few example items - edit it to your own list.")
    lines = ITEMS_FILE.read_text(encoding="utf-8").splitlines()
    return list(dict.fromkeys(l.strip() for l in lines if l.strip() and not l.strip().startswith("#")))


# --------------------------------------------------------------------------- #
# API key storage
# --------------------------------------------------------------------------- #
def read_config() -> dict[str, Any]:
    try:
        data = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def write_config(**changes: Any) -> None:
    CONFIG_FILE.write_text(json.dumps({**read_config(), **changes}, indent=2), encoding="utf-8")


def load_api_key() -> str | None:
    return os.environ.get("DONUTSMP_API_KEY", "").strip() or read_config().get("donutsmp_api_key") or None


def save_api_key(key: str) -> None:
    write_config(donutsmp_api_key=key)


def ask_api_key(reason: str) -> str | None:
    if not sys.stdin.isatty():
        return None
    print(f"\n{reason}")
    print("In DonutSMP type /api in chat, then paste the key here (Enter to skip).")
    try:
        key = input("API key: ").strip()
    except (EOFError, KeyboardInterrupt):
        print()
        return None
    if key:
        save_api_key(key)
        print(f"Saved to {CONFIG_FILE.name} (plain text - keep it private).")
        return key
    return None


# --------------------------------------------------------------------------- #
# Database
# --------------------------------------------------------------------------- #
def open_database() -> sqlite3.Connection:
    db = sqlite3.connect(DB_PATH)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA foreign_keys=ON")
    db.executescript("""
        CREATE TABLE IF NOT EXISTS sync_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            target TEXT NOT NULL,
            mode TEXT NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            status TEXT NOT NULL,
            records_found INTEGER NOT NULL DEFAULT 0,
            records_saved INTEGER NOT NULL DEFAULT 0,
            error TEXT
        );
        CREATE TABLE IF NOT EXISTS market_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL REFERENCES sync_runs(id),
            category TEXT NOT NULL,
            source_url TEXT NOT NULL,
            record_type TEXT NOT NULL,
            item_key TEXT,
            item_name TEXT,
            record_json TEXT NOT NULL,
            content_hash TEXT NOT NULL,
            retrieved_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_market_category ON market_records(category);
        CREATE INDEX IF NOT EXISTS idx_market_item ON market_records(item_key);
        CREATE INDEX IF NOT EXISTS idx_market_time ON market_records(retrieved_at);
        CREATE TABLE IF NOT EXISTS network_payloads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL REFERENCES sync_runs(id),
            category TEXT NOT NULL,
            request_url TEXT NOT NULL,
            http_status INTEGER,
            payload_json TEXT NOT NULL,
            retrieved_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_payload_run ON network_payloads(run_id);
    """)
    db.commit()
    return db


def item_identity(record: dict[str, Any]) -> tuple[str | None, str | None]:
    product = record.get("product")
    if isinstance(product, dict):
        key = product.get("key") or product.get("item") or product.get("id")
        name = product.get("name") or product.get("display_name") or product.get("displayName")
    else:
        key = (record.get("item_key") or record.get("item_id") or record.get("itemId")
               or record.get("itemName") or record.get("key") or record.get("id"))
        name = (record.get("item_name") or record.get("name") or record.get("display_name")
                or record.get("displayName") or record.get("itemName"))
    if key is None and name is not None:
        key = str(name).strip().lower().replace(" ", "_")
    return (str(key) if key is not None else None,
            str(name) if name is not None else None)


# --------------------------------------------------------------------------- #
# Finding records / totals in scraped data
# --------------------------------------------------------------------------- #
def is_record(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    keys = {str(k).lower() for k in value}
    return bool(keys & RECORD_KEYS) and bool(keys & VALUE_KEYS)


def extract_records(payload: Any) -> list[dict[str, Any]]:
    """Recursively collect data-shaped objects from any JSON document."""
    found: list[dict[str, Any]] = []
    seen: set[str] = set()

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            if is_record(node):
                # Outermost match wins and is kept whole: a recipe's own ingredient
                # list ({"name": ..., "count": ...}) must not be counted as extra items.
                digest = digest_of(node)
                if digest not in seen:
                    seen.add(digest)
                    found.append(node)
                return
            for child in node.values():
                if isinstance(child, (dict, list)):
                    walk(child)
        elif isinstance(node, list):
            for child in node:
                if isinstance(child, (dict, list)):
                    walk(child)

    walk(payload)
    return found


def find_total(payload: Any) -> int | None:
    candidates: list[int] = []

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                if (str(key).lower() in TOTAL_KEYS and isinstance(value, int)
                        and not isinstance(value, bool) and value > 0):
                    candidates.append(value)
                if isinstance(value, (dict, list)):
                    walk(value)
        elif isinstance(node, list):
            for value in node:
                if isinstance(value, (dict, list)):
                    walk(value)

    walk(payload)
    return max(candidates) if candidates else None


def total_from_text(text: str) -> int | None:
    patterns = (
        r"(?:showing|displaying)\s+[\d,]+\s+(?:of|/)\s+([\d,]+)",
        r"[\d,]+\s+of\s+([\d,]+)\s+(?:items|results|entries)",
        r"([\d,]+)\s+(?:items|results|entries)\s+(?:total|found)",
    )
    for pattern in patterns:
        match = re.search(pattern, text, re.I)
        if match:
            return int(match.group(1).replace(",", ""))
    return None


# --------------------------------------------------------------------------- #
# Live progress display (matches the concept art: the block redraws in place)
# --------------------------------------------------------------------------- #
def enable_vt_windows() -> None:
    if os.name != "nt":
        return
    try:
        import ctypes
        kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
        handle = kernel32.GetStdHandle(-11)
        mode = ctypes.c_uint()
        if kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
            kernel32.SetConsoleMode(handle, mode.value | 0x0004)
    except (AttributeError, OSError):
        pass


class LiveDisplay:
    """A block of lines redrawn in place. Falls back to plain prints if not a terminal."""

    def __init__(self, header: str) -> None:
        enable_vt_windows()
        self.lines: list[str] = [header]
        self.drawn = 0
        self.tty = sys.stdout.isatty()
        self._last_render = 0.0
        if self.tty:
            self._render(force=True)
        else:
            print(header, flush=True)

    def add_line(self, text: str) -> int:
        self.lines.append(text)
        if self.tty:
            self._render(force=True)
        return len(self.lines) - 1

    def set(self, index: int, text: str, final: bool = False) -> None:
        if self.lines[index] == text:
            return
        self.lines[index] = text
        if self.tty:
            self._render(force=final)
        elif final:
            print(text, flush=True)

    def _render(self, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self._last_render < 0.1:   # max ~10 redraws/second
            return
        self._last_render = now
        width = max(20, shutil.get_terminal_size((120, 30)).columns - 1)
        out = [f"\033[{self.drawn}F"] if self.drawn else []
        out += [line[:width] + "\033[K\n" for line in self.lines]
        sys.stdout.write("".join(out))
        sys.stdout.flush()
        self.drawn = len(self.lines)

    def close(self) -> None:
        if self.tty:
            self._render(force=True)


def format_elapsed(seconds: float) -> str:
    mins, secs = divmod(int(max(0, seconds)), 60)
    return f"{mins}m {secs:02d}s" if mins else f"{secs}s"


def format_eta(seconds: float) -> str:
    mins, secs = divmod(int(max(0, seconds)), 60)
    return f"~{mins}m {secs:02d}s left" if mins else f"~{secs}s left"


# --------------------------------------------------------------------------- #
# Scrape state
# --------------------------------------------------------------------------- #
@dataclass
class ScrapeState:
    category: str
    url: str
    json_records: dict[str, dict[str, Any]] = field(default_factory=dict)    # every JSON record seen
    relevant_records: dict[str, dict[str, Any]] = field(default_factory=dict)  # ...from URLs about this page
    dom_rows: dict[str, dict[str, Any]] = field(default_factory=dict)
    payloads: list[tuple[str, int | None, Any]] = field(default_factory=list)
    responses: list[dict[str, Any]] = field(default_factory=list)
    total: int | None = None
    errors: list[str] = field(default_factory=list)
    first_record_at: float | None = None
    on_change: Callable[[], None] = lambda: None
    activity: str = "starting"
    started: float = field(default_factory=time.monotonic)
    last_count: int = 0
    last_new_at: float = field(default_factory=time.monotonic)
    blocked: bool = False
    page_no: int = 1
    est_total: int | None = None      # running estimate, raised whenever a new page is found
    api_error: bool = False
    current_search: str | None = None   # item being searched right now; tagged onto its records
    search_total: int = 0               # number of items to search (0 = not in search mode)
    search_done: int = 0
    search_started: float = field(default_factory=time.monotonic)
    empty_items: list[str] = field(default_factory=list)
    note: str = ""
    search_hits: int = 0                # records seen during the current search (even repeats)

    @property
    def shown_total(self) -> tuple[int | None, bool]:
        """(total, is_estimate). A total the site reports wins; otherwise the page-based estimate."""
        if self.total:
            return self.total, False
        if self.est_total:
            return max(self.est_total, self.count), True
        return None, False

    @property
    def out_of_time(self) -> bool:
        return time.monotonic() - self.started > MAX_SECONDS_PER_SITE

    def _candidates(self) -> list[tuple[str, dict[str, dict[str, Any]]]]:
        """Possible record sets, best first: JSON from URLs about this page, the rows actually
        rendered on the page, then (last resort) every JSON record the site sent."""
        found: list[tuple[str, dict[str, dict[str, Any]]]] = []
        if len(self.relevant_records) >= MIN_JSON_RECORDS:
            found.append(("network_json", self.relevant_records))
        if self.dom_rows:
            found.append(("rendered_dom", self.dom_rows))
        if self.json_records and (len(self.json_records) >= MIN_JSON_RECORDS or not found):
            found.append(("network_json", self.json_records))
        return found

    def _chosen(self) -> tuple[str, dict[str, dict[str, Any]]]:
        found = self._candidates()
        if not found:
            return "network_json", {}
        if self.total and not self.search_total:
            # The page itself says how many entries it lists: trust the set that matches that.
            best = min(found, key=lambda c: abs(len(c[1]) - self.total))
            if abs(len(best[1]) - self.total) <= 0.25 * self.total:
                return best
        return found[0]

    @property
    def count(self) -> int:
        return len(self._chosen()[1])

    def chosen_records(self) -> list[tuple[str, dict[str, Any]]]:
        kind, records = self._chosen()
        return [(kind, record) for record in records.values()]

    def is_relevant(self, url: str) -> bool:
        return any(hint in url.lower() for hint in URL_HINTS.get(self.category, ()))

    def _touch(self) -> None:
        count = self.count
        if count > self.last_count:
            self.last_count = count
            self.last_new_at = time.monotonic()
        if self.first_record_at is None and count:
            self.first_record_at = time.monotonic()
        self.on_change()

    def add_json_record(self, record: dict[str, Any], relevant: bool = False) -> None:
        if self.search_total and not self.current_search:
            return                        # search mode: ignore what the page loaded before/after a search
        if self.current_search:
            self.search_hits += 1
            record = {**record, "_search": self.current_search}
        digest = digest_of(without_volatile(record))
        self.json_records.setdefault(digest, record)
        if relevant:
            self.relevant_records.setdefault(digest, record)

    def add_dom_row(self, row: dict[str, Any]) -> None:
        if self.search_total and not self.current_search:
            return
        if self.current_search:
            self.search_hits += 1
        # Identify a row by its text only. Including the page address would make the same
        # listing count again every time it shows up on another page.
        self.dom_rows.setdefault(digest_of(row["text"]), row)

    def add_payload(self, url: str, status: int | None, value: Any) -> None:
        self.payloads.append((url, status, value))
        relevant = self.is_relevant(url)
        if relevant:                       # a "total" in some unrelated, site-wide response is not ours
            self.note_total(find_total(value))
        for record in extract_records(value):
            self.add_json_record(record, relevant)
        self._touch()

    def note_total(self, total: int | None) -> None:
        if total and (self.total is None or total > self.total):
            self.total = total

    def progress_text(self) -> str:
        """'(count/total) - ~ETA left' like the concept art; before an ETA exists, show
        elapsed time and what the scraper is doing so it never looks frozen.
        A leading ~ on the total means it is still an estimate that grows as pages are found."""
        count = self.count
        if self.search_total:                       # search mode: (items searched / items to search)
            now = time.monotonic()
            text = f"({self.search_done}/{self.search_total})"
            if 1 <= self.search_done < self.search_total:
                per_item = (now - self.search_started) / self.search_done
                return f"{text} - {format_eta(per_item * (self.search_total - self.search_done))} - {count} records"
            return f"{text} - {format_elapsed(now - self.started)} - {self.activity} - {count} records"
        total, estimated = self.shown_total
        text = f"({count}/{('~' if estimated else '') + str(total) if total else '?'})"
        if total and self.first_record_at and 10 <= count < total:
            elapsed = time.monotonic() - self.first_record_at
            if elapsed >= 2:
                return f"{text} - {format_eta((total - count) / (count / elapsed))}"
        return f"{text} - {format_elapsed(time.monotonic() - self.started)} - {self.activity}"


# --------------------------------------------------------------------------- #
# Page scraping
# --------------------------------------------------------------------------- #
async def visible_dom_records(page: Page) -> tuple[list[str], str]:
    """Extract compact rendered rows/cards, avoiding giant page containers."""
    result = await page.evaluate("""() => {
      const selectors = 'tr,[role="row"],[data-item],[data-product],a[href*="/auction/"],button';
      const out = [];
      const seen = new Set();
      for (const el of document.querySelectorAll(selectors)) {
        const r = el.getBoundingClientRect();
        if (r.width === 0 || r.height === 0) continue;
        const text = (el.innerText || el.textContent || '').replace(/\\s+/g, ' ').trim();
        if (!text || text.length < 3 || text.length > 700) continue;
        if (!(/[$£€]|\\b\\d[\\d,]*(?:\\.\\d+)?\\b/.test(text))) continue;
        if (!seen.has(text)) { seen.add(text); out.push(text); }
      }
      const body = document.body ? document.body.innerText : '';
      return {rows: out, body: body.slice(0, 250000)};
    }""")
    return result.get("rows", []), result.get("body", "")


async def inline_json(page: Page) -> list[tuple[str, Any]]:
    """JSON embedded in the page itself (e.g. Next.js __NEXT_DATA__), which never hits XHR."""
    try:
        scripts = await page.evaluate("""() => [...document.querySelectorAll(
            'script#__NEXT_DATA__, script[type="application/json"]')]
            .map(s => [s.id || s.type, s.textContent || ''])""")
    except Exception:
        return []
    found = []
    for name, text in scripts:
        try:
            found.append((f"inline:{name}", json.loads(text)))
        except (ValueError, TypeError):
            continue
    return found


async def harvest(page: Page, state: ScrapeState) -> None:
    """Read everything currently on the page into the state."""
    rows, body = await visible_dom_records(page)
    state.note_total(total_from_text(body))
    if BLOCK_RE.search(body[:3000]):
        state.blocked = True
    for text in rows:
        if MONEY_RE.search(text) or ROW_WORDS_RE.search(text):
            row = {"text": text, "page_url": page.url}
            if state.current_search:
                row["search"] = state.current_search
            state.add_dom_row(row)
    seen_urls = {u for u, _, _ in state.payloads}
    for name, value in await inline_json(page):
        key = f"{name}@{page.url}"
        if key not in seen_urls:
            state.add_payload(key, None, value)
    state._touch()


SCROLL_JS = """() => {
  let moved = false;
  const root = document.scrollingElement;
  if (root && root.scrollTop + root.clientHeight < root.scrollHeight - 5) {
    root.scrollTop = Math.min(root.scrollTop + Math.max(root.clientHeight * 0.85, 500), root.scrollHeight);
    moved = true;
  }
  const candidates = [...document.querySelectorAll('*')]
    .filter(e => e.clientHeight > 180 && e.scrollHeight > e.clientHeight + 100)
    .sort((a, b) => (b.scrollHeight - b.clientHeight) - (a.scrollHeight - a.clientHeight))
    .slice(0, 5);
  for (const e of candidates) {
    if (e.scrollTop + e.clientHeight < e.scrollHeight - 5) {
      e.scrollTop = Math.min(e.scrollTop + Math.max(e.clientHeight * 0.85, 400), e.scrollHeight);
      moved = true;
    }
  }
  return moved;
}"""


async def scroll_and_scrape(page: Page, state: ScrapeState) -> None:
    """Scroll the page and nested virtual lists so every row gets rendered (and harvested)."""
    stuck, previous = 0, -1
    deadline = time.monotonic() + MAX_SECONDS_PER_PAGE
    for step in range(1, MAX_SCROLL_STEPS + 1):
        state.activity = (f"scrolling page {state.page_no} (step {step}), "
                          f"{len(state.responses)} API calls, {len(state.dom_rows)} rows")
        await harvest(page, state)
        moved = await page.evaluate(SCROLL_JS)
        if state.count != previous:
            stuck, previous = 0, state.count
        elif not moved:
            stuck += 1
        else:
            stuck = 0
        if stuck >= 4 or time.monotonic() > deadline or state.out_of_time:
            break
        if step >= EMPTY_SCROLL_STEPS and state.count == 0:
            break                      # endless page with nothing on it: don't keep scrolling
        await page.wait_for_timeout(SCROLL_PAUSE_MS)
    await harvest(page, state)


PAGINATION_JS = """(source) => {
  const base = new URL(source);
  const current = new URL(location.href);
  const candidates = [];
  for (const el of document.querySelectorAll('a[href],button[formaction]')) {
    const hrefRaw = el.getAttribute('href') || el.getAttribute('formaction');
    if (!hrefRaw) continue;
    let url;
    try { url = new URL(hrefRaw, location.href); } catch { continue; }
    if (url.origin !== base.origin) continue;
    const label = (el.innerText || el.getAttribute('aria-label') || el.title || '').trim();
    const paginationParam = [...url.searchParams.keys()].some(k => /^(page|p|offset|cursor|start)$/i.test(k));
    const numericLabel = /^\\d+$/.test(label);
    const nextLabel = /^(next|older|more pages|›|»|→|next page)$/i.test(label);
    const sameSection = url.pathname === base.pathname || url.pathname === current.pathname;
    if (url.href !== current.href && sameSection && (paginationParam || numericLabel || nextLabel)) {
      candidates.push(url.href);
    }
  }
  return [...new Set(candidates)];
}"""


async def crawl_pagination(page: Page, state: ScrapeState, source_url: str) -> None:
    """Follow the pagination links the page exposes, discovering more as each page loads.

    Pagination bars usually only show a window of links (1 2 3 ... 88), so the
    links must be re-read on every page rather than once on the first page.
    """
    visited = {page.url}
    queue: deque[str] = deque(await page.evaluate(PAGINATION_JS, source_url))
    queued = set(queue)
    stale = 0

    def update_estimate() -> None:
        # rows per page so far x pages known (visited + discovered-but-not-yet-visited)
        if state.count and visited:
            state.est_total = int(state.count + len(queue) * (state.count / len(visited)))
            state.on_change()

    update_estimate()
    while queue and len(visited) < MAX_PAGES and not state.out_of_time:
        target = queue.popleft()
        if target in visited:
            continue
        visited.add(target)
        state.page_no = len(visited)
        state.activity = f"loading page {state.page_no} ({len(queue)} more queued)"
        state.on_change()
        before = state.count
        try:
            await page.goto(target, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(900)
            await scroll_and_scrape(page, state)
            for link in await page.evaluate(PAGINATION_JS, source_url):
                if link not in visited and link not in queued:
                    queued.add(link)
                    queue.append(link)
        except Exception as exc:
            state.errors.append(f"pagination {target}: {type(exc).__name__}: {exc}")
        update_estimate()
        stale = stale + 1 if state.count == before else 0
        if stale >= STALE_PAGES_LIMIT:
            state.errors.append(f"stopped paginating: {STALE_PAGES_LIMIT} pages in a row added nothing new")
            break


async def find_search_box(page: Page) -> Any | None:
    """The first visible, enabled search field on the page (revealing a hidden one if needed)."""
    for attempt in (0, 1):
        for selector in SEARCH_SELECTORS:
            locator = page.locator(selector)
            try:
                found = await locator.count()
            except Exception:
                continue
            for i in range(min(found, 5)):
                candidate = locator.nth(i)
                try:
                    if await candidate.is_visible() and await candidate.is_enabled():
                        return candidate
                except Exception:
                    continue
        if attempt == 0:                      # maybe the box sits behind a magnifier button
            for selector in SEARCH_REVEAL_SELECTORS:
                try:
                    await page.locator(selector).first.click(timeout=1500)
                    await page.wait_for_timeout(500)
                    break
                except Exception:
                    continue
    return None


async def search_items(page: Page, state: ScrapeState, items: list[str]) -> None:
    """For each item: type it into the site's search box and read the first page of results."""
    state.search_total, state.search_done = len(items), 0
    state.search_started = time.monotonic()
    for item in items:
        if state.out_of_time:
            break
        state.current_search = item
        state.activity = f'searching "{item}"'
        state.on_change()
        state.search_hits = 0
        json_before, dom_before = len(state.json_records), len(state.dom_rows)
        try:
            box = await find_search_box(page)     # looked up each time: single-page sites re-render
            if box is None:
                raise RuntimeError("search box disappeared")
            await box.click()
            await box.fill("")
            await box.fill(item)
            await box.press("Enter")
            try:
                await page.wait_for_load_state("networkidle", timeout=SEARCH_IDLE_MS)
            except Exception:
                pass
            await page.wait_for_timeout(SEARCH_SETTLE_MS)
            for _ in range(SEARCH_SCROLL_STEPS):  # first page only: just render its lazy rows
                await harvest(page, state)
                if not await page.evaluate(SCROLL_JS):
                    break
                await page.wait_for_timeout(SCROLL_PAUSE_MS)
            await harvest(page, state)
        except Exception as exc:
            state.errors.append(f'search "{item}": {type(exc).__name__}: {exc}')
        for records, start in ((state.json_records, json_before), (state.dom_rows, dom_before)):
            for key in list(records)[start + SEARCH_MAX_RECORDS:]:    # keep a search to its first page
                del records[key]
                state.relevant_records.pop(key, None)
        if state.search_hits == 0:
            state.empty_items.append(item)
        state.search_done += 1
        state.on_change()
    state.current_search = None


async def capture_response(response: Response, state: ScrapeState) -> None:
    try:
        request_type = response.request.resource_type
        content_type = (await response.all_headers()).get("content-type", "").lower()
        is_json = "json" in content_type
        if request_type not in ("xhr", "fetch") and not is_json:
            return
        if len(state.responses) < 300:
            state.responses.append({"url": response.url, "status": response.status,
                                    "type": request_type, "content_type": content_type})
        if response.status >= 400:
            return
        state.add_payload(response.url, response.status, await response.json())
    except Exception:
        return  # empty, binary or non-JSON body, or the page closed mid-download


# --------------------------------------------------------------------------- #
# One site
# --------------------------------------------------------------------------- #
@dataclass
class Outcome:
    category: str
    url: str
    records: list[tuple[str, dict[str, Any]]]
    payloads: list[tuple[str, int | None, Any]]
    total: int
    status: str                      # completed | no_data | failed
    error: str | None = None
    debug_path: Path | None = None


async def save_debug_files(page: Page | None, state: ScrapeState) -> Path | None:
    try:
        DEBUG_DIR.mkdir(exist_ok=True)
        stem = DEBUG_DIR / f"{state.category}_{datetime.now():%Y%m%d_%H%M%S}"
        if page is not None:
            try:
                await page.screenshot(path=str(stem.with_suffix(".png")), full_page=False)
            except Exception:
                pass
            try:
                stem.with_suffix(".html").write_text(await page.content(), encoding="utf-8")
            except Exception:
                pass
        report = {"url": state.url, "final_url": page.url if page else None, "errors": state.errors,
                  "json_records": len(state.json_records), "dom_rows": len(state.dom_rows),
                  "sample_dom_rows": list(state.dom_rows.values())[:10],
                  "responses": state.responses}
        stem.with_suffix(".json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
        return stem
    except OSError:
        return None


async def scrape_one(browser: Browser, category: str, url: str, display: LiveDisplay,
                     items: list[str] | None) -> Outcome:
    state = ScrapeState(category, url)
    state.search_total = len(items) if items else 0
    state.activity = "connecting"
    prefix = f'{VERB} \'{category}\' prices from "{url}" - '
    index = display.add_line(prefix + state.progress_text())
    state.on_change = lambda: display.set(index, prefix + state.progress_text())

    context = await browser.new_context(
        viewport={"width": 1600, "height": 1000},
        ignore_https_errors=True,
        user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"),
    )
    page = await context.new_page()
    tasks: set[asyncio.Task[Any]] = set()

    def on_response(response: Response) -> None:
        task = asyncio.create_task(capture_response(response, state))
        tasks.add(task)
        task.add_done_callback(tasks.discard)

    page.on("response", on_response)
    page.on("pageerror", lambda exc: state.errors.append(f"Page JavaScript: {exc}"))
    page.set_default_timeout(15000)
    page.set_default_navigation_timeout(30000)

    async def ticker() -> None:
        # Redraw every second so the elapsed time keeps moving even during a long wait.
        while True:
            state.on_change()
            await asyncio.sleep(1)

    failure: str | None = None
    ticker_task = asyncio.create_task(ticker())
    try:
        # Wait only for the first server response; waiting for every script can hang
        # on sites that keep websockets/API connections open.
        await page.goto(url, wait_until="commit", timeout=30000)
        state.activity = "loading page"
        for event, timeout in (("domcontentloaded", 15000), ("networkidle", 8000)):
            try:
                await page.wait_for_load_state(event, timeout=timeout)
            except Exception as exc:  # slow or never-idle sites are fine; scrape what rendered
                state.errors.append(f"{event} wait: {type(exc).__name__}")
        await page.wait_for_timeout(1500)
        if items and await find_search_box(page):
            await search_items(page, state, items)
        else:
            if items:                                  # no search box: read the whole page instead
                state.search_total = 0
                state.note = "no search box found, read the whole page"
                state.errors.append(state.note)
            await scroll_and_scrape(page, state)
            if state.count == 0:           # data may arrive late (or after a bot-check page clears)
                for _ in range(EMPTY_WAIT_STEPS):
                    state.activity = "waiting for data" + (" - bot-check page detected?" if state.blocked else "")
                    state.on_change()
                    await page.wait_for_timeout(2000)
                    await harvest(page, state)
                    if state.count:
                        await scroll_and_scrape(page, state)
                        break
            if category == "AH" and state.count:        # the whole market spans many pages
                await crawl_pagination(page, state, url)
        state.activity = "finishing"
        if tasks:
            await asyncio.gather(*list(tasks), return_exceptions=True)
        await harvest(page, state)
    except asyncio.CancelledError:
        # Ctrl+C: keep a snapshot so you can see what the page looked like.
        state.activity = "cancelled"
        path = await save_debug_files(page, state)
        if path:
            print(f"\nDebug files saved: {path}.png / .html / .json")
        raise
    except Exception as exc:
        failure = f"{type(exc).__name__}: {str(exc).splitlines()[0] if str(exc) else ''}".strip()
        state.errors.append(failure)
    finally:
        ticker_task.cancel()
        if tasks:
            await asyncio.gather(*list(tasks), return_exceptions=True)

    debug_path = None
    if not state.chosen_records():
        debug_path = await save_debug_files(page, state)
    await context.close()
    return finish_outcome(display, index, prefix, state, failure, debug_path)


def finish_outcome(display: LiveDisplay, index: int, prefix: str, state: ScrapeState,
                   failure: str | None, debug_path: Path | None) -> Outcome:
    """Turn the final state into the 'Completed' / error line and an Outcome."""
    records = state.chosen_records()
    saved = len(records)
    searching = state.search_total > 0
    total = saved if saved else (state.total or 0)   # a finished sync's total is what it actually got
    if not searching and state.total and saved and abs(state.total - saved) > max(2, state.total // 100):
        total = state.total                           # site-reported total differs noticeably: show it
    if failure and saved == 0:
        status = "failed"
        error = failure if state.api_error else (
            f"Could not open the site: {failure}. Check internet access, DNS, firewall/antivirus.")
        tail = "Failed"
    elif saved == 0 and state.blocked:
        status = "no_data"
        error = ("The site showed a bot-check page ('Just a moment...' / verify you are human). "
                 "Headless browsers are often blocked by this.")
        tail = "Blocked by bot check"
    elif saved == 0 and searching:
        status = "no_data"
        error = (f"None of the {state.search_total} searches returned results. The search box may work "
                 "differently than expected - see the debug files, or try 'Sync <source> All'.")
        tail = "No results"
    elif saved == 0:
        status = "no_data"
        error = ("The page opened but no data-shaped JSON or price rows were found. "
                 "It may need a login, load its data some other way, or the layout changed.")
        if state.category == "AH" and state.url != API_URL:
            error += " For AH, set a DonutSMP API key with 'Sync Key' - it reads every listing and cannot be blocked."
        tail = "No data detected"
    else:
        status = "completed"
        error = None
        if searching:
            tail = f"Completed - {saved} records"
            if state.empty_items:
                tail += f" ({len(state.empty_items)} of {state.search_total} searches found nothing)"
        else:
            tail = "Completed" if saved == total else f"Completed (site reports {total})"
            if state.note:
                tail += f" - {state.note}"
    count_text = f"({state.search_done}/{state.search_total})" if searching else f"({saved}/{total})"
    display.set(index, prefix + f"{count_text} - {tail}", final=True)
    return Outcome(state.category, state.url, records, state.payloads, total, status, error, debug_path)


# --------------------------------------------------------------------------- #
# AH via the official API
# --------------------------------------------------------------------------- #
class ApiAuthError(Exception):
    pass


class RateLimiter:
    """Spaces request start times so the key never exceeds its per-minute limit."""

    def __init__(self, per_minute: int) -> None:
        self.interval = 60.0 / per_minute
        self._next = 0.0
        self._lock = asyncio.Lock()

    async def wait(self) -> None:
        async with self._lock:
            now = time.monotonic()
            delay = max(0.0, self._next - now)
            self._next = max(now, self._next) + self.interval
        if delay:
            await asyncio.sleep(delay)


async def fetch_api_page(request: Any, limiter: RateLimiter, state: ScrapeState, page: int) -> list[dict[str, Any]] | None:
    """One page of listings (null padding removed). None = the page does not exist."""
    for attempt in range(4):
        await limiter.wait()
        try:
            response = await request.get(f"{API_URL}/{page}", timeout=30000)
        except Exception as exc:
            if attempt == 3:
                raise
            state.errors.append(f"page {page}: {type(exc).__name__}, retrying")
            await asyncio.sleep(1 + attempt)
            continue
        status = response.status
        if len(state.responses) < 300:
            state.responses.append({"url": response.url, "status": status})
        if status == 401:
            raise ApiAuthError("The DonutSMP API rejected the key (401).")
        if status == 429:
            wait = response.headers.get("retry-after", "")
            await asyncio.sleep(float(wait) if wait.replace(".", "", 1).isdigit() else 5 * (attempt + 1))
            continue
        if status >= 500 and attempt == 0:
            await asyncio.sleep(0.5)
            continue
        if status >= 400:
            return None
        try:
            payload = await response.json()
        except Exception:
            return None
        items = payload.get("result") if isinstance(payload, dict) else payload
        if not isinstance(items, list):
            return None
        return [item for item in items if isinstance(item, dict)]
    return None


async def crawl_api(request: Any, state: ScrapeState) -> None:
    """Find how many pages exist, then fetch them all. The total is updated every time a page is found."""
    limiter = RateLimiter(API_REQUESTS_PER_MINUTE)
    fetched: dict[int, int] = {}

    async def get(page: int) -> bool:
        items = await fetch_api_page(request, limiter, state, page)
        if not items:
            return False
        fetched[page] = len(items)
        for item in items:
            state.add_json_record(item)
        state._touch()
        return True

    # 1) Probe 1, 2, 4, 8 ... until a page is empty, raising the total with each page found.
    state.activity = "finding how many pages exist"
    low, high, probe = 0, None, 1
    while probe <= API_MAX_PAGES:
        if await get(probe):
            low = probe
            state.est_total = probe * API_PAGE_SIZE
            state.on_change()
            probe *= 2
        else:
            high = probe
            break
    if low == 0:
        return                                   # no listings at all
    if high is None:
        high = API_MAX_PAGES + 1
    # 2) Binary search between the last page that exists and the first that doesn't.
    while high - low > 1 and not state.out_of_time:
        mid = (low + high) // 2
        if await get(mid):
            low = mid
            state.est_total = mid * API_PAGE_SIZE
            state.on_change()
        else:
            high = mid
    last = low
    state.est_total = (last - 1) * API_PAGE_SIZE + fetched.get(last, API_PAGE_SIZE)

    # 3) Fetch everything else, a few pages at a time.
    todo: asyncio.Queue[int] = asyncio.Queue()
    for page in range(1, last + 1):
        if page not in fetched:
            todo.put_nowait(page)
    done = len(fetched)

    async def worker() -> None:
        nonlocal done
        while not state.out_of_time:
            try:
                page = todo.get_nowait()
            except asyncio.QueueEmpty:
                return
            await get(page)
            done += 1
            state.activity = f"fetching page {done}/{last}"
            state.on_change()

    await asyncio.gather(*(worker() for _ in range(API_WORKERS)))

    # 4) Listings can be added while we crawl: keep going while the next page still has data.
    nxt = last + 1
    while not state.out_of_time and nxt <= API_MAX_PAGES and await get(nxt):
        last = nxt
        state.est_total = (last - 1) * API_PAGE_SIZE + fetched[last]
        state.on_change()
        nxt += 1


def find_record_list(payload: Any) -> list[dict[str, Any]]:
    """The biggest list of objects in a JSON document (also unwraps {"id": {...}, ...} maps)."""
    best: list[dict[str, Any]] = []

    def consider(items: list[dict[str, Any]]) -> None:
        nonlocal best
        if len(items) > len(best):
            best = items

    def walk(node: Any, depth: int = 0) -> None:
        if depth > 6:
            return
        if isinstance(node, list):
            dicts = [x for x in node if isinstance(x, dict)]
            if dicts and len(dicts) >= len(node) / 2:
                consider(dicts)
            for child in node:
                if isinstance(child, (list, dict)):
                    walk(child, depth + 1)
        elif isinstance(node, dict):
            values = list(node.values())
            dict_values = [v for v in values if isinstance(v, dict)]
            if len(dict_values) >= 3 and len(dict_values) >= 0.8 * len(values):
                consider([{"_key": k, **v} for k, v in node.items() if isinstance(v, dict)])
            for child in values:
                if isinstance(child, (list, dict)):
                    walk(child, depth + 1)

    walk(payload)
    return best


async def crawl_catalogue(request: Any, state: ScrapeState) -> None:
    """List every item donut.auction knows by searching it. A search returns at most N results, so any
    query that fills that cap is split into longer queries (a -> aa, ab, ...) until none is capped."""
    limiter = RateLimiter(CATALOGUE_REQUESTS_PER_MINUTE)
    gate = asyncio.Semaphore(API_WORKERS)
    stats = {"cap": 0, "queries": 0}

    async def search(query: str) -> int | None:
        async with gate:
            for attempt in range(3):
                await limiter.wait()
                response = await request.get(CATALOGUE_URL, params={"q": query}, timeout=30000)
                if len(state.responses) < 6:
                    state.responses.append({"url": response.url, "status": response.status,
                                            "sample": (await response.text())[:300]})
                if response.status == 429:
                    await asyncio.sleep(3 * (attempt + 1))
                    continue
                if response.status >= 500 and attempt == 0:
                    await asyncio.sleep(0.5)
                    continue
                if response.status != 200:
                    return None
                try:
                    items = find_record_list(await response.json())
                except Exception:
                    return None
                for item in items:
                    state.add_json_record({**item, "_source": "item search"}, relevant=True)
                stats["queries"] += 1
                state.activity = f'listing every item: searched "{query}" ({stats["queries"]} searches)'
                state._touch()
                return len(items)
        return None

    async def drill(queries: list[str], depth: int) -> None:
        if not queries or depth > CATALOGUE_MAX_DEPTH or state.out_of_time:
            return
        counts = await asyncio.gather(*(search(q) for q in queries))
        stats["cap"] = max([stats["cap"], *(n for n in counts if n)])
        capped = [q for q, n in zip(queries, counts) if n is not None and n >= stats["cap"] >= 5]
        if capped:
            state.est_total = int(state.count + len(capped) * len(CATALOGUE_ALPHABET) * 0.3 *
                                  (state.count / max(1, stats["queries"])))
            state.on_change()
        await drill([q + ch for q in capped for ch in CATALOGUE_ALPHABET], depth + 1)

    await drill(list(CATALOGUE_ALPHABET), 1)


async def scrape_tickers(playwright: Any, category: str, display: LiveDisplay) -> Outcome:
    """donut.auction's own JSON API: one request returns a live snapshot of the whole market."""
    state = ScrapeState(category, TICKERS_URL)
    state.activity = "downloading the market snapshot"
    prefix = f'{VERB} \'{category}\' prices from "{TICKERS_URL}" - '
    index = display.add_line(prefix + state.progress_text())
    state.on_change = lambda: display.set(index, prefix + state.progress_text())
    request = await playwright.request.new_context(extra_http_headers=SITE_HEADERS)

    async def ticker() -> None:
        while True:
            state.on_change()
            await asyncio.sleep(1)

    failure: str | None = None
    ticker_task = asyncio.create_task(ticker())
    try:
        payload: Any = None
        status = 0
        for attempt in range(3):
            response = await request.get(TICKERS_URL, timeout=60000)
            status = response.status
            state.responses.append({"url": TICKERS_URL, "status": status, "sample": (await response.text())[:300]})
            if status == 200:
                try:
                    payload = await response.json()
                except Exception:
                    failure = "donut.auction answered, but not with JSON"
                break
            await asyncio.sleep(2 * (attempt + 1))
        if payload is None and not failure:
            failure = f"donut.auction's API answered HTTP {status}"
        if payload is not None:
            state.payloads.append((TICKERS_URL, status, payload))
            for record in find_record_list(payload):
                state.add_json_record(record, relevant=True)
            state._touch()
            if not state.json_records:
                failure = "donut.auction's API answered, but its data had an unexpected shape"
        if not failure:                       # the snapshot is only a ~45 listing sample: list every item too
            before = len(state.json_records)
            try:
                await crawl_catalogue(request, state)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                state.errors.append(f"item catalogue: {type(exc).__name__}: {exc}")
            if len(state.json_records) == before:
                state.note = ("only a small sample was available - add a DonutSMP API key (Sync Key) "
                              "for the entire market")
    except asyncio.CancelledError:
        state.activity = "cancelled"
        raise
    except Exception as exc:
        failure = f"{type(exc).__name__}: {str(exc).splitlines()[0] if str(exc) else ''}".strip()
    finally:
        ticker_task.cancel()
        await request.dispose()
    if failure:
        state.api_error = True
    debug_path = None if state.chosen_records() else await save_debug_files(None, state)
    return finish_outcome(display, index, prefix, state, failure, debug_path)


async def scrape_ah(playwright: Any, get_browser: Callable[[], Any], display: LiveDisplay,
                    api_key: str | None) -> Outcome:
    """The entire AH market. Tries, in order: the official DonutSMP API (every listing, needs a key),
    donut.auction's JSON API (one row per item), donut.auction in a browser. First success wins."""
    attempts: list[Callable[[], Any]] = []
    if api_key:
        attempts.append(lambda: scrape_api(playwright, "AH", api_key, display))
    attempts.append(lambda: scrape_tickers(playwright, "AH", display))

    async def browse() -> Outcome:
        return await scrape_one(await get_browser(), "AH", SOURCES["AH"], display, None)

    attempts.append(browse)
    outcome: Outcome | None = None
    for attempt in attempts:
        outcome = await attempt()
        if outcome.status == "completed":
            break
    assert outcome is not None
    return outcome


async def scrape_api(playwright: Any, category: str, api_key: str, display: LiveDisplay) -> Outcome:
    state = ScrapeState(category, API_URL)
    state.activity = "connecting to the DonutSMP API"
    prefix = f'{VERB} \'{category}\' prices from "{API_URL}" - '
    index = display.add_line(prefix + state.progress_text())
    state.on_change = lambda: display.set(index, prefix + state.progress_text())

    request = await playwright.request.new_context(
        extra_http_headers={"Authorization": f"Bearer {api_key}", "Accept": "application/json"})

    async def ticker() -> None:
        while True:
            state.on_change()
            await asyncio.sleep(1)

    failure: str | None = None
    ticker_task = asyncio.create_task(ticker())
    try:
        await crawl_api(request, state)           # every listing, page by page
    except ApiAuthError as exc:
        state.api_error = True
        failure = f"{exc} Run 'Sync Key' to enter a new key (type /api in DonutSMP)."
    except asyncio.CancelledError:
        state.activity = "cancelled"
        raise
    except Exception as exc:
        state.api_error = True
        failure = f"API request failed: {type(exc).__name__}: {str(exc).splitlines()[0] if str(exc) else ''}".strip()
    finally:
        ticker_task.cancel()
        await request.dispose()

    debug_path = None
    if not state.chosen_records():
        debug_path = await save_debug_files(None, state)
    return finish_outcome(display, index, prefix, state, failure, debug_path)


def save_outcome(db: sqlite3.Connection, run_id: int, outcome: Outcome) -> int:
    """Write one site's results in a single transaction. Returns records saved."""
    now = utc_now()
    payload_rows = [
        (run_id, outcome.category, url, status,
         json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str), now)
        for url, status, value in outcome.payloads
    ]
    record_rows = []
    for record_type, record in outcome.records:
        encoded = json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)
        key, name = item_identity(record)
        record_rows.append((run_id, outcome.category, outcome.url, record_type, key, name,
                            encoded, hashlib.sha256(encoded.encode("utf-8")).hexdigest(), now))
    with db:
        db.executemany("""INSERT INTO network_payloads
            (run_id, category, request_url, http_status, payload_json, retrieved_at)
            VALUES (?, ?, ?, ?, ?, ?)""", payload_rows)
        db.executemany("""INSERT INTO market_records
            (run_id, category, source_url, record_type, item_key, item_name,
             record_json, content_hash, retrieved_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""", record_rows)
    return len(record_rows)


# --------------------------------------------------------------------------- #
# Sync command
# --------------------------------------------------------------------------- #
async def run_sync(categories: tuple[str, ...], mode: str, items: list[str] | None) -> None:
    """items: names to search for (first page of results each), or None to read everything."""
    db = open_database()
    target = categories[0] if len(categories) == 1 else "Full"
    cursor = db.execute("INSERT INTO sync_runs(target, mode, started_at, status) VALUES (?, ?, ?, 'running')",
                        (target, mode, utc_now()))
    run_id = int(cursor.lastrowid)
    db.commit()

    api_key = None
    if "AH" in categories:
        api_key = load_api_key()
        if not api_key:
            api_key = ask_api_key(
                "Without a DonutSMP API key AH is only partial: no website lists every listing. "
                "The official API returns the ENTIRE market.")
        if not api_key:
            print("No DonutSMP API key - AH will be partial (donut.auction only). 'Sync Key' adds one.")

    print()
    display = LiveDisplay(f"'{target}' - '{mode}' Sync Initiated...")
    outcomes: list[Outcome] = []
    saved_total = 0
    status, run_error = "failed", None
    try:
        async with async_playwright() as playwright:
            browser: Browser | None = None

            async def get_browser() -> Browser:
                nonlocal browser
                if browser is None:
                    browser = await playwright.chromium.launch(
                        headless=True,
                        args=["--disable-dev-shm-usage", "--disable-background-networking"],
                    )
                return browser

            try:
                for category in categories:
                    if category == "AH":
                        outcome = await scrape_ah(playwright, get_browser, display, api_key)
                    else:
                        outcome = await scrape_one(await get_browser(), category, SOURCES[category], display,
                                                   items if category in SEARCHED else None)
                    saved_total += save_outcome(db, run_id, outcome)
                    outcomes.append(outcome)
            finally:
                if browser:
                    await browser.close()
        status = "completed" if all(o.status == "completed" for o in outcomes) else "partial"
    except asyncio.CancelledError:
        status, run_error = "cancelled", "Cancelled by user"
        raise
    except Exception as exc:
        run_error = f"{type(exc).__name__}: {exc}"
        print(f"\nBrowser error: {run_error}")
    finally:
        display.close()
        problems = "; ".join(f"{o.category}: {o.error}" for o in outcomes if o.error)
        db.execute("UPDATE sync_runs SET finished_at=?, status=?, records_found=?, records_saved=?, error=? WHERE id=?",
                   (utc_now(), status, saved_total, saved_total, run_error or problems or None, run_id))
        db.commit()
        db.close()

    for outcome in outcomes:
        if outcome.error:
            print(f"\n{outcome.category}: {outcome.error}")
            if outcome.debug_path:
                print(f"  Debug files saved: {outcome.debug_path}.png / .html / .json")
    print(f"\nSaved {saved_total} records to {DB_PATH.name}")


# --------------------------------------------------------------------------- #
# Command line
# --------------------------------------------------------------------------- #
def show_help() -> None:
    lines = [
        "-Sync Help - Lists all commands in the Sync hierarchy.",
        "-Sync Full - Scrapes all six websites and saves their data to SQLite.",
        "-Sync AH - The ENTIRE Auction House: DonutSMP API (every listing, needs a key), else donut.auction.",
        "-Sync Key - Set or replace your DonutSMP API key (type /api in DonutSMP to get one).",
        "-Sync Orders - Searches DonutMeta buyer orders for each item (first page of results).",
        "-Sync Craft - Scrapes the whole DonutMeta crafting page.",
        "-Sync Smelt - Scrapes the whole DonutMeta smelting page.",
        "-Sync Villager - Scrapes the whole DonutMeta villager trade page.",
        "-Sync Potions - Scrapes the whole DonutMeta potion page.",
        "",
        "Usage:",
        "  DMA>Sync Full                       AH + Craft/Smelt/Villager/Potions in full; Orders searched per item",
        "  DMA>Sync AH                         the entire Auction House market",
        "  DMA>Sync Orders diamond sword       search one item (first page of results)",
        "  DMA>Sync Orders sword, ingot        several items, separated by commas",
        "  DMA>Sync Orders All                 read all of Orders instead of searching",
        "",
        "Manual runs a single sync and returns to the command prompt.",
    ]
    if SHOW_FUTURE_AUTO_OPTION:
        lines.append(f"  {FUTURE_AUTO_LABEL} - Reserved for a future automatic scheduler; not active in this release.")
    lines += [
        "",
        "The scraper uses Chromium to read rendered site content and capture JSON",
        "responses. If a site returns nothing, screenshots and page dumps are saved",
        "to the 'debug' folder. Try Sync AH first before Sync Full.",
        f"Database: {DB_PATH.name} (SQLite)",
    ]
    print("\n".join(lines))


def parse_sync_command(parts: list[str]) -> tuple[tuple[str, ...] | None, str, list[str] | None]:
    """Sync <target> [Manual] [item name[, another item] | All]  ->  (categories, mode, items)."""
    if len(parts) < 2 or parts[1].lower() in {"help", "-help", "--help", "-h"}:
        show_help()
        return None, "Manual", None
    target = parts[1].lower()
    if target == "key":
        if not ask_api_key("Enter your DonutSMP API key."):
            print("Key unchanged.")
        return None, "Manual", None
    rest = parts[2:]
    mode = "Manual"
    if rest and rest[0].lower() in {"manual", "auto"}:
        mode = rest.pop(0).capitalize()
    if mode != "Manual":
        print("Only Manual sync is available in this release.")
        return None, "Manual", None
    if target == "full":
        categories: tuple[str, ...] | None = FULL_ORDER
    elif target in COMMANDS:
        categories = (COMMANDS[target],)
    else:
        print("Unknown Sync command. Type 'Sync Help'.")
        return None, "Manual", None

    items: list[str] | None
    if not SEARCHED.intersection(categories):                   # Craft / Smelt / Villager / Potions
        if rest and [word.lower() for word in rest] != ["all"]:
            print(f"{categories[0]} is always read in full - item names are ignored.")
        return categories, mode, None
    if [word.lower() for word in rest] == ["all"]:
        items = None                                            # read everything
    elif rest:
        items = [name.strip() for name in " ".join(rest).split(",") if name.strip()]
    else:
        items = load_items()
        if not items:
            print(f"{ITEMS_FILE.name} has no items. Add one per line, or type: Sync {parts[1]} diamond sword")
            return None, "Manual", None
    return categories, mode, items


def main() -> None:
    print("Donut Market Analyzer - SQL Website Synchroniser")
    print("Type 'Sync Help' for commands. Type 'Exit' to quit.")
    while True:
        try:
            raw = input("\nDMA>").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return
        if not raw:
            continue
        parts = raw.split()
        if parts[0].lower() in {"exit", "quit"}:
            return
        if parts[0].lower() != "sync":
            print("Unknown command. Type 'Sync Help'.")
            continue
        categories, mode, items = parse_sync_command(parts)
        if categories:
            try:
                asyncio.run(run_sync(categories, mode, items))
            except KeyboardInterrupt:
                print("\nSync cancelled.")


if __name__ == "__main__":
    main()
