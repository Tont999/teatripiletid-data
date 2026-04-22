#!/usr/bin/env python3
"""Scrape Eesti Draamateater and Tallinna Linnateater kava pages.

This scraper runs in a GitHub Actions environment (no network restrictions),
fetches each theater's public kava page, attempts to extract a list of
upcoming performances with date/time and ticket-purchase links, and writes
a single structured JSON file (`data/state.json`) that downstream consumers
(a Cowork scheduled task) can read.

Design goals:
  - Permissive parsing: multiple fallback strategies per theater since
    site HTML may change without notice.
  - Always emit a valid state.json even if a theater fails (with
    `scraped_ok: false` and an error message) so downstream tooling can
    reason about partial failures.
  - Save raw HTML under `data/raw/` so diagnosing breakage is easy.

Output schema is documented in the top-level README.
"""
from __future__ import annotations

import json
import re
import sys
import traceback
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import requests
from bs4 import BeautifulSoup, Tag
from dateutil import parser as dateparser

SCRAPER_VERSION = "1.0.0"
UA = (
    "Mozilla/5.0 (compatible; TeatripiletidScraper/"
    + SCRAPER_VERSION
    + "; +https://github.com/Tont999/teatripiletid-data)"
)
TIMEOUT = 30

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
RAW = DATA / "raw"
DATA.mkdir(exist_ok=True)
RAW.mkdir(exist_ok=True)

_log_lines: list[str] = []


def log(msg: str) -> None:
    line = f"[{datetime.now(timezone.utc).isoformat(timespec='seconds')}] {msg}"
    print(line, flush=True)
    _log_lines.append(line)


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------
@dataclass
class Show:
    id: str
    title: str
    datetime_str: str
    iso_datetime: str | None = None
    venue: str | None = None
    ticket_url: str | None = None
    ticket_status: str | None = None  # "available" | "sold_out" | "limited" | "unknown"
    price_range: str | None = None


@dataclass
class Theater:
    id: str
    name: str
    source_url: str
    scraped_ok: bool = False
    error: str | None = None
    shows: list[Show] = field(default_factory=list)


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
def fetch(url: str) -> str:
    log(f"GET {url}")
    resp = requests.get(url, headers={"User-Agent": UA, "Accept-Language": "et,en;q=0.5"}, timeout=TIMEOUT)
    resp.raise_for_status()
    resp.encoding = resp.apparent_encoding or resp.encoding
    return resp.text


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------
EST_MONTHS = {
    "jaan": 1, "jaanuar": 1,
    "veebr": 2, "veebruar": 2,
    "märts": 3, "marts": 3,
    "apr": 4, "aprill": 4,
    "mai": 5,
    "juun": 6, "juuni": 6,
    "juul": 7, "juuli": 7,
    "aug": 8, "august": 8,
    "sept": 9, "september": 9,
    "okt": 10, "oktoober": 10,
    "nov": 11, "november": 11,
    "dets": 12, "detsember": 12,
}

DATE_PATTERNS = [
    # 10.05.2026 19:00 / 10.05.2026 kell 19:00
    re.compile(r"(?P<d>\d{1,2})\.(?P<m>\d{1,2})\.(?P<y>\d{4})(?:[^\d]{0,6}(?:kell\s*)?(?P<h>\d{1,2})[.:](?P<mi>\d{2}))?"),
    # 10. mai 2026 19:00
    re.compile(
        r"(?P<d>\d{1,2})\.?\s+(?P<mon>"
        + "|".join(sorted(EST_MONTHS.keys(), key=len, reverse=True))
        + r")\w*\s+(?P<y>\d{4})(?:[^\d]{0,6}(?:kell\s*)?(?P<h>\d{1,2})[.:](?P<mi>\d{2}))?",
        re.IGNORECASE,
    ),
    # 2026-05-10T19:00
    re.compile(r"(?P<y>\d{4})-(?P<m>\d{2})-(?P<d>\d{2})(?:[T\s](?P<h>\d{2}):(?P<mi>\d{2}))?"),
]


def parse_datetime(text: str) -> tuple[str | None, str | None]:
    """Return (raw, iso) best-effort date extraction from free text."""
    if not text:
        return None, None
    text = re.sub(r"\s+", " ", text).strip()

    for pat in DATE_PATTERNS:
        m = pat.search(text)
        if not m:
            continue
        gd = m.groupdict()
        try:
            if "mon" in gd and gd.get("mon"):
                month = EST_MONTHS[gd["mon"].lower()]
            else:
                month = int(gd["m"])
            year = int(gd["y"])
            day = int(gd["d"])
            hour = int(gd["h"]) if gd.get("h") else 0
            minute = int(gd["mi"]) if gd.get("mi") else 0
            # Assume Europe/Tallinn offset (+3 summer, +2 winter). We don't
            # know DST here reliably; emit naive ISO (no offset) — good enough
            # for sorting & display.
            iso = f"{year:04d}-{month:02d}-{day:02d}T{hour:02d}:{minute:02d}:00"
            raw = m.group(0)
            return raw, iso
        except Exception:
            continue

    # Fallback: dateutil's fuzzy parser (English mostly).
    try:
        dt = dateparser.parse(text, fuzzy=True, dayfirst=True)
        return text, dt.isoformat(timespec="minutes")
    except Exception:
        return text or None, None


def make_show_id(theater_id: str, title: str, iso_dt: str | None, raw_dt: str | None) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", (title or "").lower()).strip("-")[:60]
    dt_key = iso_dt or (raw_dt or "").replace(" ", "_")
    return f"{theater_id}:{slug}:{dt_key}"


def text_of(node: Tag | None) -> str:
    if node is None:
        return ""
    return re.sub(r"\s+", " ", node.get_text(" ", strip=True)).strip()


def absolute_url(base: str, href: str | None) -> str | None:
    if not href:
        return None
    from urllib.parse import urljoin
    return urljoin(base, href)


def detect_ticket_status(fragment_text: str) -> str | None:
    t = fragment_text.lower()
    if any(k in t for k in ["läbi müüdud", "välja müüdud", "välja mü", "välja müüd", "sold out"]):
        return "sold_out"
    if any(k in t for k in ["vähe pileteid", "viimased piletid", "limited"]):
        return "limited"
    if any(k in t for k in ["osta pilet", "osta piletid", "piletid", "buy tickets", "piletilevi"]):
        return "available"
    return None


# ---------------------------------------------------------------------------
# Theater-specific scrapers
# ---------------------------------------------------------------------------
def scrape_draamateater() -> Theater:
    t = Theater(id="draamateater", name="Eesti Draamateater", source_url="https://www.draamateater.ee/kava")
    try:
        html = fetch(t.source_url)
        (RAW / "draamateater.html").write_text(html, encoding="utf-8")
        soup = BeautifulSoup(html, "lxml")

        # Strategy 1: look for article-like repeating blocks with a
        # heading and a date-ish string.
        candidates: list[Tag] = []
        for tag_name in ("article", "li", "div"):
            for node in soup.find_all(tag_name):
                txt = text_of(node)
                if not txt or len(txt) < 20 or len(txt) > 2000:
                    continue
                # Must contain a date-like pattern.
                if not any(p.search(txt) for p in DATE_PATTERNS):
                    continue
                # Must have a link out (ideally ticket or show page).
                if not node.find("a"):
                    continue
                candidates.append(node)

        log(f"draamateater: {len(candidates)} candidate blocks")

        seen: set[str] = set()
        for node in candidates:
            # Title: prefer first heading, else first link text.
            title_node = node.find(["h1", "h2", "h3", "h4"])
            title = text_of(title_node) if title_node else ""
            if not title:
                a = node.find("a")
                title = text_of(a)
            if not title or len(title) < 2:
                continue

            block_text = text_of(node)
            raw_dt, iso_dt = parse_datetime(block_text)
            if not raw_dt:
                continue

            # Ticket / detail link: prefer piletilevi, else first link.
            ticket_url: str | None = None
            for a in node.find_all("a", href=True):
                href = a["href"]
                if "piletilevi" in href.lower() or "ticket" in href.lower():
                    ticket_url = absolute_url(t.source_url, href)
                    break
            if not ticket_url:
                a = node.find("a", href=True)
                if a:
                    ticket_url = absolute_url(t.source_url, a["href"])

            show = Show(
                id=make_show_id(t.id, title, iso_dt, raw_dt),
                title=title,
                datetime_str=raw_dt,
                iso_datetime=iso_dt,
                ticket_url=ticket_url,
                ticket_status=detect_ticket_status(block_text),
            )
            if show.id in seen:
                continue
            seen.add(show.id)
            t.shows.append(show)

        t.scraped_ok = True
        log(f"draamateater: extracted {len(t.shows)} shows")
    except Exception as exc:
        t.scraped_ok = False
        t.error = f"{type(exc).__name__}: {exc}"
        log(f"draamateater FAILED: {t.error}")
        log(traceback.format_exc())
    return t


def scrape_linnateater() -> Theater:
    t = Theater(id="linnateater", name="Tallinna Linnateater", source_url="https://linnateater.ee/kava")
    try:
        html = fetch(t.source_url)
        (RAW / "linnateater.html").write_text(html, encoding="utf-8")
        soup = BeautifulSoup(html, "lxml")

        candidates: list[Tag] = []
        for tag_name in ("article", "li", "div"):
            for node in soup.find_all(tag_name):
                txt = text_of(node)
                if not txt or len(txt) < 20 or len(txt) > 2000:
                    continue
                if not any(p.search(txt) for p in DATE_PATTERNS):
                    continue
                if not node.find("a"):
                    continue
                candidates.append(node)

        log(f"linnateater: {len(candidates)} candidate blocks")

        seen: set[str] = set()
        for node in candidates:
            title_node = node.find(["h1", "h2", "h3", "h4"])
            title = text_of(title_node) if title_node else ""
            if not title:
                a = node.find("a")
                title = text_of(a)
            if not title or len(title) < 2:
                continue

            block_text = text_of(node)
            raw_dt, iso_dt = parse_datetime(block_text)
            if not raw_dt:
                continue

            ticket_url: str | None = None
            for a in node.find_all("a", href=True):
                href = a["href"]
                href_l = href.lower()
                if "piletilevi" in href_l or "osta" in href_l or "ticket" in href_l:
                    ticket_url = absolute_url(t.source_url, href)
                    break
            if not ticket_url:
                a = node.find("a", href=True)
                if a:
                    ticket_url = absolute_url(t.source_url, a["href"])

            show = Show(
                id=make_show_id(t.id, title, iso_dt, raw_dt),
                title=title,
                datetime_str=raw_dt,
                iso_datetime=iso_dt,
                ticket_url=ticket_url,
                ticket_status=detect_ticket_status(block_text),
            )
            if show.id in seen:
                continue
            seen.add(show.id)
            t.shows.append(show)

        t.scraped_ok = True
        log(f"linnateater: extracted {len(t.shows)} shows")
    except Exception as exc:
        t.scraped_ok = False
        t.error = f"{type(exc).__name__}: {exc}"
        log(f"linnateater FAILED: {t.error}")
        log(traceback.format_exc())
    return t


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    log(f"scraper v{SCRAPER_VERSION} starting")

    theaters = [
        scrape_draamateater(),
        scrape_linnateater(),
    ]

    state: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "scraper_version": SCRAPER_VERSION,
        "theaters": [
            {
                "id": t.id,
                "name": t.name,
                "source_url": t.source_url,
                "scraped_ok": t.scraped_ok,
                "error": t.error,
                "shows": [asdict(s) for s in sorted(t.shows, key=lambda s: (s.iso_datetime or "9999", s.title))],
            }
            for t in theaters
        ],
    }

    (DATA / "state.json").write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    log(f"wrote data/state.json — draamateater={len(theaters[0].shows)}, linnateater={len(theaters[1].shows)}")

    (DATA / "log.txt").write_text("\n".join(_log_lines), encoding="utf-8")

    # Exit non-zero only if BOTH theaters failed — otherwise partial data is
    # still useful and we want the commit to happen.
    if not any(t.scraped_ok for t in theaters):
        log("both theaters failed — exiting non-zero")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
