#!/usr/bin/env python3
"""Scrape Eesti Draamateater and Tallinna Linnateater kava pages.

The scraper does NOT hardcode kava URLs — it fetches each theater's homepage
first, discovers the most likely "Kava / Repertuaar" link from the navigation,
follows it, and parses the resulting page. If discovery fails it falls back to
parsing the homepage itself. This makes the scraper resilient to URL changes.

Output:
  data/state.json    - combined structured state
  data/raw/*.html    - raw HTML snapshots (diagnostic)
  data/log.txt       - run log
"""
from __future__ import annotations

import json
import re
import sys
import traceback
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup, Tag
from dateutil import parser as dateparser

SCRAPER_VERSION = "1.1.0"
UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36 TeatripiletidScraper/" + SCRAPER_VERSION
)
TIMEOUT = 30
KAVA_KEYWORDS = [
    "kava", "repertuaar", "repertoire", "etendused", "programm",
    "programme", "calendar", "kalender", "lava",
]

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
    ticket_status: str | None = None
    price_range: str | None = None


@dataclass
class Theater:
    id: str
    name: str
    homepage: str
    source_url: str = ""  # filled once discovery succeeds
    scraped_ok: bool = False
    error: str | None = None
    shows: list[Show] = field(default_factory=list)


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
def new_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "et,en;q=0.5",
    })
    return s


def fetch(session: requests.Session, url: str, allow_404: bool = False) -> tuple[int, str]:
    log(f"GET {url}")
    resp = session.get(url, timeout=TIMEOUT, allow_redirects=True)
    if not allow_404:
        resp.raise_for_status()
    resp.encoding = resp.apparent_encoding or resp.encoding
    log(f"  -> {resp.status_code}, final url {resp.url}, {len(resp.text)} bytes")
    return resp.status_code, resp.text


def fetch_first_ok(session: requests.Session, urls: list[str]) -> tuple[str | None, str | None]:
    """Try URLs in order, return (url, html) of first that returns 200 with content."""
    for u in urls:
        try:
            code, html = fetch(session, u, allow_404=True)
            if code == 200 and html and len(html) > 500:
                return u, html
        except Exception as e:
            log(f"  fetch exception for {u}: {e}")
    return None, None


# ---------------------------------------------------------------------------
# Kava URL discovery
# ---------------------------------------------------------------------------
def discover_kava_urls(base_url: str, html: str) -> list[str]:
    """Score all links on a homepage by how likely they point to the
    programme / repertoire page. Returns top candidates in score order."""
    soup = BeautifulSoup(html, "lxml")
    base_host = urlparse(base_url).netloc.lower()
    scored: list[tuple[int, str]] = []
    for a in soup.find_all("a", href=True):
        text = (a.get_text() or "").strip().lower()
        href = a["href"].strip()
        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue
        abs_url = urljoin(base_url, href)
        # Only consider same-host links (avoid leaving the site).
        if urlparse(abs_url).netloc.lower() != base_host:
            continue
        score = 0
        for kw in KAVA_KEYWORDS:
            if text == kw:
                score += 100
            elif text.startswith(kw + " ") or text.endswith(" " + kw):
                score += 50
            elif kw in text:
                score += 25
            if kw in abs_url.lower():
                score += 10
        if score > 0:
            scored.append((score, abs_url))
    # dedupe preserving highest score per url
    best: dict[str, int] = {}
    for score, url in scored:
        if url not in best or best[url] < score:
            best[url] = score
    ranked = sorted(best.items(), key=lambda kv: kv[1], reverse=True)
    urls = [u for u, _ in ranked[:6]]
    log(f"  kava candidates: {urls}")
    return urls


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
    # 10.05.2026 19:00 / 10.05 19:00 / 10.05.26
    re.compile(r"(?P<d>\d{1,2})\.(?P<m>\d{1,2})\.?(?P<y>\d{2,4})?(?:[^\d]{0,6}(?:kell\s*)?(?P<h>\d{1,2})[.:](?P<mi>\d{2}))?"),
    # 10. mai 2026 19:00
    re.compile(
        r"(?P<d>\d{1,2})\.?\s+(?P<mon>"
        + "|".join(sorted(EST_MONTHS.keys(), key=len, reverse=True))
        + r")\w*\s+(?P<y>\d{4})(?:[^\d]{0,6}(?:kell\s*)?(?P<h>\d{1,2})[.:](?P<mi>\d{2}))?",
        re.IGNORECASE | re.UNICODE,
    ),
    # 2026-05-10T19:00 or 2026-05-10 19:00
    re.compile(r"(?P<y>\d{4})-(?P<m>\d{2})-(?P<d>\d{2})(?:[T\s](?P<h>\d{2}):(?P<mi>\d{2}))?"),
]


def parse_datetime(text: str) -> tuple[str | None, str | None]:
    if not text:
        return None, None
    text = re.sub(r"\s+", " ", text).strip()

    for pat in DATE_PATTERNS:
        m = pat.search(text)
        if not m:
            continue
        gd = m.groupdict()
        try:
            if gd.get("mon"):
                month = EST_MONTHS[gd["mon"].lower()]
            else:
                month = int(gd["m"])
            year_str = gd.get("y")
            if year_str:
                year = int(year_str)
                if year < 100:
                    year = 2000 + year
            else:
                # no year — assume current or next year based on month vs today
                now = datetime.now()
                year = now.year if month >= now.month else now.year + 1
            day = int(gd["d"])
            hour = int(gd["h"]) if gd.get("h") else 0
            minute = int(gd["mi"]) if gd.get("mi") else 0
            iso = f"{year:04d}-{month:02d}-{day:02d}T{hour:02d}:{minute:02d}:00"
            return m.group(0), iso
        except Exception:
            continue

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
    return urljoin(base, href)


def detect_ticket_status(fragment_text: str) -> str | None:
    t = fragment_text.lower()
    if any(k in t for k in ["läbi müüdud", "välja müüdud", "välja mü", "sold out", "välja mü"]):
        return "sold_out"
    if any(k in t for k in ["vähe pileteid", "viimased piletid", "limited"]):
        return "limited"
    if any(k in t for k in ["osta pilet", "osta piletid", "piletid", "buy tickets", "piletilevi"]):
        return "available"
    return None


# ---------------------------------------------------------------------------
# Generic block-based show extraction
# ---------------------------------------------------------------------------
def extract_shows_from_html(html: str, theater_id: str, source_url: str) -> list[Show]:
    soup = BeautifulSoup(html, "lxml")
    candidates: list[Tag] = []
    # A "candidate" is any DOM node whose text contains a date-like pattern
    # AND has a link AND a reasonable size.
    for tag_name in ("article", "li", "div", "section"):
        for node in soup.find_all(tag_name):
            txt = text_of(node)
            if not txt or len(txt) < 15 or len(txt) > 2500:
                continue
            if not any(p.search(txt) for p in DATE_PATTERNS):
                continue
            if not node.find("a"):
                continue
            candidates.append(node)

    # Favor leaf nodes (don't double-count parent + child wrapping same info)
    leaves: list[Tag] = []
    cand_set = set(id(c) for c in candidates)
    for c in candidates:
        has_child_candidate = False
        for d in c.find_all(True):
            if id(d) in cand_set and d is not c:
                has_child_candidate = True
                break
        if not has_child_candidate:
            leaves.append(c)
    log(f"  {theater_id}: {len(candidates)} candidate blocks, {len(leaves)} leaves")

    shows: list[Show] = []
    seen: set[str] = set()
    for node in leaves:
        title_node = node.find(["h1", "h2", "h3", "h4", "h5"])
        title = text_of(title_node) if title_node else ""
        if not title:
            a = node.find("a")
            title = text_of(a)
        title = (title or "").strip()
        if not title or len(title) < 2:
            continue

        block_text = text_of(node)
        raw_dt, iso_dt = parse_datetime(block_text)
        if not raw_dt:
            continue

        ticket_url: str | None = None
        detail_url: str | None = None
        for a in node.find_all("a", href=True):
            href_raw = a["href"]
            href_l = href_raw.lower()
            if "piletilevi" in href_l or "ticketer" in href_l:
                ticket_url = absolute_url(source_url, href_raw)
                break
            atext = (a.get_text() or "").strip().lower()
            if any(k in atext for k in ["osta", "buy", "pilet", "ticket"]):
                ticket_url = absolute_url(source_url, href_raw)
                break
        if not ticket_url:
            a = node.find("a", href=True)
            if a:
                detail_url = absolute_url(source_url, a["href"])

        show = Show(
            id=make_show_id(theater_id, title, iso_dt, raw_dt),
            title=title,
            datetime_str=raw_dt,
            iso_datetime=iso_dt,
            ticket_url=ticket_url or detail_url,
            ticket_status=detect_ticket_status(block_text),
        )
        if show.id in seen:
            continue
        seen.add(show.id)
        shows.append(show)

    return shows


# ---------------------------------------------------------------------------
# Per-theater entrypoints
# ---------------------------------------------------------------------------
def scrape_theater(t: Theater) -> Theater:
    session = new_session()
    try:
        # Step 1: fetch homepage
        code, home_html = fetch(session, t.homepage, allow_404=True)
        if code != 200 or not home_html:
            raise RuntimeError(f"homepage fetch returned {code}")

        (RAW / f"{t.id}_home.html").write_text(home_html, encoding="utf-8")

        # Step 2: discover candidate kava URLs from homepage links.
        candidates = discover_kava_urls(t.homepage, home_html)

        # Step 3: try each candidate, pick first that returns 200 with enough body.
        chosen_url, chosen_html = fetch_first_ok(session, candidates)

        if not chosen_url:
            log(f"  {t.id}: no kava candidate worked, using homepage as source")
            chosen_url = t.homepage
            chosen_html = home_html

        t.source_url = chosen_url
        (RAW / f"{t.id}_kava.html").write_text(chosen_html or "", encoding="utf-8")

        # Step 4: parse the chosen page for shows
        shows = extract_shows_from_html(chosen_html, t.id, chosen_url)

        # Also mine the homepage itself — many theater homepages list upcoming
        # shows on the front page too. Merge by show id.
        if chosen_url != t.homepage:
            extra = extract_shows_from_html(home_html, t.id, t.homepage)
            have = {s.id for s in shows}
            for s in extra:
                if s.id not in have:
                    shows.append(s)
                    have.add(s.id)

        t.shows = shows
        t.scraped_ok = True
        log(f"{t.id}: {len(shows)} shows extracted from {chosen_url}")
    except Exception as exc:
        t.scraped_ok = False
        t.error = f"{type(exc).__name__}: {exc}"
        log(f"{t.id} FAILED: {t.error}")
        log(traceback.format_exc())
    return t


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    log(f"scraper v{SCRAPER_VERSION} starting")

    theaters = [
        scrape_theater(Theater(
            id="draamateater",
            name="Eesti Draamateater",
            homepage="https://www.draamateater.ee/",
        )),
        scrape_theater(Theater(
            id="linnateater",
            name="Tallinna Linnateater",
            homepage="https://linnateater.ee/",
        )),
    ]

    state: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "scraper_version": SCRAPER_VERSION,
        "theaters": [
            {
                "id": t.id,
                "name": t.name,
                "homepage": t.homepage,
                "source_url": t.source_url or t.homepage,
                "scraped_ok": t.scraped_ok,
                "error": t.error,
                "shows": [asdict(s) for s in sorted(t.shows, key=lambda s: (s.iso_datetime or "9999", s.title))],
            }
            for t in theaters
        ],
    }

    (DATA / "state.json").write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    log(
        "wrote data/state.json — "
        f"draamateater={len(theaters[0].shows)} ok={theaters[0].scraped_ok}, "
        f"linnateater={len(theaters[1].shows)} ok={theaters[1].scraped_ok}"
    )

    (DATA / "log.txt").write_text("\n".join(_log_lines), encoding="utf-8")

    # Exit non-zero ONLY if every theater failed both homepage and discovery —
    # we still want successful partial scrapes to commit.
    if not any(t.scraped_ok for t in theaters):
        log("both theaters failed — exiting non-zero")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
