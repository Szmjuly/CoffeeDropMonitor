#!/usr/bin/env python3
"""
Coffee Drop Monitor with Tasting Notes
=====================================

This script monitors several specialty coffee roasters for new releases and
notifies subscribers via ntfy. It extends the previous implementation by
fetching individual product pages to extract additional metadata for each
coffee, including tasting notes, origin/region, producer, variety, and
processing method. These details are stored in Firestore alongside the
basic product record and can be surfaced on your static site via deep
links.

Supported roasters (as of August 2025):

* Black & White Coffee Roasters
* Moonwake Coffee Roasters
* SEY Coffee
* Prodigal Coffee
* Hydrangea Coffee
* Brandywine Coffee Roasters

The script gracefully handles cases where a roaster’s site does not expose
these details by leaving those fields blank. Additional roaster
configurations can be added easily at the bottom of this file.

Usage:

    # Normal scrape
    python coffee_monitor.py run

    # Simulate N new drops across specified roasters
    python coffee_monitor.py simulate --count 5 --roasters "Black & White Coffee Roasters,SEY Coffee"

Environment variables (.env or exported) required:

    GOOGLE_APPLICATION_CREDENTIALS  Path to your Firebase service-account JSON
    FIREBASE_PROJECT_ID            Your Firebase project ID
    FIREBASE_COLLECTION            Firestore collection name (default 'coffees')
    NTFY_TOPIC                     ntfy topic for push notifications
    NTFY_SERVER                    ntfy server (default https://ntfy.sh)
    SITE_BASE_URL                  Base URL of your GitHub Pages site

Optional environment variables:

    NTFY_CLICK_URL                 Fallback click URL for ntfy notifications
    COFFEE_DB                      SQLite database path for local state

See README or project documentation for full setup instructions.
"""

from __future__ import annotations
import os
import re
import time
import hashlib
import argparse
import sqlite3
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import List, Optional, Iterable, Tuple, Dict, Callable, Set

import requests
from bs4 import BeautifulSoup
from collections import defaultdict

try:
    from dotenv import load_dotenv
except Exception:
    # Optional dependency; if not present the script still works
    load_dotenv = None

# Firestore client
from google.cloud import firestore

# -----------------------------------------------------------------------------
# Configuration and constants

ISO = "%Y-%m-%d %H:%M:%S%z"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Determine base directory for relative paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# SQLite database path; can be overridden via COFFEE_DB env var
DB_PATH = os.environ.get("COFFEE_DB", os.path.join(BASE_DIR, "coffee_firestore.db"))

# Help text constants
DEV_HELP = "Use local firebase.json for credentials."
TRIED_DEFAULT_COLLECTION = "coffees_tried"


@dataclass
class Product:
    """Represents a single coffee product on a roaster’s site."""

    roaster: str
    title: str
    url: str
    price_text: str
    in_stock: bool
    producer: str = ""
    country: str = ""
    region: str = ""
    process: str = ""
    variety: str = ""
    notes: str = ""
    profile: str = ""
    image: str = ""

    def id(self) -> str:
        """Stable identifier derived from the product URL."""
        return hashlib.sha1(self.url.encode("utf-8")).hexdigest()[:16]


def product_id_from_url(url: str) -> str:
    """Compute the same product ID used for Firestore doc IDs from a URL."""
    return hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]


@dataclass
class RoasterConfig:
    """Configuration for scraping a roaster’s listing page."""

    name: str
    start_url: str
    product_selector: str
    title_selector: str
    link_selector: str
    price_selector: Optional[str]
    soldout_selector: Optional[str]

    def sleep_between(self) -> float:
        """Delay between product requests to be polite to servers."""
        return 1.2


# List of roaster configurations. To add a new roaster, append a new
# RoasterConfig entry and optionally register a detail parser (see
# DETAIL_PARSERS below).
ROASTERS: List[RoasterConfig] = [
    # UPDATED CONFIGURATION FOR BLACK & WHITE ROASTERS
    RoasterConfig(
        name="Black & White Coffee Roasters",
        start_url="https://www.blackwhiteroasters.com/collections/all-coffee",
        product_selector="product-block",
        title_selector="div.product-block__title",
        link_selector="a.product-link",
        price_selector="span.price__current",
        soldout_selector="span.price-label--sold-out",
    ),
    RoasterConfig(
        name="Moonwake Coffee Roasters",
        start_url="https://moonwakecoffeeroasters.com/pages/shop-coffees",
        product_selector=(
            "li.grid__item, div.product-grid__item, div.collection-product, div.card__content, div.card-information"
        ),
        title_selector="a[href*='/products/'], h3.card__heading a",
        link_selector="a[href*='/products/']",
        price_selector="span.price-item, span.price-item--regular, span.price, span.money",
        soldout_selector="span.badge--sold-out, span.sold-out, button[disabled], p.sold-out, span.badge--sold-out",
    ),
    # New roasters
    RoasterConfig(
        name="SEY Coffee",
        start_url="https://www.seycoffee.com/collections/coffee",
        # The listing is minimal; just anchor tags. Use general selectors.
        product_selector="a[href*='/products/']",
        title_selector="a[href*='/products/']",
        link_selector="a[href*='/products/']",
        price_selector=None,
        soldout_selector=None,
    ),
    RoasterConfig(
        name="Prodigal Coffee",
        start_url="https://getprodigal.com/collections/roasted-coffee",
        product_selector="a[href*='/products/']",
        title_selector="a[href*='/products/']",
        link_selector="a[href*='/products/']",
        price_selector=None,
        soldout_selector=None,
    ),
    RoasterConfig(
        name="Hydrangea Coffee Roasters",
        start_url="https://hydrangea.coffee/",
        product_selector="a[href*='/products/']",
        title_selector="a[href*='/products/']",
        link_selector="a[href*='/products/']",
        price_selector=None,
        soldout_selector=None,
    ),
    RoasterConfig(
        name="Brandywine Coffee Roasters",
        start_url="https://www.brandywinecoffeeroasters.com/collections/all-coffee-1",
        product_selector=(
            "li.grid__item, div.product-grid__item, div.collection-product, div.product-card"
        ),
        title_selector="a[href*='/products/']",
        link_selector="a[href*='/products/']",
        price_selector="span.price-item, span.price-item--regular, span.money",
        soldout_selector="span.badge--sold-out, span.sold-out, button[disabled], span.sold-out-badge",
    ),
]


# -----------------------------------------------------------------------------
# SQLite storage (for new-item detection)

SCHEMA = """
CREATE TABLE IF NOT EXISTS coffees (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  roaster TEXT NOT NULL,
  title TEXT NOT NULL,
  url TEXT NOT NULL UNIQUE,
  price TEXT,
  in_stock INTEGER NOT NULL DEFAULT 1,
  first_seen TEXT NOT NULL,
  last_seen TEXT NOT NULL,
  producer TEXT DEFAULT '',
  country TEXT DEFAULT '',
  region TEXT DEFAULT '',
  process TEXT DEFAULT '',
  variety TEXT DEFAULT '',
  notes TEXT DEFAULT '',
  profile TEXT DEFAULT ''
);
"""


def ensure_db_schema(conn: sqlite3.Connection) -> None:
    """Add any missing columns to keep SQLite schema aligned with Firestore.

    - Adds columns: price, producer, country, region, process, variety, notes, profile
    - If a legacy column price_text exists and price is missing, populate price from it.
    """
    cur = conn.cursor()
    cols = {row[1] for row in cur.execute("PRAGMA table_info('coffees')").fetchall()}
    to_add: List[Tuple[str, str]] = []
    if "price" not in cols:
        to_add.append(("price", "TEXT DEFAULT ''"))
    if "tried" not in cols:
        to_add.append(("tried", "INTEGER DEFAULT 0"))
    for c in ("producer", "country", "region", "process", "variety", "notes", "profile"):
        if c not in cols:
            to_add.append((c, "TEXT DEFAULT ''"))
    for name, decl in to_add:
        cur.execute(f"ALTER TABLE coffees ADD COLUMN {name} {decl}")
    # Populate price from legacy price_text if present
    cols_after = {row[1] for row in cur.execute("PRAGMA table_info('coffees')").fetchall()}
    if "price" in cols_after and "price_text" in cols_after:
        cur.execute("UPDATE coffees SET price = COALESCE(NULLIF(price, ''), price_text)")
    conn.commit()


def db_connect() -> sqlite3.Connection:
    """Connect to (and initialize) the local SQLite database.

    Respects the COFFEE_DB environment variable at runtime, falling back to
    the default DB_PATH determined at import.
    """
    path = os.environ.get("COFFEE_DB", DB_PATH)
    db_dir = os.path.dirname(path) or "."
    os.makedirs(db_dir, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute(SCHEMA)
    ensure_db_schema(conn)
    return conn


# -----------------------------------------------------------------------------
# HTTP helpers

def fetch_html(url: str, timeout: float = 30.0, retries: int = 3) -> str:
    """Fetch a URL and return its text. Retries on transient errors."""
    last_err: Optional[Exception] = None
    for i in range(retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=timeout)
            if resp.status_code >= 400:
                raise RuntimeError(f"HTTP {resp.status_code} for {url}")
            return resp.text
        except Exception as exc:
            last_err = exc
            time.sleep(1.0 * (i + 1))
    raise RuntimeError(f"Failed to fetch {url}: {last_err}")


_space_re = re.compile(r"\s+")


def normalize_space(s: str) -> str:
    """Collapse runs of whitespace into a single space and strip."""
    return _space_re.sub(" ", s).strip()


def parse_products(cfg: RoasterConfig, html: str) -> List[Product]:
    """Parse a roaster's listing page into basic Product objects."""
    soup = BeautifulSoup(html, "html.parser")
    out: List[Product] = []
    nodes = soup.select(cfg.product_selector)
    seen_urls = set()

    def extract(node) -> Optional[Product]:
        link = node.select_one(cfg.link_selector)
        if not link or not link.get("href"):
            return None
        href = link.get("href")
        # Build absolute URL if necessary
        if href.startswith("/"):
            from urllib.parse import urljoin

            url = urljoin(cfg.start_url, href)
        else:
            url = href
        title_el = node.select_one(cfg.title_selector) or link
        title = normalize_space(title_el.get_text(" "))
        # Try to extract an image URL if present
        img_url = ""
        try:
            img_el = node.select_one('img')
            if img_el and (img_el.get('src') or img_el.get('data-src') or img_el.get('data-srcset')):
                img_url = img_el.get('src') or img_el.get('data-src') or (img_el.get('data-srcset') or '').split(' ')[0]
        except Exception:
            img_url = ""
        price = ""
        pe = node.select_one(cfg.price_selector) if cfg.price_selector else None
        if pe:
            price = normalize_space(pe.get_text(" "))
        in_stock = True
        se = node.select_one(cfg.soldout_selector) if cfg.soldout_selector else None
        if se and re.search(r"sold\s*out", se.get_text(" "), re.I):
            in_stock = False
        # Additional fallback: inspect full text for 'Sold out'
        if in_stock and re.search(r"\bSold\s*out\b", node.get_text(" "), re.I):
            in_stock = False
        return Product(
            roaster=cfg.name,
            title=title,
            url=url,
            price_text=price,
            in_stock=in_stock,
            image=img_url,
        )

    for n in nodes:
        prod = extract(n)
        if prod and prod.url not in seen_urls:
            out.append(prod)
            seen_urls.add(prod.url)
    # Fallback: if nothing parsed, try to collect all links to /products/
    if not out:
        for a in soup.select("a[href*='/products/']"):
            href = a.get("href") or ""
            if not href:
                continue
            if href.startswith("/"):
                from urllib.parse import urljoin

                url = urljoin(cfg.start_url, href)
            else:
                url = href
            if url in seen_urls:
                continue
            title = normalize_space(a.get_text(" ")) or "(untitled)"
            out.append(Product(roaster=cfg.name, title=title, url=url, price_text="", in_stock=True))
            seen_urls.add(url)
    return out


# -----------------------------------------------------------------------------
# Detail parsers per roaster

def parse_details_blackwhite(html: str) -> Dict[str, str]:
    """Extract tasting notes and other metadata from a Black & White product page."""
    # Use BeautifulSoup to get clean text
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n")
    details: Dict[str, str] = {}
    # Tasting notes: look for 'TAKE A SIP' section or bullet notes
    # We'll search for lines after 'TAKE A SIP' that describe flavor notes
    m = re.search(r"TAKE\s+A\s+SIP\s*\|\s*(.*?)\n", text, re.IGNORECASE)
    if m:
        # The matched string may include sentences; take until the next capitalised section
        notes = m.group(1).strip()
        details["notes"] = notes
    else:
        # Fallback: search for typical flavor descriptors like 'notes of'
        m2 = re.search(r"notes of ([^.\n]+)", text, re.IGNORECASE)
        if m2:
            details["notes"] = m2.group(1).strip()
    # Origin, Producer, Process, Variety
    for label in ["Origin", "Producer", "Process", "Variety"]:
        pattern = rf"{label}\s*\|\s*([^\n]+)"
        m = re.search(pattern, text)
        if m:
            val = m.group(1).strip()
            key = label.lower()  # origin -> origin
            details[key] = val

    # If an origin was found, treat it as region and derive country
    # Some Black & White pages list "Origin | Santa Maria, Huila, Colombia"
    # We'll split by comma to extract the country (last element)
    origin_val = details.get("origin")
    if origin_val:
        # Set region equal to the full origin
        details.setdefault("region", origin_val)
        parts = origin_val.split(",")
        if len(parts) > 1:
            # Country is last component
            details.setdefault("country", parts[-1].strip())
    return details


def parse_details_moonwake(html: str) -> Dict[str, str]:
    """Extract tasting notes and other metadata from a Moonwake product page."""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n")
    details: Dict[str, str] = {}
    # Tasting notes
    m = re.search(r"Tasting Notes:\s*([^\n]+)", text)
    if m:
        details["notes"] = m.group(1).strip()
    # Region
    m = re.search(r"Region:\s*([^\n]+)", text)
    if m:
        details["region"] = m.group(1).strip()
        # Split country and region if comma present
        parts = details["region"].split(",")
        if len(parts) > 1:
            details["country"] = parts[-1].strip()
    # Producer
    m = re.search(r"Producer:\s*([^\n]+)", text)
    if m:
        details["producer"] = m.group(1).strip()
    # Process
    m = re.search(r"Process:\s*([^\n]+)", text)
    if m:
        details["process"] = m.group(1).strip()
    # Variety
    m = re.search(r"Variety:\s*([^\n]+)", text)
    if m:
        details["variety"] = m.group(1).strip()
    return details


def parse_details_sey(html: str) -> Dict[str, str]:
    """Extract tasting notes and other metadata from a SEY product page."""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n")
    details: Dict[str, str] = {}
    # Notes appear after 'In the cup we find'
    m = re.search(r"In the cup we find ([^.\n]+)", text)
    if m:
        details["notes"] = m.group(1).strip()
    # Region
    m = re.search(r"REGION\s*\n\s*([^\n]+)", text)
    if m:
        region = m.group(1).strip()
        details["region"] = region
        parts = region.split(",")
        if len(parts) > 1:
            details["country"] = parts[-1].strip()
    # Producer (often not listed for SEY; skip if absent)
    m = re.search(r"PRODUCER\s*\n\s*([^\n]+)", text)
    if m:
        details["producer"] = m.group(1).strip()
    # Process (under PROCESSING)
    m = re.search(r"PROCESSING\s*\n\s*([^\n]+)", text)
    if m:
        details["process"] = m.group(1).strip()
    # Variety
    m = re.search(r"VARIETAL\s*\n\s*([^\n]+)", text)
    if m:
        details["variety"] = m.group(1).strip()
    return details


def parse_details_prodigal(html: str) -> Dict[str, str]:
    """Extract tasting notes and other metadata from a Prodigal Coffee product page."""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n")
    details: Dict[str, str] = {}
    # Tasting notes may be in the meta description (jasmine florals, ripe peach ...). Use meta tag first.
    # If none, search for typical descriptors in body.
    for meta in soup.find_all('meta'):
        if meta.get('name') == 'description' and meta.get('content'):
            content = meta['content']
            # Extract part after a dash or '—'
            m = re.search(r"–\s*([^\n]+)", content)
            if m:
                details["notes"] = m.group(1).strip()
            else:
                details["notes"] = content.strip()
            break
    # Process, Region, Variety, Producer appear as 'Process:' etc.
    patterns = {
        "process": r"Process:\s*([^\n]+)",
        "region": r"Region:\s*([^\n]+)",
        "variety": r"Variety:\s*([^\n]+)",
        "producer": r"Producer:\s*([^\n]+)",
    }
    for key, pat in patterns.items():
        m = re.search(pat, text)
        if m:
            details[key] = m.group(1).strip()
    # If region contains country information
    if "region" in details and "country" not in details:
        parts = details["region"].split(",")
        if len(parts) > 1:
            details["country"] = parts[-1].strip()
    return details


def parse_details_hydrangea(html: str) -> Dict[str, str]:
    """Extract tasting notes and other metadata from a Hydrangea Coffee product page."""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n")
    details: Dict[str, str] = {}
    # The page exposes 'Tastes Like:' etc.
    m = re.search(r"Tastes Like:\s*([^\n]+)", text)
    if m:
        details["notes"] = m.group(1).strip()
    m = re.search(r"Origin:\s*([^\n]+)", text)
    if m:
        details["region"] = m.group(1).strip()
        parts = details["region"].split(",")
        if len(parts) > 1:
            details["country"] = parts[-1].strip()
    m = re.search(r"Variety:\s*([^\n]+)", text)
    if m:
        details["variety"] = m.group(1).strip()
    m = re.search(r"Producer:\s*([^\n]+)", text)
    if m:
        details["producer"] = m.group(1).strip()
    m = re.search(r"Process:\s*([^\n]+)", text)
    if m:
        details["process"] = m.group(1).strip()
    return details


def parse_details_brandywine(html: str) -> Dict[str, str]:
    """Extract tasting notes and other metadata from a Brandywine Coffee product page."""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n")
    details: Dict[str, str] = {}
    # Tasting notes: search around 'TAKE A SIP' or 'notes of'
    m = re.search(r"TAKE\s+A\s+SIP\s*\|\s*(.*?)\n", text, re.IGNORECASE)
    if m:
        details["notes"] = m.group(1).strip()
    else:
        m2 = re.search(r"notes of ([^.\n]+)", text, re.IGNORECASE)
        if m2:
            details["notes"] = m2.group(1).strip()
    # Origin, Producer, Process, Variety lines separated by '|'
    for label in ["Origin", "Producer", "Process", "Variety"]:
        pat = rf"{label}\s*\|\s*([^\n]+)"
        m = re.search(pat, text)
        if m:
            details[label.lower()] = m.group(1).strip()
    # If origin exists, derive country
    if "origin" in details and "country" not in details:
        parts = details["origin"].split(",")
        if len(parts) > 1:
            details["country"] = parts[-1].strip()
        details["region"] = details["origin"]
    return details


# Mapping of roaster names to detail parser functions
DETAIL_PARSERS: Dict[str, Callable[[str], Dict[str, str]]] = {
    "Black & White Coffee Roasters": parse_details_blackwhite,
    "Moonwake Coffee Roasters": parse_details_moonwake,
    "SEY Coffee": parse_details_sey,
    "Prodigal Coffee": parse_details_prodigal,
    "Hydrangea Coffee Roasters": parse_details_hydrangea,
    "Brandywine Coffee Roasters": parse_details_brandywine,
}


def scrape_product_details(url: str, roaster: str) -> Dict[str, str]:
    """Fetch and parse a single product page to extract additional metadata."""
    try:
        html = fetch_html(url)
    except Exception as exc:
        print(f"[WARN] Failed to fetch product page {url}: {exc}")
        return {}
    parser = DETAIL_PARSERS.get(roaster)
    if not parser:
        return {}
    try:
        details = parser(html)
        # Generic image extraction fallback from product page
        try:
            soup = BeautifulSoup(html, "html.parser")
            img = soup.find('meta', attrs={'property': 'og:image'}) or soup.find('img')
            if img:
                src = img.get('content') or img.get('src') or img.get('data-src')
                if src and 'image' not in details:
                    details['image'] = src
        except Exception:
            pass
        return details
    except Exception as exc:
        print(f"[WARN] Failed to parse details for {url}: {exc}")
        return {}


# -----------------------------------------------------------------------------
# Firestore persistence and notification logic

def load_env() -> dict:
    """Load environment variables from a .env file if available."""
    if load_dotenv:
        load_dotenv(os.path.join(BASE_DIR, ".env"))
    return dict(os.environ)


def firestore_client(env: dict):
    """Instantiate a Firestore client from environment configuration."""
    # GOOGLE_APPLICATION_CREDENTIALS should point to the JSON key
    project = env.get("FIREBASE_PROJECT_ID")
    if project:
        return firestore.Client(project=project)
    # Allow auto-detection from credentials if project not provided
    return firestore.Client()


def upsert_firestore(db, env: dict, p: Product, first_seen: str, last_seen: str) -> str:
    """Write or merge a product record into Firestore."""
    col = env.get("FIREBASE_COLLECTION", "coffees")
    doc_id = p.id()
    db.collection(col).document(doc_id).set(
        {
            "roaster": p.roaster,
            "title": p.title,
            "url": p.url,
            "price": p.price_text,
            "in_stock": bool(p.in_stock),
            "first_seen": first_seen,
            "last_seen": last_seen,
            "producer": p.producer,
            "country": p.country,
            "region": p.region,
            "process": p.process,
            "variety": p.variety,
            "notes": p.notes,
            "profile": p.profile,
            "image": p.image,
        },
        merge=True,
    )
    return doc_id


def load_firestore_index(db, env: dict) -> Dict[str, str]:
    """Load a mapping of Firestore doc_id -> first_seen for the collection."""
    col = env.get("FIREBASE_COLLECTION", "coffees")
    out: Dict[str, str] = {}
    for doc in db.collection(col).stream():
        data = doc.to_dict() or {}
        out[doc.id] = str(data.get("first_seen", ""))
    return out


def mark_stale_by_roaster(db, env: dict, seen_ids_by_roaster: Dict[str, Set[str]], batch_size: int = 400) -> int:
    """Mark Firestore docs as out-of-stock for processed roasters when their IDs were not seen.

    For each roaster in seen_ids_by_roaster, updates docs in the target collection whose
    document IDs are not present in the seen set, setting in_stock=False. Performs batched
    updates and returns the total number of documents updated.
    """
    col = env.get("FIREBASE_COLLECTION", "coffees")
    total = 0
    for roaster, seen_ids in seen_ids_by_roaster.items():
        try:
            query = db.collection(col).where("roaster", "==", roaster)
            batch = db.batch()
            ops = 0
            for d in query.stream():
                if seen_ids and d.id in seen_ids:
                    continue
                batch.update(d.reference, {"in_stock": False})
                ops += 1
                if ops >= batch_size:
                    batch.commit()
                    total += ops
                    batch = db.batch()
                    ops = 0
            if ops:
                batch.commit()
                total += ops
        except Exception as exc:
            print(f"[WARN] Firestore staleness for roaster '{roaster}' failed: {exc}")
            continue
    return total

def _truncate_lines(lines: List[str], max_lines: int = 8) -> List[str]:
    """Shorten long notification bodies with a '+N more…' suffix."""
    if len(lines) <= max_lines:
        return lines
    hidden = len(lines) - max_lines
    return lines[:max_lines] + [f"+{hidden} more…"]


def build_ntfy_message(new_items: List[Tuple[Product, str]]) -> Tuple[str, List[str], Optional[str]]:
    """
    Build a contextual ntfy notification title and body.

    Returns (title, body_lines, deep_id) where deep_id is used for click
    through to the first new coffee. See README for details.
    """
    if not new_items:
        return "", [], None
    n = len(new_items)
    first_pid = new_items[0][1]
    roasters = [p.roaster for (p, _) in new_items]
    unique_roasters = sorted(set(roasters))
    # Single new coffee
    if n == 1:
        p, pid = new_items[0]
        title = f"New from {p.roaster}: {p.title}"
        bits = []
        if p.price_text:
            bits.append(p.price_text)
        body = [(" • ".join(bits) if bits else "New coffee!"), p.url]
        return title, body, pid
    # Multiple
    by_r: Dict[str, List[Tuple[Product, str]]] = defaultdict(list)
    for p, pid in new_items:
        by_r[p.roaster].append((p, pid))
    if len(unique_roasters) == 1:
        r = unique_roasters[0]
        title = f"{n} new coffees from {r}"
        lines = [
            f"• {p.title}" + (f" — {p.price_text}" if p.price_text else "")
            for (p, _) in by_r[r]
        ]
        lines = _truncate_lines(lines, max_lines=8)
        return title, lines, first_pid
    else:
        title = f"New drops: {n} coffees from {len(unique_roasters)} roasters"
        lines: List[str] = []
        for r in unique_roasters:
            items = by_r[r]
            shown = items[:2]
            extras = len(items) - len(shown)
            ro_line = f"{r}: " + "; ".join(p.title for (p, _) in shown)
            if extras > 0:
                ro_line += f"; +{extras} more"
            lines.append(f"• {ro_line}")
        lines = _truncate_lines(lines, max_lines=8)
        return title, lines, first_pid


def notify_ntfy(
    lines: List[str],
    env: dict,
    deep_id: Optional[str] = None,
    title: str = "New coffee drops!",
    deep_ids: Optional[List[str]] = None,
) -> None:
    """Send a push notification via ntfy."""
    topic = env.get("NTFY_TOPIC")
    server = (env.get("NTFY_SERVER") or "https://ntfy.sh").rstrip("/")
    site = env.get("SITE_BASE_URL")
    click_url = env.get("NTFY_CLICK_URL")
    if not topic:
        return
    headers = {
        "Title": title,
        "Priority": "high",
        "Tags": "coffee",
    }
    # Determine click action: multi-id deep link takes precedence
    if site and deep_ids:
        ids_joined = ",".join(deep_ids[:6])  # limit length
        headers["Click"] = site.rstrip("/") + f"/?ids={ids_joined}"
    elif site and deep_id:
        headers["Click"] = site.rstrip("/") + f"/?id={deep_id}"
    elif click_url:
        headers["Click"] = click_url
    try:
        requests.post(
            f"{server}/{topic}",
            data=("\n".join(lines)).encode("utf-8"),
            headers=headers,
            timeout=30,
        )
    except Exception as exc:
        print(f"[WARN] ntfy failed: {exc}")


# -----------------------------------------------------------------------------
# Destructive maintenance commands
def _delete_file(path: str) -> bool:
    """Delete a file if it exists. Returns True if removed."""
    try:
        if os.path.exists(path):
            os.remove(path)
            return True
    except Exception as exc:
        print(f"[WARN] Failed to delete {path}: {exc}")
    return False


def clear_local_sqlite(env: dict) -> int:
    """Remove the local SQLite DB file and its WAL/SHM sidecars."""
    db_path = env.get("COFFEE_DB") or DB_PATH
    removed = 0
    for p in (db_path, db_path + "-wal", db_path + "-shm"):
        if _delete_file(p):
            removed += 1
    return removed


def clear_firestore_collection(db, env: dict, collection: Optional[str] = None, batch_size: int = 400) -> int:
    """
    Delete all documents in the given Firestore collection in batches.
    Returns the total number of documents deleted.
    """
    col = collection or env.get("FIREBASE_COLLECTION", "coffees")
    total = 0
    while True:
        docs = list(db.collection(col).limit(batch_size).stream())
        if not docs:
            break
        batch = db.batch()
        for d in docs:
            batch.delete(d.reference)
        batch.commit()
        total += len(docs)
        time.sleep(0.1)
    return total


def clear_datastores(force: bool = False) -> None:
    """
    Danger: Purge Firestore collection and remove local SQLite database.
    Requires FIREBASE_PROJECT_ID (and optionally FIREBASE_COLLECTION) to be set.
    """
    env = load_env()
    fdb = firestore_client(env)
    col = env.get("FIREBASE_COLLECTION", "coffees")
    tried_col = env.get("FIREBASE_TRIED_COLLECTION", TRIED_DEFAULT_COLLECTION)
    if col == tried_col:
        print("[ERROR] Refusing to clear tried collection with 'clear'. Use 'clear-tried' instead.")
        return
    db_path = env.get("COFFEE_DB") or DB_PATH
    if not force:
        prompt = (
            "This will DELETE all Firestore docs in "
            f"'{col}' and remove the local DB '{db_path}'.\n"
            "Type 'yes' to proceed: "
        )
        try:
            confirm = input(prompt)
        except EOFError:
            confirm = ""
        if str(confirm).strip().lower() != "yes":
            print("[INFO] Clear aborted.")
            return
    deleted = 0
    try:
        deleted = clear_firestore_collection(fdb, env, col)
    except Exception as exc:
        print(f"[ERROR] Firestore clear failed: {exc}")
    removed = clear_local_sqlite(env)
    print(f"[INFO] Firestore documents deleted: {deleted}")
    print(f"[INFO] Local DB files removed: {removed} (main/WAL/SHM)")


# -----------------------------------------------------------------------------
# Tried collection management

def mark_tried(id: Optional[str] = None, url: Optional[str] = None, notes: str = "", rating: Optional[int] = None) -> None:
    """Mark a coffee as tried by adding/updating a doc in the tried collection and setting SQLite flag."""
    env = load_env()
    fdb = firestore_client(env)
    conn = db_connect()
    main_col = env.get("FIREBASE_COLLECTION", "coffees")
    tried_col = env.get("FIREBASE_TRIED_COLLECTION", TRIED_DEFAULT_COLLECTION)
    if not id and not url:
        raise ValueError("Provide --id or --url")
    # Derive missing pieces from main collection
    if url and not id:
        id = product_id_from_url(url)
    roaster = ""
    title = ""
    if id and not url:
        try:
            d = fdb.collection(main_col).document(id).get()
            if d.exists:
                data = d.to_dict() or {}
                url = str(data.get("url", ""))
                roaster = str(data.get("roaster", ""))
                title = str(data.get("title", ""))
        except Exception:
            pass
    if id:
        try:
            d = fdb.collection(main_col).document(id).get()
            if d.exists and not title:
                data = d.to_dict() or {}
                roaster = roaster or str(data.get("roaster", ""))
                title = title or str(data.get("title", ""))
                url = url or str(data.get("url", ""))
        except Exception:
            pass
    if not id:
        raise ValueError("Unable to determine doc id from URL")
    now = datetime.now(timezone.utc).strftime(ISO)
    payload = {
        "doc_id": id,
        "url": url or "",
        "roaster": roaster,
        "title": title,
        "last_tried_on": now,
    }
    if notes:
        payload["last_notes"] = notes
    if rating is not None:
        try:
            payload["last_rating"] = int(rating)
        except Exception:
            pass
    ref = fdb.collection(tried_col).document(id)
    ref.set(payload, merge=True)
    # Append to history array
    try:
        history_item = {"tried_on": now}
        if notes:
            history_item["notes"] = notes
        if rating is not None:
            history_item["rating"] = int(rating)
        ref.update({"history": firestore.ArrayUnion([history_item])})
    except Exception:
        # If update fails (e.g., new doc), it's okay—the main fields are already set
        pass
    # Update SQLite tried flag if the row exists
    if url:
        cur = conn.cursor()
        cur.execute("UPDATE coffees SET tried=1 WHERE url=?", (url,))
        conn.commit()
    print(f"[INFO] Marked tried: id={id} url={url}")


def unmark_tried(id: Optional[str] = None, url: Optional[str] = None) -> None:
    """Remove tried mark by deleting the tried collection doc and clearing SQLite flag."""
    env = load_env()
    fdb = firestore_client(env)
    conn = db_connect()
    main_col = env.get("FIREBASE_COLLECTION", "coffees")
    tried_col = env.get("FIREBASE_TRIED_COLLECTION", TRIED_DEFAULT_COLLECTION)
    if not id and not url:
        raise ValueError("Provide --id or --url")
    if url and not id:
        id = product_id_from_url(url)
    if id and not url:
        try:
            d = fdb.collection(main_col).document(id).get()
            if d.exists:
                data = d.to_dict() or {}
                url = str(data.get("url", ""))
        except Exception:
            pass
    if not id:
        raise ValueError("Unable to determine doc id from URL")
    try:
        fdb.collection(tried_col).document(id).delete()
    except Exception as exc:
        print(f"[WARN] Failed to delete tried doc {id}: {exc}")
    if url:
        cur = conn.cursor()
        cur.execute("UPDATE coffees SET tried=0 WHERE url=?", (url,))
        conn.commit()
    print(f"[INFO] Unmarked tried: id={id} url={url}")


def list_tried(limit: int = 50) -> None:
    """List tried coffees with their last tried timestamp."""
    env = load_env()
    fdb = firestore_client(env)
    tried_col = env.get("FIREBASE_TRIED_COLLECTION", TRIED_DEFAULT_COLLECTION)
    docs = list(fdb.collection(tried_col).stream())
    items = []
    for d in docs:
        data = d.to_dict() or {}
        items.append(
            (
                str(data.get("last_tried_on", "")),
                d.id,
                str(data.get("roaster", "")),
                str(data.get("title", "")),
                str(data.get("url", "")),
            )
        )
    # Sort by timestamp desc
    items.sort(key=lambda t: t[0], reverse=True)
    for row in items[: max(0, int(limit))]:
        ts, pid, roaster, title, url = row
        print(f"{ts} | {roaster} | {title} | {url} | id={pid}")


def clear_tried(force: bool = False) -> None:
    """Clear the tried collection only (protected from regular 'clear')."""
    env = load_env()
    fdb = firestore_client(env)
    tried_col = env.get("FIREBASE_TRIED_COLLECTION", TRIED_DEFAULT_COLLECTION)
    if not force:
        prompt = (
            f"This will DELETE all Firestore docs in '{tried_col}' (tried coffees history).\n"
            "Type 'yes' to proceed: "
        )
        try:
            confirm = input(prompt)
        except EOFError:
            confirm = ""
        if str(confirm).strip().lower() != "yes":
            print("[INFO] Clear-tried aborted.")
            return
    deleted = clear_firestore_collection(fdb, env, tried_col)
    print(f"[INFO] Tried documents deleted: {deleted}")

def clear_sqlite_only(force: bool = False) -> None:
    """Remove only the local SQLite database (with confirmation unless forced)."""
    env = load_env()
    db_path = env.get("COFFEE_DB") or DB_PATH
    if not force:
        prompt = (
            f"This will remove the local DB '{db_path}'.\n"
            "Type 'yes' to proceed: "
        )
        try:
            confirm = input(prompt)
        except EOFError:
            confirm = ""
        if str(confirm).strip().lower() != "yes":
            print("[INFO] Clear-db aborted.")
            return
    removed = clear_local_sqlite(env)
    print(f"[INFO] Local DB files removed: {removed} (main/WAL/SHM)")

# Persistence & orchestration

def ensure_row_sqlite(conn: sqlite3.Connection, p: Product, first_seen: str, last_seen: str) -> None:
    """Insert or update a product row in SQLite to mirror Firestore.

    The table uses 'price' (not 'price_text'); for compatibility we also update
    legacy 'price_text' if it exists.
    """
    cur = conn.cursor()
    # Check if legacy column exists
    cols = {row[1] for row in cur.execute("PRAGMA table_info('coffees')").fetchall()}
    has_legacy_price_text = "price_text" in cols
    row = cur.execute("SELECT id FROM coffees WHERE url=?", (p.url,)).fetchone()
    if row is None:
        cur.execute(
            "INSERT INTO coffees (roaster,title,url,price,in_stock,first_seen,last_seen,producer,country,region,process,variety,notes,profile,image) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                p.roaster,
                p.title,
                p.url,
                p.price_text,
                int(p.in_stock),
                first_seen,
                last_seen,
                p.producer,
                p.country,
                p.region,
                p.process,
                p.variety,
                p.notes,
                p.profile,
                p.image,
            ),
        )
        if has_legacy_price_text:
            cur.execute("UPDATE coffees SET price_text = ? WHERE url = ?", (p.price_text, p.url))
    else:
        cur.execute(
            "UPDATE coffees SET title=?, price=?, in_stock=?, last_seen=?, producer=?, country=?, region=?, process=?, variety=?, notes=?, profile=?, image=? WHERE url=?",
            (
                p.title,
                p.price_text,
                int(p.in_stock),
                last_seen,
                p.producer,
                p.country,
                p.region,
                p.process,
                p.variety,
                p.notes,
                p.profile,
                p.image,
                p.url,
            ),
        )
        # Preserve first_seen value; if row existed, do not overwrite
        if has_legacy_price_text:
            cur.execute("UPDATE coffees SET price_text = ? WHERE url = ?", (p.price_text, p.url))
    conn.commit()


def mark_stale_sqlite(conn: sqlite3.Connection, seen_by_roaster: Dict[str, Set[str]]) -> int:
    """Mark rows as out-of-stock for roasters processed this run where URL not seen."""
    total = 0
    cur = conn.cursor()
    for roaster, urls in seen_by_roaster.items():
        if urls:
            placeholders = ",".join(["?"] * len(urls))
            params = [roaster] + list(urls)
            cur.execute(
                f"UPDATE coffees SET in_stock=0 WHERE roaster=? AND url NOT IN ({placeholders})",
                params,
            )
        else:
            # If no items seen for this roaster (but run succeeded), mark all as out-of-stock
            cur.execute("UPDATE coffees SET in_stock=0 WHERE roaster=?", (roaster,))
        total += cur.rowcount if cur.rowcount is not None else 0
    conn.commit()
    return total


def ensure_row_sqlite_from_doc(conn: sqlite3.Connection, data: Dict[str, object]) -> None:
    """Upsert a SQLite row using a Firestore document dict."""
    cur = conn.cursor()
    cols = {row[1] for row in cur.execute("PRAGMA table_info('coffees')").fetchall()}
    has_legacy_price_text = "price_text" in cols
    roaster = str(data.get("roaster", ""))
    title = str(data.get("title", ""))
    url = str(data.get("url", ""))
    price = str(data.get("price", ""))
    in_stock = 1 if bool(data.get("in_stock", False)) else 0
    first_seen = str(data.get("first_seen", ""))
    last_seen = str(data.get("last_seen", ""))
    producer = str(data.get("producer", ""))
    country = str(data.get("country", ""))
    region = str(data.get("region", ""))
    process = str(data.get("process", ""))
    variety = str(data.get("variety", ""))
    notes = str(data.get("notes", ""))
    profile = str(data.get("profile", ""))
    row = cur.execute("SELECT id FROM coffees WHERE url=?", (url,)).fetchone()
    if row is None:
        cur.execute(
            "INSERT INTO coffees (roaster,title,url,price,in_stock,first_seen,last_seen,producer,country,region,process,variety,notes,profile) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                roaster, title, url, price, in_stock, first_seen, last_seen, producer, country, region, process, variety, notes, profile,
            ),
        )
        if has_legacy_price_text:
            cur.execute("UPDATE coffees SET price_text = ? WHERE url = ?", (price, url))
    else:
        cur.execute(
            "UPDATE coffees SET title=?, price=?, in_stock=?, first_seen=?, last_seen=?, producer=?, country=?, region=?, process=?, variety=?, notes=?, profile=? WHERE url=?",
            (
                title, price, in_stock, first_seen, last_seen, producer, country, region, process, variety, notes, profile, url,
            ),
        )
        if has_legacy_price_text:
            cur.execute("UPDATE coffees SET price_text = ? WHERE url = ?", (price, url))


def sync_sqlite_from_firestore() -> Dict[str, int]:
    """One-way sync: read Firestore and mirror into SQLite, then mark local stale."""
    env = load_env()
    fdb = firestore_client(env)
    conn = db_connect()
    col = env.get("FIREBASE_COLLECTION", "coffees")
    inserted = 0
    updated = 0
    docs = list(fdb.collection(col).stream())
    urls_in_stock_by_roaster: Dict[str, Set[str]] = defaultdict(set)
    cur = conn.cursor()
    for d in docs:
        data = d.to_dict() or {}
        before = cur.execute("SELECT title, price, in_stock, last_seen FROM coffees WHERE url=?", (data.get("url", ""),)).fetchone()
        ensure_row_sqlite_from_doc(conn, data)
        after = cur.execute("SELECT title, price, in_stock, last_seen FROM coffees WHERE url=?", (data.get("url", ""),)).fetchone()
        if before is None:
            inserted += 1
        elif after != before:
            updated += 1
        if data.get("in_stock") and data.get("url") and data.get("roaster"):
            urls_in_stock_by_roaster[str(data.get("roaster"))].add(str(data.get("url")))
    # Mark as out-of-stock anything not in Firestore's current in-stock set for each roaster
    stale_updates = mark_stale_sqlite(conn, urls_in_stock_by_roaster)
    # Sync 'tried' flags from tried collection into SQLite
    tried_marked = 0
    try:
        tried_col = env.get("FIREBASE_TRIED_COLLECTION", TRIED_DEFAULT_COLLECTION)
        tdocs = list(fdb.collection(tried_col).stream())
        # Reset all tried flags, then set those present in tried collection
        cur.execute("UPDATE coffees SET tried=0")
        for td in tdocs:
            tdata = td.to_dict() or {}
            turl = str(tdata.get("url", ""))
            if turl:
                cur.execute("UPDATE coffees SET tried=1 WHERE url=?", (turl,))
                tried_marked += cur.rowcount if cur.rowcount is not None else 0
        conn.commit()
    except Exception as exc:
        print(f"[WARN] Tried sync failed: {exc}")
    return {"inserted": inserted, "updated": updated, "sqlite_stale_updates": stale_updates, "tried_marked": tried_marked}


def run_once(roasters: Iterable[RoasterConfig]) -> int:
    """Fetch roasters, persist to Firestore first, mirror to SQLite, mark stale, and notify."""
    env = load_env()
    fdb = firestore_client(env)
    conn = db_connect()
    existing = load_firestore_index(fdb, env)
    new_items: List[Tuple[Product, str]] = []
    seen_ids_by_roaster: Dict[str, Set[str]] = defaultdict(set)
    seen_urls_by_roaster: Dict[str, Set[str]] = defaultdict(set)
    succeeded_roasters: Set[str] = set()
    for cfg in roasters:
        print(f"[INFO] Fetching {cfg.name} … {cfg.start_url}")
        try:
            html = fetch_html(cfg.start_url)
            items = parse_products(cfg, html)
            print(f"[INFO] Parsed {len(items)} products from {cfg.name}")
            succeeded_roasters.add(cfg.name)
        except Exception as exc:
            print(f"[ERROR] {cfg.name}: {exc}")
            continue
        for p in items:
            # Enrich product details
            details = scrape_product_details(p.url, cfg.name)
            p.producer = details.get("producer", "")
            p.country = details.get("country", "")
            p.region = details.get("region", "")
            p.process = details.get("process", "")
            p.variety = details.get("variety", "")
            p.notes = details.get("notes", "")
            p.profile = details.get("profile", "")
            p.image = details.get("image", p.image)
            try:
                now = datetime.now(timezone.utc).strftime(ISO)
                pid = p.id()
                is_new = pid not in existing
                first = now if is_new else (existing.get(pid) or now)
                last = now
                doc_id = upsert_firestore(fdb, env, p, first, last)
                # Mirror to SQLite only on Firestore success
                ensure_row_sqlite(conn, p, first, last)
                if is_new:
                    new_items.append((p, doc_id))
                seen_ids_by_roaster[p.roaster].add(pid)
                seen_urls_by_roaster[p.roaster].add(p.url)
            except Exception as exc:
                print(f"[WARN] persist failed for {p.url}: {exc}")
                continue
        time.sleep(cfg.sleep_between())
    # Mark stale (only for roasters that were successfully processed)
    if succeeded_roasters:
        # Filter seen maps to only include succeeded roasters
        seen_ids_filtered = {r: ids for r, ids in seen_ids_by_roaster.items() if r in succeeded_roasters}
        try:
            updated = mark_stale_by_roaster(fdb, env, seen_ids_filtered)
            print(f"[INFO] Firestore stale updates: {updated}")
        except Exception as exc:
            print(f"[WARN] Firestore staleness pass failed: {exc}")
        seen_urls_filtered = {r: urls for r, urls in seen_urls_by_roaster.items() if r in succeeded_roasters}
        try:
            updated = mark_stale_sqlite(conn, seen_urls_filtered)
            print(f"[INFO] SQLite stale updates: {updated}")
        except Exception as exc:
            print(f"[WARN] SQLite staleness pass failed: {exc}")
    if new_items:
        title, body_lines, first_id = build_ntfy_message(new_items)
        id_list = [pid for (_p, pid) in new_items]
        notify_ntfy(body_lines, env, deep_id=first_id, deep_ids=id_list, title=title)
        print(title)
        print("\n".join(body_lines))
    else:
        print("[INFO] No new items this run.")
    return len(new_items)


def simulate_new_drop(count: int = 1, roasters: Optional[List[str]] = None) -> int:
    """
    Create N fake coffees across one or more roasters and send a single ntfy push.

    :param count: total number of coffees to simulate
    :param roasters: list of roaster names to use; defaults to generic names
    :return: number of new items simulated
    """
    env = load_env()
    fdb = firestore_client(env)
    conn = db_connect()
    existing = load_firestore_index(fdb, env)
    if not roasters:
        roasters = [
            "Simulation Roaster A",
            "Simulation Roaster B",
            "Simulation Roaster C",
        ]
    ts = int(time.time())
    new_items: List[Tuple[Product, str]] = []
    for i in range(count):
        r = roasters[i % len(roasters)]
        fake = Product(
            roaster=r,
            title=f"Test Drop {ts}-{i+1}",
            url=f"https://example.com/products/test-{ts}-{i+1}",
            price_text=f"${18 + (i % 7)}.00",
            in_stock=True,
            country=["Ethiopia", "Kenya", "Colombia", "Guatemala", "Testland"][i % 5],
            profile=["fruity", "floral", "chocolate", "nutty", "spice"][i % 5],
            notes=["berry", "citrus", "caramel", "stone fruit", "tropical"][i % 5],
        )
        now = datetime.now(timezone.utc).strftime(ISO)
        pid = fake.id()
        is_new = pid not in existing
        first = now if is_new else (existing.get(pid) or now)
        last = now
        doc_id = upsert_firestore(fdb, env, fake, first, last)
        ensure_row_sqlite(conn, fake, first, last)
        if is_new:
            new_items.append((fake, doc_id))
    if not new_items:
        print(
            "[INFO] Simulation created items that were already present (no 'new'). Try increasing count or tweak URLs."
        )
        return 0
    title, body_lines, first_id = build_ntfy_message(new_items)
    id_list = [pid for (_p, pid) in new_items]
    notify_ntfy(body_lines, env, deep_id=first_id, deep_ids=id_list, title=title)
    print(title)
    print("\n".join(body_lines))
    return len(new_items)


# -----------------------------------------------------------------------------
# CLI entry point

def main() -> None:
    parser = argparse.ArgumentParser(description="Monitor coffee roasters for new drops and extract metadata.")
    parser.add_argument(
        "--dev",
        action="store_true",
        help="Use firebase.json in this script's directory for credentials (overrides GOOGLE_APPLICATION_CREDENTIALS).",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    run_parser = subparsers.add_parser("run", help="Run the scraper once for all configured roasters.")
    run_parser.add_argument("--collection", type=str, default=None, help="Firestore collection to use (overrides env).")
    run_parser.add_argument("--db", type=str, default=None, help="Path to SQLite DB file (overrides env).")
    run_parser.add_argument("--dev", action="store_true", help=DEV_HELP)
    sim_parser = subparsers.add_parser("simulate", help="Simulate new coffee drops.")
    sim_parser.add_argument(
        "--count", type=int, default=1, help="Number of coffees to simulate (total)."
    )
    sim_parser.add_argument(
        "--roasters",
        type=str,
        default="",
        help="Comma-separated roaster names (e.g., 'Black & White Coffee Roasters,SEY Coffee'); default uses simulation roasters.",
    )
    sim_parser.add_argument("--collection", type=str, default=None, help="Firestore collection to use (overrides env).")
    sim_parser.add_argument("--db", type=str, default=None, help="Path to SQLite DB file (overrides env).")
    sim_parser.add_argument("--dev", action="store_true", help=DEV_HELP)
    clr_parser = subparsers.add_parser("clear", help="Delete local SQLite DB file and purge Firestore collection.", aliases=["reset"])
    clr_parser.add_argument("--force", "-f", action="store_true", help="Skip confirmation prompt.")
    clr_parser.add_argument("--collection", type=str, default=None, help="Firestore collection to purge (overrides env).")
    clr_parser.add_argument("--db", type=str, default=None, help="Path to SQLite DB file to remove (overrides env).")
    clr_parser.add_argument("--dev", action="store_true", help=DEV_HELP)
    sync_parser = subparsers.add_parser("sync", help="Sync SQLite cache from Firestore (one-way).")
    sync_parser.add_argument("--collection", type=str, default=None, help="Firestore collection to use (overrides env).")
    sync_parser.add_argument("--db", type=str, default=None, help="Path to SQLite DB file (overrides env).")
    sync_parser.add_argument("--dev", action="store_true", help=DEV_HELP)
    clrdb_parser = subparsers.add_parser("clear-db", help="Delete local SQLite DB file only.")
    clrdb_parser.add_argument("--force", "-f", action="store_true", help="Skip confirmation prompt.")
    clrdb_parser.add_argument("--db", type=str, default=None, help="Path to SQLite DB file to remove (overrides env).")
    clrdb_parser.add_argument("--dev", action="store_true", help=DEV_HELP)
    # Tried collection commands
    tried_add = subparsers.add_parser("mark-tried", help="Mark a coffee as tried.")
    tried_add.add_argument("--id", type=str, default=None, help="Product doc id (hash).")
    tried_add.add_argument("--url", type=str, default=None, help="Product URL (used to derive id).")
    tried_add.add_argument("--notes", type=str, default="", help="Optional notes for this try event.")
    tried_add.add_argument("--rating", type=int, default=None, help="Optional rating integer.")
    tried_add.add_argument("--dev", action="store_true", help=DEV_HELP)

    tried_remove = subparsers.add_parser("unmark-tried", help="Remove tried mark for a coffee.")
    tried_remove.add_argument("--id", type=str, default=None, help="Product doc id (hash).")
    tried_remove.add_argument("--url", type=str, default=None, help="Product URL (used to derive id).")
    tried_remove.add_argument("--dev", action="store_true", help=DEV_HELP)

    tried_list = subparsers.add_parser("list-tried", help="List tried coffees.")
    tried_list.add_argument("--limit", type=int, default=50, help="Max rows to display.")
    tried_list.add_argument("--dev", action="store_true", help=DEV_HELP)

    tried_clear = subparsers.add_parser("clear-tried", help="Delete all tried records (protected command).")
    tried_clear.add_argument("--force", "-f", action="store_true", help="Skip confirmation prompt.")
    tried_clear.add_argument("--dev", action="store_true", help=DEV_HELP)
    args = parser.parse_args()
    # Apply dev credentials override early so downstream env sees it
    if getattr(args, "dev", False):
        local_key = os.path.join(BASE_DIR, "firebase.json")
        if os.path.exists(local_key):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = local_key
            # Try to infer project id if not already set
            if not os.environ.get("FIREBASE_PROJECT_ID"):
                try:
                    import json
                    with open(local_key, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    proj = data.get("project_id") or data.get("quota_project_id")
                    if proj:
                        os.environ["FIREBASE_PROJECT_ID"] = proj
                except Exception as exc:
                    print(f"[WARN] --dev: unable to read project_id from firebase.json: {exc}")
        else:
            print(f"[WARN] --dev enabled but no firebase.json found at {local_key}")
    # Apply overrides to process environment before executing commands
    if getattr(args, "collection", None):
        os.environ["FIREBASE_COLLECTION"] = args.collection  # used by load_env
    if getattr(args, "db", None):
        os.environ["COFFEE_DB"] = args.db
    if args.command == "run":
        run_once(ROASTERS)
    elif args.command == "simulate":
        roaster_list = [x.strip() for x in args.roasters.split(",") if x.strip()] or None
        simulate_new_drop(count=args.count, roasters=roaster_list)
    elif args.command == "clear":
        clear_datastores(force=args.force)
    elif args.command == "clear-db":
        clear_sqlite_only(force=args.force)
    elif args.command == "sync":
        res = sync_sqlite_from_firestore()
        print(
            f"[INFO] Sync complete. Inserted: {res['inserted']}, Updated: {res['updated']}, SQLite stale updates: {res['sqlite_stale_updates']}, Tried marked: {res.get('tried_marked', 0)}"
        )
    elif args.command == "mark-tried":
        mark_tried(id=getattr(args, "id", None), url=getattr(args, "url", None), notes=getattr(args, "notes", ""), rating=getattr(args, "rating", None))
    elif args.command == "unmark-tried":
        unmark_tried(id=getattr(args, "id", None), url=getattr(args, "url", None))
    elif args.command == "list-tried":
        list_tried(limit=getattr(args, "limit", 50))
    elif args.command == "clear-tried":
        clear_tried(force=args.force)


if __name__ == "__main__":
    main()