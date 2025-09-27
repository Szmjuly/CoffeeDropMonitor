# CoffeeDropMonitor

Dual-database monitor for specialty coffee drops. Scrapes multiple roasters, enriches product pages with tasting notes and metadata, and persists to Firestore (canonical) with a local SQLite cache for speed and resilience.

## Architecture
- Firestore is the source of truth for the frontend.
- SQLite mirrors Firestore for fast local checks (e.g., new-item detection) and offline resilience.
- Writes are Firestore-first; on success, SQLite is updated to match.
- Staleness pass marks items not seen in a run as out-of-stock, scoped per roaster.

## Requirements
- Python 3.9+
- Install dependencies:

```bash
pip install -r requirements.txt
```

## Environment
Create a `.env` (or export variables):

- `GOOGLE_APPLICATION_CREDENTIALS` — path to the Firebase service-account JSON
- `FIREBASE_PROJECT_ID` — Firebase project ID
- `FIREBASE_COLLECTION` — Firestore collection name (default `coffees`)
- `FIREBASE_TRIED_COLLECTION` — Firestore tried coffees collection name (default `coffees_tried`)
- `COFFEE_DB` — path to SQLite DB (default `<repo>/coffee_firestore.db`)
- `NTFY_TOPIC` — ntfy topic for notifications
- `NTFY_SERVER` — ntfy server base (default `https://ntfy.sh`)
- `SITE_BASE_URL` — base URL for deep-link clicks
- `NTFY_CLICK_URL` — optional fallback click URL

See `.env.example` for a template.

## CLI

```bash
python coffee_monitor.py --help
```

Commands:
- `run` — scrape all configured roasters once
- `simulate [--count N] [--roasters "A,B"]` — generate test drops into Firestore (and mirror to SQLite)
- `sync` — one-way reconcile: mirror Firestore -> SQLite, then mark local stale
- `clear` (alias: `reset`) — purge Firestore collection and remove local SQLite
- `clear-db` — remove only the local SQLite DB files
- `mark-tried` — mark a coffee as tried (accepts `--id` or `--url`, optional `--notes`, `--rating`)
- `unmark-tried` — remove tried mark for a coffee
- `list-tried` — list tried coffees
- `clear-tried` — delete all tried records (protected; requires confirmation or `-f`)

### CLI Overrides
All commands accept overrides for collection and/or local DB path:

- `--collection <name>` — override `FIREBASE_COLLECTION` for the command
- `--db <path>` — override `COFFEE_DB` for the command

Examples:

```bash
python coffee_monitor.py run --collection coffees
python coffee_monitor.py sync --db d:/data/coffee_cache.db
python coffee_monitor.py clear --collection coffees --db ./coffee_firestore.db -f
```

### Dev Mode
Use a local service account key without touching your global env:

- Pass `--dev` to any command to instruct the app to use `firebase.json` located in the repository directory (same folder as `coffee_monitor.py`).
- If `FIREBASE_PROJECT_ID` is not set, the project ID is inferred from that `firebase.json`.

Examples:

```bash
python coffee_monitor.py run --dev
python coffee_monitor.py sync --collection coffees --dev
python coffee_monitor.py clear --collection coffees --db ./coffee_firestore.db -f --dev
```

## Tried Coffees (Protected)
- Separate Firestore collection for tried coffees, set via `FIREBASE_TRIED_COLLECTION` (default `coffees_tried`).
- Protected from accidental deletion: `clear` refuses to target the tried collection. Use `clear-tried` explicitly.
- `sync` also mirrors tried flags from the tried collection into SQLite (sets `coffees.tried`).

Examples:

```bash
# Mark a coffee as tried by doc id with notes/rating
python coffee_monitor.py mark-tried --id <doc_id> --notes "good bloom" --rating 5

# Or by URL (doc id derived from URL)
python coffee_monitor.py mark-tried --url https://roaster.com/products/abc

# Remove tried mark
python coffee_monitor.py unmark-tried --id <doc_id>

# List tried coffees
python coffee_monitor.py list-tried --limit 50

# Clear only tried collection (requires confirmation or -f)
python coffee_monitor.py clear-tried -f
```

## Operation Order (run)
1) Fetch each roaster listing
2) Parse items and enrich by scraping each product page for details (notes, producer, country, region, process, variety, profile)
3) Upsert to Firestore (canonical) with `first_seen`/`last_seen`
4) On success, upsert to SQLite mirror
5) Track new items for notification
6) Per-roaster staleness: mark not-seen items as `in_stock=false` in Firestore, then SQLite
7) Send ntfy notification (single or grouped)

## Staleness Rules
- Only roasters that successfully parsed in the current run are considered for staleness.
- Firestore updates use `mark_stale_by_roaster`; SQLite uses `mark_stale_sqlite`.

## Sync Semantics
- `sync` mirrors Firestore -> SQLite and then marks local rows not present as in-stock in Firestore as `in_stock=false`.
- Also mirrors `tried` flags from the tried collection into SQLite (sets the `tried` column).
- Firestore remains canonical; bi-directional reconciliation is intentionally avoided to reduce divergence.

## Schema Parity
SQLite table `coffees` mirrors Firestore fields:
- `roaster, title, url (unique), price, in_stock, first_seen, last_seen`
- `producer, country, region, process, variety, notes, profile`

- Plus a local `tried` flag (`INTEGER` 0/1) mirrored from the tried collection for convenience.

A lightweight migration ensures missing columns are added and legacy `price_text` is respected if present.

## Adding Roasters
- Add a `RoasterConfig` entry in `coffee_monitor.py` with selectors for list pages.
- Optionally add a detail parser in `DETAIL_PARSERS` to extract notes/metadata from product pages.

## Future-Proofing
- Designed to support additional collections (e.g., `purchased`, `tried`, `sub-coffees`).
- Use `FIREBASE_COLLECTION` to target a collection; consider extending the CLI with a `--collection` flag for multi-list workflows.
- If/when multi-collection support is added, replicate the Firestore-first + SQLite-mirror pipeline per collection.

## Frontend Notes
- Frontend reads Firestore directly. Grouping by roaster/country and a default ungrouped "All coffees" view are supported design patterns.
- Aim for mobile-friendly cards with consistent height and readable modals. Avoid overflow in long roaster names.

## Troubleshooting
- Ensure `GOOGLE_APPLICATION_CREDENTIALS` points to a valid service account JSON with Firestore permissions.
- `sync` requires Firestore access; it won’t modify Firestore, only local SQLite.
- Use `clear-db -f` to quickly reset the local cache without touching Firestore.
