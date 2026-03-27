"""
app.py — SteamDeal Flask backend
Serves game data as a JSON API and refreshes every 10 minutes via APScheduler.
"""

import json
import logging
import os
import time
import threading
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS
from apscheduler.schedulers.background import BackgroundScheduler

# ── import our existing modules ───────────────────────────────────────────────
from steam_parser import SteamParser
from analytics import enrich_games, rank_publishers, find_notable_promotions, generate_summary

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
log = logging.getLogger("steamdeal")

app = Flask(__name__, static_folder="static", template_folder="templates")
CORS(app)

CACHE_FILE = Path("cache/games.json")
CACHE_FILE.parent.mkdir(exist_ok=True)

_cache: dict = {}
_cache_lock = threading.Lock()
_last_updated: str = "never"


# ── data fetching ─────────────────────────────────────────────────────────────

def fetch_and_cache():
    global _last_updated
    log.info("Fetching fresh Steam data …")
    try:
        parser = SteamParser(country="BE")   # Belgium pricing

        games = []
        featured = parser.get_featured_games()
        log.info("  featured: %d", len(featured))
        games.extend(featured)

        top = parser.get_top_sellers_steamspy(limit=80)
        log.info("  top sellers: %d", len(top))
        games.extend(top)

        specials = parser.get_specials_steamspy()
        log.info("  specials: %d", len(specials))
        games.extend(specials)

        # de-duplicate
        seen, unique = set(), []
        for g in games:
            aid = g.get("app_id")
            if aid and aid not in seen:
                seen.add(aid)
                unique.append(g)

        unique = enrich_games(unique)
        publishers = rank_publishers(unique)
        promotions = find_notable_promotions(unique)
        summary = generate_summary(unique, publishers)

        payload = {
            "games": unique,
            "publishers": publishers[:20],
            "promotions": promotions,
            "summary": summary,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }

        with _cache_lock:
            _cache.update(payload)

        CACHE_FILE.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
        _last_updated = datetime.now(timezone.utc).strftime("%H:%M UTC")
        log.info("Cache updated — %d games", len(unique))

    except Exception as e:
        log.error("Fetch failed: %s", e)
        # try loading from disk cache as fallback
        if CACHE_FILE.exists() and not _cache:
            log.info("Loading from disk cache …")
            with _cache_lock:
                _cache.update(json.loads(CACHE_FILE.read_text()))


def load_disk_cache():
    """Load disk cache on startup so the API responds instantly."""
    if CACHE_FILE.exists():
        try:
            with _cache_lock:
                _cache.update(json.loads(CACHE_FILE.read_text()))
            log.info("Loaded disk cache (%d games)", len(_cache.get("games", [])))
        except Exception as e:
            log.warning("Could not load disk cache: %s", e)


# ── routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory("templates", "index.html")

@app.route("/api/games")
def api_games():
    with _cache_lock:
        return jsonify({
            "games": _cache.get("games", []),
            "summary": _cache.get("summary", {}),
            "fetched_at": _cache.get("fetched_at", ""),
            "last_updated": _last_updated,
        })

@app.route("/api/publishers")
def api_publishers():
    with _cache_lock:
        return jsonify(_cache.get("publishers", []))

@app.route("/api/promotions")
def api_promotions():
    with _cache_lock:
        return jsonify(_cache.get("promotions", {}))

@app.route("/api/status")
def api_status():
    with _cache_lock:
        return jsonify({
            "game_count": len(_cache.get("games", [])),
            "last_updated": _last_updated,
            "fetched_at": _cache.get("fetched_at", ""),
        })

@app.route("/static/<path:filename>")
def static_files(filename):
    return send_from_directory("static", filename)


# ── scheduler ─────────────────────────────────────────────────────────────────

def start_scheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(fetch_and_cache, "interval", minutes=10, id="refresh")
    scheduler.start()
    log.info("Scheduler started — refreshing every 10 minutes")


# ── main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    load_disk_cache()
    # First fetch in background so server starts instantly
    t = threading.Thread(target=fetch_and_cache, daemon=True)
    t.start()
    start_scheduler()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
