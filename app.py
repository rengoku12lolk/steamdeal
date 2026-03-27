"""
SteamDeal Flask backend
Clean + production-ready version for Railway
"""

import json
import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS
from apscheduler.schedulers.background import BackgroundScheduler

# ✅ correct imports
from steam_parser import SteamParser
from analytics import (
    enrich_games,
    rank_publishers,
    find_notable_promotions,
    generate_summary,
)

# ── setup ─────────────────────────────────────────────

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("steamdeal")

app = Flask(__name__, static_folder="static", template_folder="templates")
CORS(app)

CACHE_FILE = Path("cache/games.json")
CACHE_FILE.parent.mkdir(exist_ok=True)

_cache = {}
_cache_lock = threading.Lock()
_last_updated = "never"

# ── data fetching ─────────────────────────────────────

def fetch_and_cache():
    global _last_updated
    log.info("Fetching Steam data...")

    try:
        parser = SteamParser(country="BE")

        games = []
        games.extend(parser.get_featured_games())
        games.extend(parser.get_top_sellers_steamspy(limit=80))
        games.extend(parser.get_specials_steamspy())

        # remove duplicates
        seen = set()
        unique = []
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
            _cache.clear()
            _cache.update(payload)

        CACHE_FILE.write_text(json.dumps(payload, indent=2))
        _last_updated = datetime.now(timezone.utc).strftime("%H:%M UTC")

        log.info("Cache updated: %d games", len(unique))

    except Exception as e:
        log.error(f"Fetch failed: {e}")

# ── startup cache ─────────────────────────────────────

def load_cache():
    if CACHE_FILE.exists():
        try:
            with _cache_lock:
                _cache.update(json.loads(CACHE_FILE.read_text()))
            log.info("Loaded cache from disk")
        except Exception as e:
            log.warning(f"Cache load failed: {e}")

# ── routes ────────────────────────────────────────────

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
        return jsonify(_cache.get("promotions", []))

# ✅ IMPORTANT: Railway healthcheck
@app.route("/api/status")
def api_status():
    return {"status": "ok"}

@app.route("/static/<path:filename>")
def static_files(filename):
    return send_from_directory("static", filename)

# ── scheduler ─────────────────────────────────────────

def start_scheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(fetch_and_cache, "interval", minutes=10)
    scheduler.start()
    log.info("Scheduler started")

# ── run ───────────────────────────────────────────────

if __name__ == "__main__":
    load_cache()

    # run fetch in background
    threading.Thread(target=fetch_and_cache, daemon=True).start()

    start_scheduler()

    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
