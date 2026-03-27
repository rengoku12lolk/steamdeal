"""
steam_parser.py
---------------
Core Steam API integration module.
Fetches game data, deals, and publisher info via Steam's public APIs.
"""

import time
import logging
import requests
from typing import Optional
from datetime import datetime

logger = logging.getLogger(__name__)

STEAM_API_BASE = "https://store.steampowered.com/api"
STEAM_FEATURED = "https://store.steampowered.com/api/featured"
STEAM_FEATURED_CATS = "https://store.steampowered.com/api/featuredcategories"
STEAM_SEARCH = "https://store.steampowered.com/api/storesearch"
STEAM_SPECIALS = "https://store.steampowered.com/search/results"

# Public SteamSpy API (no key required, rate-limited)
STEAMSPY_API = "https://steamspy.com/api.php"


class RateLimiter:
    """Simple token bucket rate limiter."""

    def __init__(self, calls_per_second: float = 1.0):
        self.min_interval = 1.0 / calls_per_second
        self._last_call = 0.0

    def wait(self):
        elapsed = time.time() - self._last_call
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self._last_call = time.time()


class SteamParser:
    """
    Fetches and normalises data from Steam's public store APIs.
    All endpoints used are documented and publicly accessible —
    no scraping of HTML pages is performed.
    """

    def __init__(self, language: str = "english", country: str = "US",
                 rate_limit: float = 1.5):
        self.language = language
        self.country = country
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "SteamAnalyticsTool/1.0 (educational project)"
        })
        self.limiter = RateLimiter(calls_per_second=rate_limit)

    def _get(self, url: str, params: dict = None, retries: int = 3) -> Optional[dict]:
        """GET with retry + back-off."""
        params = params or {}
        for attempt in range(retries):
            self.limiter.wait()
            try:
                r = self.session.get(url, params=params, timeout=15)
                r.raise_for_status()
                return r.json()
            except requests.exceptions.HTTPError as e:
                if r.status_code == 429:
                    wait = 2 ** (attempt + 2)
                    logger.warning("Rate limited — waiting %ss", wait)
                    time.sleep(wait)
                else:
                    logger.error("HTTP error %s for %s", r.status_code, url)
                    return None
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout) as e:
                logger.warning("Network error (attempt %d): %s", attempt + 1, e)
                time.sleep(2 ** attempt)
            except ValueError:
                logger.error("JSON parse error for %s", url)
                return None
        return None

    # ------------------------------------------------------------------
    # Game detail
    # ------------------------------------------------------------------

    def get_app_details(self, app_id: int) -> Optional[dict]:
        """Return normalised game record for a single Steam app."""
        data = self._get(
            f"{STEAM_API_BASE}/appdetails",
            params={"appids": app_id, "cc": self.country, "l": self.language}
        )
        if not data or str(app_id) not in data:
            return None
        app = data[str(app_id)]
        if not app.get("success"):
            return None
        return self._normalise_app(app["data"])

    def _normalise_app(self, raw: dict) -> dict:
        """Map raw Steam API fields to a clean, consistent schema."""
        price_info = raw.get("price_overview", {})
        original_cents = price_info.get("initial", 0)
        current_cents = price_info.get("final", 0)
        discount_pct = price_info.get("discount_percent", 0)

        genres = [g["description"] for g in raw.get("genres", [])]
        categories = [c["description"] for c in raw.get("categories", [])]
        publishers = raw.get("publishers", [])
        developers = raw.get("developers", [])

        release = raw.get("release_date", {})
        release_date = release.get("date", "N/A")

        mc = raw.get("metacritic", {})

        return {
            "app_id": raw.get("steam_appid"),
            "title": raw.get("name", "Unknown"),
            "type": raw.get("type", "game"),
            "short_description": raw.get("short_description", ""),
            "current_price_usd": round(current_cents / 100, 2),
            "original_price_usd": round(original_cents / 100, 2),
            "discount_percent": discount_pct,
            "is_free": raw.get("is_free", False),
            "release_date": release_date,
            "developers": developers,
            "publishers": publishers,
            "genres": genres,
            "categories": categories,
            "tags": [],                          # populated via SteamSpy
            "review_score": None,                # populated separately
            "review_count": None,
            "review_summary": None,
            "metacritic_score": mc.get("score"),
            "platforms": [k for k, v in raw.get("platforms", {}).items() if v],
            "header_image": raw.get("header_image", ""),
            "store_url": f"https://store.steampowered.com/app/{raw.get('steam_appid')}",
            "fetched_at": datetime.utcnow().isoformat(),
        }

    # ------------------------------------------------------------------
    # Featured / specials
    # ------------------------------------------------------------------

    def get_featured_games(self) -> list[dict]:
        """Return currently featured games on the Steam front page."""
        data = self._get(STEAM_FEATURED)
        if not data:
            return []
        games = []
        for section in ("large_capsules", "featured_win", "featured_mac",
                         "featured_linux"):
            for item in data.get(section, []):
                games.append(self._normalise_featured_item(item))
        seen = set()
        return [g for g in games if g["app_id"] not in seen
                and not seen.add(g["app_id"])]

    def _normalise_featured_item(self, item: dict) -> dict:
        discount = item.get("discount_percent", 0)
        orig = item.get("original_price", 0) or 0
        final = item.get("final_price", 0) or 0
        return {
            "app_id": item.get("id"),
            "title": item.get("name", "Unknown"),
            "type": "game",
            "short_description": "",
            "current_price_usd": round(final / 100, 2),
            "original_price_usd": round(orig / 100, 2),
            "discount_percent": discount,
            "is_free": orig == 0,
            "release_date": "N/A",
            "developers": [],
            "publishers": [],
            "genres": [],
            "categories": [],
            "tags": [],
            "review_score": None,
            "review_count": None,
            "review_summary": None,
            "metacritic_score": None,
            "platforms": [],
            "header_image": item.get("large_capsule_image", ""),
            "store_url": f"https://store.steampowered.com/app/{item.get('id')}",
            "fetched_at": datetime.utcnow().isoformat(),
        }

    def get_featured_categories(self) -> dict:
        """Return games grouped by Steam's featured categories (Specials, etc.)."""
        return self._get(STEAM_FEATURED_CATS) or {}

    # ------------------------------------------------------------------
    # SteamSpy enrichment (tags, owners, playtime, review counts)
    # ------------------------------------------------------------------

    def enrich_with_steamspy(self, app_id: int) -> dict:
        """Fetch SteamSpy data for tags, owners, and review scores."""
        data = self._get(STEAMSPY_API, params={"request": "appdetails",
                                                "appid": app_id})
        if not data:
            return {}
        positive = data.get("positive", 0) or 0
        negative = data.get("negative", 0) or 0
        total = positive + negative
        score = round(positive / total * 100, 1) if total > 0 else None

        tags = list((data.get("tags") or {}).keys())

        return {
            "tags": tags,
            "review_score": score,
            "review_count": total,
            "review_summary": self._review_label(score, total),
            "owners_estimate": data.get("owners", "N/A"),
            "avg_playtime_forever": data.get("average_forever", 0),
            "median_playtime_forever": data.get("median_forever", 0),
        }

    @staticmethod
    def _review_label(score: Optional[float], count: int) -> str:
        if score is None or count < 10:
            return "No data"
        if count < 50:
            prefix = ""
        elif count < 500:
            prefix = ""
        else:
            prefix = "Overwhelmingly " if score >= 95 else ""
        if score >= 95:
            return prefix + "Overwhelmingly Positive"
        if score >= 80:
            return "Very Positive"
        if score >= 70:
            return "Mostly Positive"
        if score >= 40:
            return "Mixed"
        if score >= 20:
            return "Mostly Negative"
        return "Overwhelmingly Negative"

    # ------------------------------------------------------------------
    # Batch helpers
    # ------------------------------------------------------------------

    def get_top_sellers_steamspy(self, limit: int = 100) -> list[dict]:
        """Return top sellers from SteamSpy (no Steam key required)."""
        data = self._get(STEAMSPY_API, params={"request": "top100in2weeks"})
        if not data:
            return []
        results = []
        for app_id_str, info in list(data.items())[:limit]:
            positive = info.get("positive", 0) or 0
            negative = info.get("negative", 0) or 0
            total = positive + negative
            score = round(positive / total * 100, 1) if total > 0 else None
            results.append({
                "app_id": int(app_id_str),
                "title": info.get("name", "Unknown"),
                "owners_estimate": info.get("owners", "N/A"),
                "review_score": score,
                "review_count": total,
                "review_summary": self._review_label(score, total),
                "developers": [info.get("developer", "")] if info.get("developer") else [],
                "publishers": [info.get("publisher", "")] if info.get("publisher") else [],
                "tags": list((info.get("tags") or {}).keys()),
                "avg_playtime_forever": info.get("average_forever", 0),
            })
        return results

    def get_specials_steamspy(self) -> list[dict]:
        """Return currently discounted games from SteamSpy."""
        data = self._get(STEAMSPY_API, params={"request": "sales"})
        if not data:
            return []
        results = []
        for app_id_str, info in data.items():
            results.append({
                "app_id": int(app_id_str),
                "title": info.get("name", "Unknown"),
                "discount_percent": info.get("discount", 0),
                "current_price_usd": round((info.get("price", 0) or 0) / 100, 2),
                "original_price_usd": round((info.get("initialprice", 0) or 0) / 100, 2),
                "developers": [info.get("developer", "")] if info.get("developer") else [],
                "publishers": [info.get("publisher", "")] if info.get("publisher") else [],
            })
        return results
