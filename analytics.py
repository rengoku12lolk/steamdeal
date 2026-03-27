"""
analytics.py
------------
Scoring, ranking, and analytical logic for the Steam analytics tool.

Scoring Methodology
-------------------
Each game receives a composite "deal score" (0–100) based on four weighted factors:

  1. Review Quality   (40%)  — normalised review score × confidence weight
  2. Discount Size    (30%)  — non-linear mapping; big discounts score higher
  3. Price Value      (15%)  — lower absolute price = higher accessibility score
  4. Popularity       (15%)  — log-scaled review count as a proxy for quality signal

Publisher ranking uses: avg review score, number of titles, % of titles that
are "Very Positive" or higher, and median deal score across their catalogue.
"""

import math
import logging
from typing import Optional
from collections import defaultdict

logger = logging.getLogger(__name__)

# Weight constants
W_REVIEW = 0.40
W_DISCOUNT = 0.30
W_PRICE = 0.15
W_POPULARITY = 0.15

# Thresholds
MIN_REVIEWS_FOR_CONFIDENCE = 50
HIGH_VALUE_REVIEW_THRESHOLD = 80.0   # score %
HIGH_VALUE_DISCOUNT_THRESHOLD = 40   # %
DEEP_DISCOUNT_THRESHOLD = 70         # %


# ---------------------------------------------------------------------------
# Core scoring
# ---------------------------------------------------------------------------

def _review_component(score: Optional[float], count: Optional[int]) -> float:
    """
    Review quality component (0–1).
    Applies a confidence penalty when review count is low.
    """
    if score is None or count is None or count == 0:
        return 0.0
    # Bayesian-style confidence: shrink toward 60 (neutral) when count is low
    confidence = min(count / MIN_REVIEWS_FOR_CONFIDENCE, 1.0)
    adjusted = score * confidence + 60.0 * (1 - confidence)
    return max(0.0, min(adjusted / 100.0, 1.0))


def _discount_component(discount_pct: int) -> float:
    """
    Discount component (0–1) with non-linear scaling.
    Rewards deep discounts disproportionately.
    """
    if discount_pct <= 0:
        return 0.0
    # sqrt mapping: 50% discount → 0.71, 75% → 0.87, 100% → 1.0
    return math.sqrt(discount_pct / 100.0)


def _price_component(current_price: float) -> float:
    """
    Price accessibility component (0–1).
    Free = 1.0; >$60 = 0.0; roughly linear in log-space.
    """
    if current_price <= 0:
        return 1.0
    if current_price >= 60:
        return 0.0
    return 1.0 - (math.log(current_price + 1) / math.log(61))


def _popularity_component(review_count: Optional[int]) -> float:
    """
    Popularity component (0–1) using log-scale.
    10 reviews → ~0.17; 1 000 → 0.50; 100 000 → 0.83.
    """
    if not review_count or review_count <= 0:
        return 0.0
    return min(math.log10(review_count + 1) / 6.0, 1.0)


def compute_deal_score(game: dict) -> float:
    """
    Return a composite deal score (0–100) for a game record.
    Higher = better deal.
    """
    r = _review_component(game.get("review_score"), game.get("review_count"))
    d = _discount_component(game.get("discount_percent", 0))
    p = _price_component(game.get("current_price_usd", 999))
    pop = _popularity_component(game.get("review_count"))

    raw = (W_REVIEW * r + W_DISCOUNT * d + W_PRICE * p + W_POPULARITY * pop)
    return round(raw * 100, 2)


def enrich_games(games: list[dict]) -> list[dict]:
    """Add deal_score and value flags to a list of game records."""
    for g in games:
        g["deal_score"] = compute_deal_score(g)
        g["is_high_value"] = _is_high_value(g)
        g["promotion_type"] = _classify_promotion(g)
    return games


def _is_high_value(g: dict) -> bool:
    score = g.get("review_score") or 0
    discount = g.get("discount_percent", 0)
    price = g.get("current_price_usd", 999)
    return (
        (score >= HIGH_VALUE_REVIEW_THRESHOLD and discount >= HIGH_VALUE_DISCOUNT_THRESHOLD)
        or (score >= 85 and price <= 5.0)
        or (discount >= DEEP_DISCOUNT_THRESHOLD and score >= 70)
    )


def _classify_promotion(g: dict) -> str:
    discount = g.get("discount_percent", 0)
    score = g.get("review_score") or 0
    if discount == 0:
        return "none"
    if discount >= DEEP_DISCOUNT_THRESHOLD:
        return "deep_discount"
    if discount >= 50 and score >= HIGH_VALUE_REVIEW_THRESHOLD:
        return "high_rated_sale"
    if discount >= 50:
        return "major_sale"
    if discount >= 25:
        return "regular_sale"
    return "minor_sale"


# ---------------------------------------------------------------------------
# Publisher / developer analytics
# ---------------------------------------------------------------------------

def rank_publishers(games: list[dict]) -> list[dict]:
    """
    Aggregate per-publisher stats and return a ranked list.

    Ranking metric = publisher_score (0–100) based on:
      - avg_review_score        (50%)
      - positive_title_rate     (30%)  — % titles >= 80 review score
      - avg_deal_score          (20%)
    """
    pub_data: dict = defaultdict(lambda: {
        "titles": [],
        "review_scores": [],
        "deal_scores": [],
    })

    for g in games:
        for pub in (g.get("publishers") or []):
            if not pub:
                continue
            pub_data[pub]["titles"].append(g.get("title", ""))
            rs = g.get("review_score")
            if rs is not None:
                pub_data[pub]["review_scores"].append(rs)
            pub_data[pub]["deal_scores"].append(g.get("deal_score", 0))

    rankings = []
    for pub, data in pub_data.items():
        rs = data["review_scores"]
        ds = data["deal_scores"]
        avg_rs = sum(rs) / len(rs) if rs else 0
        positive_rate = sum(1 for s in rs if s >= 80) / len(rs) if rs else 0
        avg_ds = sum(ds) / len(ds) if ds else 0

        pub_score = (
            0.50 * (avg_rs / 100)
            + 0.30 * positive_rate
            + 0.20 * (avg_ds / 100)
        ) * 100

        rankings.append({
            "publisher": pub,
            "title_count": len(data["titles"]),
            "titles": data["titles"],
            "avg_review_score": round(avg_rs, 1),
            "positive_title_rate": round(positive_rate * 100, 1),
            "avg_deal_score": round(avg_ds, 2),
            "publisher_score": round(pub_score, 2),
        })

    return sorted(rankings, key=lambda x: x["publisher_score"], reverse=True)


# ---------------------------------------------------------------------------
# Filtering helpers
# ---------------------------------------------------------------------------

def filter_games(games: list[dict],
                 genres: list[str] = None,
                 min_price: float = None,
                 max_price: float = None,
                 min_discount: int = None,
                 min_review_score: float = None,
                 promotion_types: list[str] = None) -> list[dict]:
    """Return filtered subset of game records."""
    result = []
    for g in games:
        if genres:
            game_genres = [x.lower() for x in (g.get("genres") or [])]
            game_tags = [x.lower() for x in (g.get("tags") or [])]
            combined = game_genres + game_tags
            if not any(genre.lower() in combined for genre in genres):
                continue
        price = g.get("current_price_usd", 0)
        if min_price is not None and price < min_price:
            continue
        if max_price is not None and price > max_price:
            continue
        if min_discount is not None and g.get("discount_percent", 0) < min_discount:
            continue
        if min_review_score is not None:
            rs = g.get("review_score")
            if rs is None or rs < min_review_score:
                continue
        if promotion_types:
            if g.get("promotion_type") not in promotion_types:
                continue
        result.append(g)
    return result


# ---------------------------------------------------------------------------
# Promotion detection
# ---------------------------------------------------------------------------

def find_notable_promotions(games: list[dict]) -> dict:
    """
    Return curated promotion buckets:
      - deep_discounts        : discount >= 70%
      - high_rated_on_sale    : review >= 80 AND discount >= 25%
      - best_value_deals      : top deal_score with any discount
      - budget_gems           : price <= $5, review >= 75
    """
    enriched = enrich_games(games) if not games[0].get("deal_score") else games
    on_sale = [g for g in enriched if g.get("discount_percent", 0) > 0]

    deep = sorted(
        [g for g in on_sale if g.get("discount_percent", 0) >= DEEP_DISCOUNT_THRESHOLD],
        key=lambda x: x["discount_percent"], reverse=True
    )
    high_rated = sorted(
        [g for g in on_sale
         if (g.get("review_score") or 0) >= HIGH_VALUE_REVIEW_THRESHOLD
         and g.get("discount_percent", 0) >= 25],
        key=lambda x: x["deal_score"], reverse=True
    )
    best_value = sorted(on_sale, key=lambda x: x["deal_score"], reverse=True)[:20]
    budget_gems = sorted(
        [g for g in enriched
         if g.get("current_price_usd", 999) <= 5.0
         and (g.get("review_score") or 0) >= 75],
        key=lambda x: x["review_score"], reverse=True
    )

    return {
        "deep_discounts": deep[:20],
        "high_rated_on_sale": high_rated[:20],
        "best_value_deals": best_value,
        "budget_gems": budget_gems[:20],
    }


# ---------------------------------------------------------------------------
# Summary statistics
# ---------------------------------------------------------------------------

def generate_summary(games: list[dict], publishers: list[dict]) -> dict:
    """Generate dashboard-level summary statistics."""
    on_sale = [g for g in games if g.get("discount_percent", 0) > 0]
    with_scores = [g for g in games if g.get("review_score") is not None]
    avg_score = (sum(g["review_score"] for g in with_scores) / len(with_scores)
                 if with_scores else 0)
    avg_discount = (sum(g["discount_percent"] for g in on_sale) / len(on_sale)
                    if on_sale else 0)

    return {
        "total_games": len(games),
        "games_on_sale": len(on_sale),
        "high_value_games": sum(1 for g in games if g.get("is_high_value")),
        "avg_review_score": round(avg_score, 1),
        "avg_discount_pct": round(avg_discount, 1),
        "top_publishers": publishers[:5],
        "promotion_breakdown": _promo_breakdown(games),
    }


def _promo_breakdown(games: list[dict]) -> dict:
    counts: dict = defaultdict(int)
    for g in games:
        counts[g.get("promotion_type", "none")] += 1
    return dict(counts)
