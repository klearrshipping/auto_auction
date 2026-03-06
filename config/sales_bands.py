"""
Shared band definitions for Japan auction sales buckets.
Used by preanalysis, valuation_engine, and valuation_service.

Bands are disjoint: each vehicle (sale record) maps to exactly one score_band
and one mileage_band, so no car appears in more than one bucket.
"""


def get_score_band(score):
    """Map raw auction score to band: R, S, 5, 3-3.5, 4-4.5."""
    if not score:
        return None
    s = str(score).strip().upper()
    if s == "R":
        return "R"
    if s == "S":
        return "S"
    if s == "5":
        return "5"
    try:
        n = float(s)
        if 3.0 <= n <= 3.5:
            return "3-3.5"
        if 4.0 <= n <= 4.5:
            return "4-4.5"
    except ValueError:
        pass
    return None


def get_mileage_band(mileage):
    """Map mileage (km) to band."""
    if mileage is None:
        return None
    try:
        m = int(mileage)
        if m <= 30000:
            return "0-30k"
        elif m <= 60000:
            return "30k-60k"
        elif m <= 90000:
            return "60k-90k"
        elif m <= 120000:
            return "90k-120k"
        elif m <= 150000:
            return "120k-150k"
        elif m <= 200000:
            return "150k-200k"
        else:
            return "200k+"
    except (ValueError, TypeError):
        return None


# Result values considered "sold" (case-insensitive)
SOLD_RESULTS = frozenset({"sold", "negotiate sold"})
