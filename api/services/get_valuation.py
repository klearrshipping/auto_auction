"""
Get valuation for auction vehicles by pairing with sales buckets.
Uses grade→model_type lookup from japan_sales for accurate matching.
"""

import json
import asyncio
from collections import defaultdict
from typing import Optional

from supabase import create_client, ClientOptions
from dotenv import load_dotenv
import os

import sys
SERVICE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(SERVICE_DIR, "..", ".."))
sys.path.insert(0, ROOT_DIR)

from config.sales_bands import get_score_band, get_mileage_band

# Env
for p in [
    os.path.join(ROOT_DIR, "tools", "aggregate_sales", ".env"),
    os.path.join(ROOT_DIR, "tools", "aggregate_auction", ".env"),
    os.path.join(ROOT_DIR, ".env"),
]:
    if os.path.exists(p):
        load_dotenv(p)
        break


def _norm(s: str) -> str:
    return (s or "").strip().upper()


class GetValuation:
    """Gets valuation for auction vehicles by pairing with sales buckets (grade→model_type lookup from japan_sales)."""

    def __init__(self):
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")
        self._sales = create_client(url, key, options=ClientOptions(schema="sales_data"))
        self._auction = create_client(url, key, options=ClientOptions(schema="auction_data"))
        self._grade_to_model_type: dict[tuple[str, str, str], str] = {}
        self._make_model_types: dict[tuple[str, str], list[str]] = {}
        self._loaded = False

    def _load_lookup(self):
        """Build (make, model, grade) → model_type from japan_sales."""
        if self._loaded:
            return
        self._loaded = True
        all_records = []
        offset = 0
        while True:
            res = self._sales.table("japan_sales").select(
                "make,model,grade,model_type"
            ).range(offset, offset + 999).execute()
            if not res.data:
                break
            all_records.extend(res.data)
            offset += 1000

        # (make, model, grade) → most common model_type
        grade_counts: dict[tuple[str, str, str], dict[str, int]] = defaultdict(lambda: defaultdict(int))
        for r in all_records:
            make = _norm(r.get("make") or "")
            model = _norm(r.get("model") or "")
            grade = _norm(r.get("grade") or "")
            mtype = (r.get("model_type") or "").strip()
            if make and model and mtype:
                key = (make, model, grade)
                grade_counts[key][mtype] += 1

        for key, counts in grade_counts.items():
            self._grade_to_model_type[key] = max(counts, key=counts.get)

        # (make, model) → list of model_types
        for (make, model, grade), mtype in self._grade_to_model_type.items():
            km = (make, model)
            if km not in self._make_model_types:
                self._make_model_types[km] = []
            if mtype not in self._make_model_types[km]:
                self._make_model_types[km].append(mtype)

    def has_sufficient_data_for_matching(self, vehicle: dict) -> bool:
        """
        True if vehicle has enough data to attempt a bucket match.
        Vehicles missing critical fields (e.g. model_type, model, grade all missing)
        should be hidden from the listing.
        """
        year = vehicle.get("year")
        make = _norm(vehicle.get("make") or "")
        model = _norm(vehicle.get("model") or "")
        grade = (vehicle.get("grade") or "").strip()
        mtype = (vehicle.get("model_type") or "").strip()
        mileage = vehicle.get("mileage")
        score = vehicle.get("score") or (vehicle.get("scores") or {}).get("score")

        if not year or year < 1900 or year > 2030:
            return False
        if not make or not model:
            return False
        if get_score_band(score) is None:
            return False
        if get_mileage_band(mileage) is None:
            return False
        # Need at least one of model_type or grade to resolve model_type (or fallbacks)
        if not mtype and not grade:
            self._load_lookup()
            km = (make, model)
            if km not in self._make_model_types:
                return False
        return True

    def _resolve_model_type(self, vehicle: dict) -> str | None:
        """Resolve model_type for vehicle using grade lookup and fallbacks."""
        make = _norm(vehicle.get("make") or "")
        model = _norm(vehicle.get("model") or "")
        grade = (vehicle.get("grade") or "").strip()
        mtype = (vehicle.get("model_type") or "").strip()

        if make and model and mtype:
            return mtype.upper()

        self._load_lookup()

        # 1. Exact (make, model, grade) - normalize grade for lookup
        grade_norm = _norm(grade)
        key = (make, model, grade_norm)
        if key in self._grade_to_model_type:
            return self._grade_to_model_type[key].upper()

        # 2. (make, model, "") when grade empty
        if grade_norm:
            key2 = (make, model, "")
            if key2 in self._grade_to_model_type:
                return self._grade_to_model_type[key2].upper()

        # 3. Try model as model_type (common when only one variant exists)
        km = (make, model)
        model_types = self._make_model_types.get(km, [])
        for mt in model_types:
            if _norm(mt) == model:
                return mt.upper()

        # 4. First model_type for (make, model)
        if km in self._make_model_types and self._make_model_types[km]:
            return self._make_model_types[km][0].upper()

        return None

    def _compute_valuation(self, bucket: dict) -> dict:
        """Compute valuation from bucket row (same logic as ValuationService)."""
        tier = bucket.get("confidence_tier", "thin")
        iqr = bucket.get("iqr", 0)
        trimmed = bucket.get("trimmed_mean")
        median = bucket.get("median_price")
        min_p = bucket.get("min_price", 0)
        max_p = bucket.get("max_price", 0)

        fv = trimmed if (tier == "strong" and trimmed) else median
        method = "trimmed mean" if (tier == "strong" and trimmed) else "median"
        if not fv and median:
            fv = median
        elif not fv:
            fv = bucket.get("mean_price", 0)

        range_low = fv - (iqr // 2) if iqr else min_p
        range_high = fv + (iqr // 2) if iqr else max_p

        return {
            "fv_price": fv,
            "fv_method": method,
            "range_low": range_low,
            "range_high": range_high,
            "median_price": median,
            "mean_price": bucket.get("mean_price"),
            "last_sold_price": bucket.get("last_sold_price"),
            "last_sold_date": bucket.get("last_sold_date"),
            "comparable_count": bucket.get("comparable_count", 0),
            "confidence_tier": tier,
            "min_price": min_p,
            "max_price": max_p,
        }

    async def pair_vehicle(self, vehicle: dict) -> tuple[dict, dict | None, dict | None]:
        """
        Pair a vehicle with its bucket and compute valuation.
        Returns (vehicle, bucket, valuation) or (vehicle, None, None) if no match.
        """
        year = vehicle.get("year")
        make = _norm(vehicle.get("make") or "")
        model = _norm(vehicle.get("model") or "")
        mileage = vehicle.get("mileage")
        score = vehicle.get("score") or (vehicle.get("scores") or {}).get("score")

        if not year or not make or not model:
            return (vehicle, None, None)

        score_band = get_score_band(score)
        mileage_band = get_mileage_band(mileage)
        if not score_band or not mileage_band:
            return (vehicle, None, None)

        model_type = self._resolve_model_type(vehicle)
        if not model_type:
            return (vehicle, None, None)

        res = self._sales.table("japan_sales_buckets").select("*").eq(
            "year", year
        ).eq("make", make).eq("model", model).eq(
            "model_type", model_type
        ).eq("score_band", score_band).eq("mileage_band", mileage_band).limit(1).execute()

        if res.data:
            bucket = res.data[0]
            valuation = self._compute_valuation(bucket)
            return (vehicle, bucket, valuation)

        # Fallback: try other model_types for (make, model)
        self._load_lookup()
        km = (make, model)
        if km in self._make_model_types:
            for mt in self._make_model_types[km]:
                res2 = self._sales.table("japan_sales_buckets").select("*").eq(
                    "year", year
                ).eq("make", make).eq("model", model).eq(
                    "model_type", mt.upper()
                ).eq("score_band", score_band).eq("mileage_band", mileage_band).limit(1).execute()
                if res2.data:
                    bucket = res2.data[0]
                    valuation = self._compute_valuation(bucket)
                    return (vehicle, bucket, valuation)

        return (vehicle, None, None)
