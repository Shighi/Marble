"""
fx_rates.py – Live FX rate fetcher for Sentinel.

Fetches current rates from the Frankfurter API (https://api.frankfurter.app),
which aggregates data from the European Central Bank. Rates are cached in
memory for 4 hours so repeated calls within the same session don't hit
the network.  Falls back to hardcoded rates if the API is unreachable.
"""
import time
import logging
import requests
from datetime import datetime

logger = logging.getLogger(__name__)

# ── Fallback / seed rates (USD base, last manually verified) ──────
FALLBACK_FX_MAP: dict[str, float] = {
    "USD": 1.0,
    "EUR": 1.08,
    "GBP": 1.27,
    "ZMW": 0.037,
    "GHS": 0.068,
    "KES": 0.0077,
    "XAF": 0.0016,
    "TZS": 0.00039,
    "EGP": 0.021,
    "UGX": 0.00027,
    "NGN": 0.00063,
    "ZAR": 0.054,
    "MYR": 0.22,
    "RWF": 0.00075,
    "TND": 0.32,
    "MAD": 0.098,
    "ZWL": 0.0031,
}

# ── In-memory cache ───────────────────────────────────────────────
_cache: dict = {
    "rates":      None,
    "fetched_at": 0.0,       # unix timestamp
    "rate_date":  None,      # date string from API e.g. "2025-03-01"
    "source":     "fallback",
}
_CACHE_TTL_SECONDS = 4 * 60 * 60   # 4 hours


def _fetch_from_api() -> dict | None:
    """
    Call Frankfurter API and return a dict of {CURRENCY: rate_vs_USD}.
    Returns None on any error.
    """
    try:
        # Frankfurter returns rates relative to the base currency.
        # We use EUR as base (most currencies are quoted vs EUR on ECB),
        # then convert everything to USD base.
        url = "https://api.frankfurter.app/latest?from=USD"
        resp = requests.get(url, timeout=8)
        resp.raise_for_status()
        data = resp.json()
        # data["rates"] contains {currency: units_per_1_USD}
        rates: dict[str, float] = {"USD": 1.0}
        for ccy, units_per_usd in data["rates"].items():
            # units_per_usd means 1 USD = X ccy  →  1 ccy = 1/X USD
            rates[ccy] = round(1.0 / units_per_usd, 8)
        return {"rates": rates, "date": data.get("date", str(datetime.utcnow().date()))}
    except Exception as exc:
        logger.warning(f"[FX] Frankfurter API call failed: {exc}")
        return None


def get_fx_map(force_refresh: bool = False) -> dict[str, float]:
    """
    Return a dict mapping currency codes → USD equivalent of 1 unit.
    Results are cached for 4 hours.  On failure returns FALLBACK_FX_MAP.
    """
    now = time.time()
    cache_age = now - _cache["fetched_at"]

    if force_refresh or _cache["rates"] is None or cache_age > _CACHE_TTL_SECONDS:
        result = _fetch_from_api()
        if result:
            # Merge with fallback so any currency not on ECB still resolves
            merged = dict(FALLBACK_FX_MAP)   # start with fallback
            merged.update(result["rates"])    # overwrite with live data
            _cache["rates"]      = merged
            _cache["fetched_at"] = now
            _cache["rate_date"]  = result["date"]
            _cache["source"]     = "live"
            logger.info(f"[FX] Live rates loaded for {result['date']} "
                        f"({len(result['rates'])} currencies)")
        else:
            if _cache["rates"] is None:
                _cache["rates"]     = dict(FALLBACK_FX_MAP)
                _cache["rate_date"] = "fallback"
                _cache["source"]    = "fallback"
            logger.warning("[FX] Using cached/fallback rates")

    return _cache["rates"]


def get_rate_info() -> dict:
    """Return metadata about the currently loaded rates."""
    get_fx_map()  # ensure initialised
    return {
        "source":    _cache["source"],
        "rate_date": _cache["rate_date"],
        "fetched_at": datetime.utcfromtimestamp(_cache["fetched_at"]).strftime("%Y-%m-%d %H:%M UTC")
                       if _cache["fetched_at"] else "never",
        "currency_count": len(_cache["rates"]) if _cache["rates"] else 0,
    }


def convert_to_usd(amount: float, currency: str) -> float:
    """Convenience: convert `amount` in `currency` to USD."""
    rates = get_fx_map()
    rate  = rates.get(str(currency).upper(), 1.0)
    return float(amount) * rate
