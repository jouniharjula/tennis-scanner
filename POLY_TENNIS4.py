#!/usr/bin/env python3
"""
Polymarket ATP + WTA Tennis Real-Time Signal Engine
Six-case alerts for LIVE matches only, with per-case cooldowns and
rate-limit-friendly API caching. Observe-only — no orders are placed.

This version:
- Skips all doubles (Polymarket + LiveTennis)
- Only sends ntfy when a Bollinger band is touched
- Mentions C6 inside the ntfy message when it applies
"""

import os
import time
import json
import warnings
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any, Tuple
from dataclasses import dataclass, field

import pandas as pd

# Force print to flush after every call
_print = print
def print(*args, **kwargs):
    kwargs.setdefault("flush", True)
    _print(*args, **kwargs)

warnings.filterwarnings("ignore", category=DeprecationWarning)

from polymarket_tennis import (
    GammaClient,
    LiveTennisClient,
    discover_tennis_markets,
    match_market,
    build_view,
)

# ============================================================================
# RELAY CONFIGURATION
# ============================================================================

PA_RELAY_URL = os.environ.get(
    "PA_RELAY_URL",
    "https://jharjula.pythonanywhere.com/relay",
)

# ============================================================================
# NTFY CONFIGURATION (reference only)
# ============================================================================

NTFY_TOPIC = os.environ.get("NTFY_TOPIC", "trademap-jl-7x9k2")
NTFY_BASE_URL = os.environ.get("NTFY_BASE_URL", "https://ntfy.sh")
NTFY_USERNAME = os.environ.get("NTFY_USERNAME", "")
NTFY_PASSWORD = os.environ.get("NTFY_PASSWORD", "")

MAX_NTFY_LENGTH = 3900
NTFY_MIN_GAP_SEC = 6.0
NTFY_INTER_CHUNK_DELAY = 6.0
NTFY_MAX_ATTEMPTS = 3
NTFY_CIRCUIT_BREAKER_THRESHOLD = 120.0
NTFY_BACKOFF_BASE_SEC = 8.0
NTFY_BACKOFF_MAX_SEC = 60.0

# ============================================================================
# SCANNER CONFIGURATION
# ============================================================================

LIVETENNIS_API_KEY = os.environ.get(
    "LIVETENNIS_API_KEY",
    "twjp_fb6099bc479d519a016dc727e174ba60",
)

POLL_INTERVAL_SECONDS = 900
LIVE_MATCHES_CACHE_SECONDS = 180
VIEW_CACHE_SECONDS = 900

BOLLINGER_PERIOD = 20
BOLLINGER_STD = 2.0
RESAMPLE_INTERVAL = "5min"
HISTORY_HOURS = 3

ENABLED_CASES = {
    "C1_BREAKPOINT_CONTEXT",
    "C2_SCORE_DRIVEN",
    "C3_TIMEBREAK_NOISE",
    "C4_VOLATILITY_ADJUSTED",
    "C5_MARKOV_FAIRVALUE",
    "C6_FAVORITE_LONGSHOT",
}

CHANGEOVER_THRESHOLD_S = 85
VOL_HIGH_IQR = 0.04
VOL_LOW_IQR = 0.015
MIN_IQR = 0.001
MARKOV_DIVERGENCE = 0.06

C4_RECENT_ACTIVITY_WINDOW_S = POLL_INTERVAL_SECONDS * 2

PRICE_NEAR_CERTAIN = 0.97
PRICE_NEAR_HOPELESS = 0.03

DEFAULT_ALERT_COOLDOWN_SECONDS = 300
CASE_SPECIFIC_COOLDOWN = {
    "C1_BREAKPOINT_CONTEXT":  600,
    "C2_SCORE_DRIVEN":        900,
    "C3_TIMEBREAK_NOISE":     900,
    "C4_VOLATILITY_ADJUSTED": 900,
    "C5_MARKOV_FAIRVALUE":    900,
    "C6_FAVORITE_LONGSHOT":   900,
}

# Special cooldown for band-touch alerts
BAND_TOUCH_COOLDOWN_SECONDS = 600

# ============================================================================
# API ENDPOINTS AND CACHES
# ============================================================================

GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
CLOB_PRICES_URL = "https://clob.polymarket.com/prices-history"

_GAMMA_TOKEN_CACHE: Dict[str, Optional[List[str]]] = {}


def _gamma_lookup_token_ids(slug: str) -> Optional[List[str]]:
    if slug in _GAMMA_TOKEN_CACHE:
        return _GAMMA_TOKEN_CACHE[slug]

    try:
        params = urllib.parse.urlencode({"slug": slug})
        url = f"{GAMMA_MARKETS_URL}?{params}"
        req = urllib.request.Request(
            url, headers={"User-Agent": "tennis-scanner/1.0"}
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        if not isinstance(data, list) or not data:
            _GAMMA_TOKEN_CACHE[slug] = None
            return None

        raw = data[0].get("clobTokenIds")
        if isinstance(raw, str):
            raw = json.loads(raw)
        if not isinstance(raw, list) or not raw:
            _GAMMA_TOKEN_CACHE[slug] = None
            return None

        _GAMMA_TOKEN_CACHE[slug] = raw
        return raw

    except Exception as e:
        print(f"  ! gamma lookup error for {slug}: {e}")
        _GAMMA_TOKEN_CACHE[slug] = None
        return None


def extract_token_id(market) -> Optional[str]:
    slug = getattr(market, "slug", None)
    if not slug:
        return None
    tokens = _gamma_lookup_token_ids(slug)
    if not tokens:
        return None
    return tokens[0]


# ============================================================================
# PRICE HISTORY
# ============================================================================

def fetch_price_history(token_id: str, hours_back: int = HISTORY_HOURS
                        ) -> Optional[pd.DataFrame]:
    end_ts = int(time.time())
    start_ts = end_ts - (hours_back * 3600)

    params = urllib.parse.urlencode({
        "market": token_id,
        "startTs": start_ts,
        "endTs": end_ts,
        "fidelity": 1,
    })
    url = f"{CLOB_PRICES_URL}?{params}"

    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "tennis-scanner/1.0"}
        )
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"  ! history fetch error: {e}")
        return None

    if isinstance(data, dict):
        history = data.get("history", [])
    elif isinstance(data, list):
        history = data
    else:
        return None

    if not history:
        return None

    rows = []
    for pt in history:
        try:
            if "t" in pt and "p" in pt:
                rows.append({"timestamp": pt["t"], "close": float(pt["p"])})
        except Exception:
            continue

    if not rows:
        return None

    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df = (df.set_index("timestamp")
            .resample(RESAMPLE_INTERVAL)
            .last()
            .dropna()
            .reset_index()
            .sort_values("timestamp")
            .reset_index(drop=True))

    print(f"  [fetch] {len(rows)} raw -> {len(df)} resampled points")
    return df


# ============================================================================
# RELAY CLIENT
# ============================================================================

class RelayClient:
    def __init__(self, relay_url: str):
        self.relay_url = relay_url
        self._last_send_ts = 0.0
        self._min_gap_sec = 2.0

    def _wait_for_slot(self):
        delta = time.monotonic() - self._last_send_ts
        if delta < self._min_gap_sec:
            time.sleep(self._min_gap_sec - delta)

    def send(self, title: str, body: str, priority: str = "default",
             tags: str = "") -> bool:
        self._wait_for_slot()

        payload = json.dumps({
            "title": title,
            "body": body,
            "priority": priority,
            "tags": tags,
        }).encode("utf-8")

        req = urllib.request.Request(
            self.relay_url,
            data=payload,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "tennis-scanner/1.0",
            },
            method="POST",
        )

        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                status = resp.status
                preview = resp.read(200).decode("utf-8", "ignore")
                print(f"  [relay] PA responded {status}: {preview[:120]}")
                self._last_send_ts = time.monotonic()
                return 200 <= status < 300
        except urllib.error.HTTPError as e:
            preview = e.read(300).decode("utf-8", "ignore")
            print(f"  [relay] PA HTTP {e.code}: {e.reason} — {preview}")
            self._last_send_ts = time.monotonic()
            return False
        except Exception as e:
            print(f"  [relay] PA error: {e}")
            self._last_send_ts = time.monotonic()
            return False


RELAY = RelayClient(PA_RELAY_URL)


# ============================================================================
# DATA STRUCTURES
# ============================================================================

@dataclass
class MarketAlert:
    market_slug: str
    last_alert_time: Optional[datetime] = None
    last_alert_type: Optional[str] = None


@dataclass
class MarketSnapshot:
    market_slug: str
    question: str
    tour: str
    outcomes: List[str]
    token_id: str
    current_price: float
    bands: Optional[Dict[str, float]]
    touch: Optional[str]
    score_line: str
    last_score_change_ts: Optional[datetime]
    break_point_flag: bool
    server: Optional[str]
    receiver: Optional[str]
    last_point_time: Optional[datetime]
    recent_prices: List[float]
    role: str
    markov_fair_prob: Optional[float] = None


@dataclass
class Signal:
    case_id: str
    direction: str
    title: str
    reason: str
    trade_action: str
    confidence: str
    details: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# HELPERS
# ============================================================================

def market_is_near_resolved(price: float) -> bool:
    return price >= PRICE_NEAR_CERTAIN or price <= PRICE_NEAR_HOPELESS


def calculate_bollinger_bands(prices: pd.Series, period: int = BOLLINGER_PERIOD,
                              std_mult: float = BOLLINGER_STD
                              ) -> Optional[Dict[str, float]]:
    if len(prices) < period:
        return None
    mean = prices.rolling(period).mean()
    std = prices.rolling(period).std()
    return {
        "middle": float(mean.iloc[-1]),
        "upper": float((mean + std_mult * std).iloc[-1]),
        "lower": float((mean - std_mult * std).iloc[-1]),
    }


def detect_band_touch(price: float, bands: Optional[Dict[str, float]],
                      tolerance: float = 0.005) -> Optional[str]:
    if bands is None:
        return None
    if pd.isna(bands["upper"]) or pd.isna(bands["lower"]):
        return None
    if price >= bands["upper"] * (1 - tolerance):
        return "upper"
    if price <= bands["lower"] * (1 + tolerance):
        return "lower"
    return None


def estimate_markov_fair_prob(score_line: str, server: Optional[str],
                              player_of_interest: str) -> Optional[float]:
    if not score_line:
        return None
    try:
        parts = score_line.replace(",", " ").split()
        sets_a, sets_b, games_a, games_b = 0, 0, 0, 0
        for p in parts:
            if "-" not in p:
                continue
            a, b = p.split("-")[:2]
            a_i, b_i = int(a), int(b)
            if a_i >= 6 or b_i >= 6:
                if a_i > b_i:
                    sets_a += 1
                else:
                    sets_b += 1
            else:
                games_a, games_b = a_i, b_i
        lead = (sets_a - sets_b) * 0.35 + (games_a - games_b) * 0.04
        return max(0.02, min(0.98, 0.5 + lead))
    except Exception:
        return None


# ============================================================================
# SHARED HELPERS
# ============================================================================

def _get_current_role(s: MarketSnapshot) -> str:
    if len(s.outcomes) != 2:
        return s.role if s.role != "unknown" else "unknown"

    p = s.current_price
    if p >= 0.55:
        role_from_price = "favorite"
    elif p <= 0.45:
        role_from_price = "underdog"
    else:
        role_from_price = "unknown"

    if s.markov_fair_prob is not None:
        role_from_markov = "favorite" if s.markov_fair_prob >= 0.55 else "underdog"
        if role_from_price == role_from_markov:
            return role_from_price
        if role_from_price == "unknown":
            return role_from_markov

    if role_from_price != "unknown":
        return role_from_price
    return s.role if s.role != "unknown" else "unknown"


def _touch_distance(s: MarketSnapshot) -> Optional[float]:
    if s.bands is None or s.touch is None:
        return None
    if s.touch == "upper":
        return s.current_price - s.bands["upper"]
    if s.touch == "lower":
        return s.bands["lower"] - s.current_price
    return None


def _is_high_vol(s: MarketSnapshot) -> bool:
    if len(s.recent_prices) < BOLLINGER_PERIOD:
        return False
    tail = pd.Series(s.recent_prices[-BOLLINGER_PERIOD:])
    iqr = float(tail.quantile(0.75) - tail.quantile(0.25))
    return iqr >= VOL_HIGH_IQR


def _seconds_since_score(s: MarketSnapshot) -> Optional[float]:
    if s.last_score_change_ts is None:
        return None
    return (datetime.now() - s.last_score_change_ts).total_seconds()


# ============================================================================
# SIX CASE DETECTORS (still used for console + C6 detection)
# ============================================================================

def case_1_breakpoint(s: MarketSnapshot) -> Optional[Signal]:
    if not s.break_point_flag or s.touch is None:
        return None
    dist = _touch_distance(s)
    if dist is None or dist < 0.006:
        return None
    if s.last_point_time is not None:
        gap = (datetime.now() - s.last_point_time).total_seconds()
        if gap >= CHANGEOVER_THRESHOLD_S:
            return None
    role = _get_current_role(s)
    high_vol = _is_high_vol(s)
    confidence = "MEDIUM"
    details = {"server": s.server, "receiver": s.receiver, "role": role, "touch_distance": round(dist, 4)}
    if s.markov_fair_prob is not None:
        div = abs(s.current_price - s.markov_fair_prob)
        details["markov_div"] = round(div, 4)
        if div >= MARKOV_DIVERGENCE:
            confidence = "HIGH"
    if high_vol and confidence == "HIGH":
        confidence = "MEDIUM"
    if s.touch == "upper":
        title = "BREAK POINT + UPPER TOUCH"
        reason = f"Server ({s.server}) odds spiked to upper band while facing break point. Market overrating the save."
        trade_action = f"FADE the server ({s.server}). Consider buying {s.receiver}."
        if role == "favorite":
            confidence = "HIGH" if confidence != "LOW" else confidence
    else:
        title = "BREAK POINT + LOWER TOUCH"
        reason = f"Receiver ({s.receiver}) odds dropped to lower band while holding break point. Market overrating the miss."
        trade_action = f"FADE the market. Consider buying {s.receiver}."
    return Signal(case_id="C1_BREAKPOINT_CONTEXT", direction="fade", title=title,
                  reason=reason, trade_action=trade_action, confidence=confidence, details=details)


def case_2_score_driven(s: MarketSnapshot) -> Optional[Signal]:
    if s.touch is None:
        return None
    dist = _touch_distance(s)
    if dist is None or dist < 0.005:
        return None
    seconds = _seconds_since_score(s)
    high_vol = _is_high_vol(s)
    if seconds is None:
        return Signal(case_id="C2_SCORE_DRIVEN", direction="info",
                      title=f"{s.touch.upper()} TOUCH — no score context",
                      reason="Band touch but no recorded score change yet.",
                      trade_action="WAIT for next scan to establish score context.",
                      confidence="LOW", details={"touch_distance": round(dist, 4)})
    recent_window = POLL_INTERVAL_SECONDS * 0.85
    if seconds <= recent_window:
        conf = "MEDIUM" if high_vol else "HIGH"
        extra = " (high-vol regime — reduced confidence)" if high_vol else ""
        return Signal(case_id="C2_SCORE_DRIVEN", direction="follow",
                      title=f"{s.touch.upper()} TOUCH — REAL SCORE MOVE",
                      reason=f"Score changed {seconds:.0f}s ago. Event-driven repricing{extra}.",
                      trade_action=f"FOLLOW the move. Buy the player whose odds touched the {s.touch} band.",
                      confidence=conf,
                      details={"seconds_since_score_change": round(seconds, 1),
                               "touch_distance": round(dist, 4), "high_vol": high_vol})
    return Signal(case_id="C2_SCORE_DRIVEN", direction="fade",
                  title=f"{s.touch.upper()} TOUCH — NO RECENT SCORE CHANGE",
                  reason=f"Score unchanged for {seconds:.0f}s but odds touched a band.",
                  trade_action=f"FADE. Buy the OPPOSITE side of the {s.touch} touch.",
                  confidence="MEDIUM",
                  details={"seconds_since_score_change": round(seconds, 1),
                           "touch_distance": round(dist, 4)})


def case_3_timebreak(s: MarketSnapshot) -> Optional[Signal]:
    if s.touch is None or s.last_point_time is None:
        return None
    gap = (datetime.now() - s.last_point_time).total_seconds()
    if gap >= CHANGEOVER_THRESHOLD_S:
        return Signal(case_id="C3_TIMEBREAK_NOISE", direction="fade",
                      title="BAND TOUCH DURING CHANGEOVER",
                      reason=f"No point for {gap:.0f}s. Band touches here are noise.",
                      trade_action="DO NOT CHASE. Wait for play to resume.",
                      confidence="HIGH", details={"gap_seconds": round(gap, 1)})
    return None


def case_4_volatility(s: MarketSnapshot) -> Optional[Signal]:
    if s.touch is None or len(s.recent_prices) < BOLLINGER_PERIOD:
        return None
    tail = pd.Series(s.recent_prices[-BOLLINGER_PERIOD:])
    q1, q3 = float(tail.quantile(0.25)), float(tail.quantile(0.75))
    iqr = q3 - q1
    if iqr < MIN_IQR:
        return None
    dist = _touch_distance(s)
    details = {"iqr": round(iqr, 4)}
    if dist is not None:
        details["touch_distance"] = round(dist, 4)
    if iqr >= VOL_HIGH_IQR:
        return Signal(case_id="C4_VOLATILITY_ADJUSTED", direction="info",
                      title=f"{s.touch.upper()} TOUCH IN HIGH-VOL REGIME",
                      reason=f"IQR={iqr:.4f} (high). Bands too noisy.",
                      trade_action="IGNORE unless confirmed by C1, C2 or C5.",
                      confidence="LOW", details={**details, "regime": "high"})
    if iqr <= VOL_LOW_IQR:
        seconds = _seconds_since_score(s)
        recently_active = seconds is not None and seconds <= C4_RECENT_ACTIVITY_WINDOW_S
        if not recently_active:
            return Signal(case_id="C4_VOLATILITY_ADJUSTED", direction="info",
                          title=f"{s.touch.upper()} TOUCH IN LOW-VOL (STALE)",
                          reason=f"IQR={iqr:.4f} low but no recent score activity. Quiet market — touch is likely noise.",
                          trade_action="IGNORE — no score context to justify FOLLOW.",
                          confidence="LOW", details={**details, "regime": "low-stale"})
        return Signal(case_id="C4_VOLATILITY_ADJUSTED", direction="follow",
                      title=f"{s.touch.upper()} TOUCH IN LOW-VOL REGIME",
                      reason=f"IQR={iqr:.4f} (low) + recent score activity. Band touch is meaningful.",
                      trade_action=f"FOLLOW. Buy the {s.touch} band touch side.",
                      confidence="MEDIUM", details={**details, "regime": "low-active"})
    return None


def case_5_markov(s: MarketSnapshot) -> Optional[Signal]:
    if s.markov_fair_prob is None or s.touch is None:
        return None
    seconds = _seconds_since_score(s)
    if seconds is None or seconds > POLL_INTERVAL_SECONDS * 1.5:
        return None
    dist = _touch_distance(s)
    if dist is None or dist < 0.006:
        return None
    if s.last_point_time is not None:
        gap = (datetime.now() - s.last_point_time).total_seconds()
        if gap >= CHANGEOVER_THRESHOLD_S:
            return None
    divergence = s.current_price - s.markov_fair_prob
    high_vol = _is_high_vol(s)
    role = _get_current_role(s)
    details = {
        "divergence": round(divergence, 4),
        "markov_fair": round(s.markov_fair_prob, 4),
        "price": round(s.current_price, 4),
        "touch_distance": round(dist, 4),
        "seconds_since_score": round(seconds, 1),
        "role": role,
    }
    threshold = MARKOV_DIVERGENCE * (1.15 if high_vol else 1.0)
    if s.touch == "upper" and divergence > threshold:
        conf = "HIGH" if abs(divergence) >= threshold * 1.3 else "MEDIUM"
        if high_vol and conf == "HIGH":
            conf = "MEDIUM"
        return Signal(case_id="C5_MARKOV_FAIRVALUE", direction="fade",
                      title="UPPER TOUCH ABOVE MARKOV FAIR VALUE",
                      reason=f"Market {s.current_price:.3f} vs Markov {s.markov_fair_prob:.3f} (+{divergence:.3f}) after recent score change.",
                      trade_action="FADE. Buy the opposite outcome.",
                      confidence=conf, details=details)
    if s.touch == "lower" and -divergence > threshold:
        conf = "HIGH" if abs(divergence) >= threshold * 1.3 else "MEDIUM"
        if high_vol and conf == "HIGH":
            conf = "MEDIUM"
        return Signal(case_id="C5_MARKOV_FAIRVALUE", direction="fade",
                      title="LOWER TOUCH BELOW MARKOV FAIR VALUE",
                      reason=f"Market {s.current_price:.3f} vs Markov {s.markov_fair_prob:.3f} ({divergence:.3f}) after recent score change.",
                      trade_action="FADE. Buy the underpriced side.",
                      confidence=conf, details=details)
    return None


def case_6_favorite_longshot(s: MarketSnapshot) -> Optional[Signal]:
    if s.touch is None:
        return None
    role = _get_current_role(s)
    if role == "unknown":
        return None
    if s.last_score_change_ts is None:
        return None
    seconds_since_score = (datetime.now() - s.last_score_change_ts).total_seconds()
    if seconds_since_score > POLL_INTERVAL_SECONDS * 1.5:
        return None
    if s.last_point_time is not None:
        gap = (datetime.now() - s.last_point_time).total_seconds()
        if gap >= CHANGEOVER_THRESHOLD_S:
            return None
    dist = _touch_distance(s)
    if dist is None or dist < 0.006:
        return None
    is_favorite_dip = (role == "favorite" and s.touch == "lower")
    is_underdog_spike = (role == "underdog" and s.touch == "upper")
    if not (is_favorite_dip or is_underdog_spike):
        return None
    high_vol = _is_high_vol(s)
    confidence = "MEDIUM"
    details = {
        "role": role,
        "price": round(s.current_price, 4),
        "touch_distance": round(dist, 4),
        "seconds_since_score": round(seconds_since_score, 1),
    }
    if s.markov_fair_prob is not None:
        markov_div = s.markov_fair_prob - s.current_price
        details["markov_fair"] = round(s.markov_fair_prob, 4)
        details["markov_div"] = round(markov_div, 4)
        if abs(markov_div) >= MARKOV_DIVERGENCE:
            confidence = "HIGH"
    if is_favorite_dip and s.current_price >= 0.62:
        confidence = "HIGH"
    if is_underdog_spike and s.current_price <= 0.38:
        confidence = "HIGH"
    if high_vol and confidence == "HIGH":
        confidence = "MEDIUM"
    elif high_vol:
        details["vol_regime"] = "high"
    if is_favorite_dip:
        title = "FAVORITE HIT LOWER BAND"
        reason = (
            f"Favorite odds dropped to lower band (dist={dist:.3f}) "
            f"after recent score change ({seconds_since_score:.0f}s ago). "
            "Market over-fearing underdog momentum."
        )
        trade_action = "FADE. Buy the FAVORITE — likely underpriced."
    else:
        title = "UNDERDOG HIT UPPER BAND"
        reason = (
            f"Underdog odds spiked to upper band (dist={dist:.3f}) "
            f"after recent score change ({seconds_since_score:.0f}s ago). "
            "Market over-valuing a short run."
        )
        trade_action = "FADE. Buy the FAVORITE — underdog overpriced."
    return Signal(
        case_id="C6_FAVORITE_LONGSHOT",
        direction="fade",
        title=title,
        reason=reason,
        trade_action=trade_action,
        confidence=confidence,
        details=details,
    )


CASE_DETECTORS = {
    "C1_BREAKPOINT_CONTEXT": case_1_breakpoint,
    "C2_SCORE_DRIVEN": case_2_score_driven,
    "C3_TIMEBREAK_NOISE": case_3_timebreak,
    "C4_VOLATILITY_ADJUSTED": case_4_volatility,
    "C5_MARKOV_FAIRVALUE": case_5_markov,
    "C6_FAVORITE_LONGSHOT": case_6_favorite_longshot,
}


def run_all_cases(s: MarketSnapshot) -> List[Signal]:
    if market_is_near_resolved(s.current_price):
        return []
    out = []
    for cid, fn in CASE_DETECTORS.items():
        if cid not in ENABLED_CASES:
            continue
        try:
            sig = fn(s)
            if sig:
                out.append(sig)
        except Exception as e:
            print(f"  ! case {cid} error: {e}")
    return out


# ============================================================================
# DISCOVERY — SINGLES ONLY
# ============================================================================

def discover_tennis_markets_all() -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    with GammaClient() as gamma:
        all_markets = discover_tennis_markets(
            gamma, market_types={"moneyline"}, matches_only=True,
        )
        for m in all_markets:
            text = f"{m.question} {m.slug}".lower()
            if "doubles" in text or "double" in text:
                continue
            tour = None
            if "atp" in text or "masters" in text or "1000" in text or "finals" in text:
                tour = "ATP"
            elif "wta" in text:
                tour = "WTA"
            if tour is None:
                continue
            out.append({"market": m, "tour": tour})
    return out


# ============================================================================
# FAVORITE / UNDERDOG + NTFY HELPERS
# ============================================================================

def _identify_favorite_underdog(
    s: MarketSnapshot,
) -> Tuple[Optional[str], Optional[str]]:
    outcomes = list(s.outcomes or [])
    if len(outcomes) != 2:
        return None, None
    p0 = p1 = None
    if isinstance(s.current_price, (int, float)):
        try:
            p0 = float(s.current_price)
            p1 = 1.0 - p0
        except Exception:
            pass
    if s.role == "underdog" and p0 is not None and p1 is not None and p0 > p1:
        p0, p1 = p1, p0
    if p0 is None or p1 is None:
        return None, None
    if p0 >= p1:
        return outcomes[0], outcomes[1]
    return outcomes[1], outcomes[0]


def _discreet_role_tag(s: MarketSnapshot) -> str:
    fav, dog = _identify_favorite_underdog(s)
    if not fav or not dog:
        return ""
    def _short(name: str, limit: int = 22) -> str:
        name = str(name).strip()
        return name if len(name) <= limit else name[:limit-1] + "…"
    return f"fav: {_short(fav)} | dog: {_short(dog)}"


def format_band_touch_ntfy(s: MarketSnapshot, c6_signal: Optional[Signal] = None) -> Tuple[str, str]:
    """Build a clean ntfy message for a band touch."""
    fav, dog = _identify_favorite_underdog(s)
    role_line = _discreet_role_tag(s)

    touch_emoji = "🔴" if s.touch == "upper" else "🔵"
    title = f"{touch_emoji} BAND TOUCH — {s.tour} — {s.touch.upper()}"

    lines = [
        f"Tour:     {s.tour}",
        f"Match:    {s.question}",
        f"Score:    {s.score_line or 'N/A'}",
        f"Server:   {s.server or 'N/A'}",
        "",
        f"Price:    {s.current_price:.4f}",
        f"Touch:    {s.touch.upper()} BAND",
    ]
    if s.bands:
        lines.append(
            f"Bands:    up={s.bands['upper']:.4f}  mid={s.bands['middle']:.4f}  lo={s.bands['lower']:.4f}"
        )
    if role_line:
        lines += ["", f"Roles:    {role_line}"]

    # C6 information
    if c6_signal:
        lines += [
            "",
            ">>> C6 FAVORITE/LONGSHOT TRIGGERED <<<",
            f"C6 Title:   {c6_signal.title}",
            f"C6 Action:  {c6_signal.trade_action}",
            f"C6 Conf:    {c6_signal.confidence}",
        ]
    else:
        lines += ["", "C6:        not triggered"]

    lines += ["", f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"]
    body = "\n".join(lines)
    return title, body


# ============================================================================
# SCANNER
# ============================================================================

class TennisBollingerScanner:
    def __init__(self):
        self.alert_state: Dict[str, MarketAlert] = {}
        self.last_score: Dict[str, str] = {}
        self.last_score_change_ts: Dict[str, datetime] = {}
        self.last_point_time: Dict[str, datetime] = {}
        self.favorite_role: Dict[str, str] = {}
        self.scan_count = 0
        self.live_client: Optional[LiveTennisClient] = None

        self._live_matches_cache: List = []
        self._live_matches_ts: Optional[datetime] = None
        self._view_cache: Dict[str, tuple] = {}

    def initialize(self):
        print(f"[{datetime.now()}] initialize() entered")
        key = (LIVETENNIS_API_KEY or "").strip()
        if not key:
            raise ValueError("LIVETENNIS_API_KEY not set.")
        self.live_client = LiveTennisClient(api_key=key)
        print(f"[{datetime.now()}] Signal Engine initialized (LIVE-only, SINGLES only).")
        print(f"[{datetime.now()}] ntfy mode: BAND TOUCH only (+ C6 note)")
        print(f"[{datetime.now()}] relay URL: {PA_RELAY_URL}")
        print(f"[{datetime.now()}] poll interval: {POLL_INTERVAL_SECONDS}s  "
              f"live cache: {LIVE_MATCHES_CACHE_SECONDS}s")

    def _get_live_matches(self, force: bool = False) -> List:
        now = datetime.now()
        use_cache = (
            not force
            and self._live_matches_ts is not None
            and (now - self._live_matches_ts).total_seconds() < LIVE_MATCHES_CACHE_SECONDS
            and len(self._live_matches_cache) > 0
        )
        if use_cache:
            age = (now - self._live_matches_ts).total_seconds()
            print(f"  [match] using cached live list "
                  f"({len(self._live_matches_cache)} matches, age {age:.0f}s)")
            return self._live_matches_cache
        try:
            fresh = self.live_client.live_matches()
            self._live_matches_cache = fresh if fresh is not None else []
            self._live_matches_ts = now
            print(f"  [match] fetched {len(self._live_matches_cache)} live matches")
        except Exception as e:
            print(f"  ! live_matches() failed: {e}")
        return self._live_matches_cache

    def _get_view(self, market, match):
        slug = getattr(market, "slug", None) or "unknown"
        now = datetime.now()
        cached = self._view_cache.get(slug)
        if cached is not None:
            ts, view = cached
            if (now - ts).total_seconds() < VIEW_CACHE_SECONDS:
                return view
        try:
            view = build_view(market, match)
        except Exception as e:
            print(f"  ! build_view error for {slug}: {e}")
            view = None
        self._view_cache[slug] = (now, view)
        return view

    def cooldown_ok_band(self, slug: str) -> bool:
        key = f"{slug}::BAND_TOUCH"
        st = self.alert_state.get(key)
        if st is None or st.last_alert_time is None:
            return True
        elapsed = (datetime.now() - st.last_alert_time).total_seconds()
        return elapsed >= BAND_TOUCH_COOLDOWN_SECONDS

    def record_band_alert(self, slug: str):
        key = f"{slug}::BAND_TOUCH"
        self.alert_state[key] = MarketAlert(
            market_slug=key,
            last_alert_time=datetime.now(),
            last_alert_type="BAND_TOUCH",
        )

    def send_band_touch_alert(self, s: MarketSnapshot, c6_signal: Optional[Signal]):
        """Only place that sends ntfy messages."""
        if not self.cooldown_ok_band(s.market_slug):
            print(f"  [ntfy] cooldown active for {s.market_slug}, skipping")
            return

        title, body = format_band_touch_ntfy(s, c6_signal)

        print("\n" + "=" * 68)
        print(f"  BAND TOUCH ALERT → ntfy")
        print(f"  {title}")
        print("=" * 68)
        print(body)
        print("=" * 68 + "\n")

        ok = RELAY.send(
            title=title,
            body=body,
            priority="high",
            tags="chart_with_upwards_trend" if s.touch == "upper" else "chart_with_downwards_trend",
        )

        if ok:
            self.record_band_alert(s.market_slug)
        else:
            print(f"  ! relay delivery failed for band touch — will retry next cycle")

    def build_snapshot(self, item) -> Optional[MarketSnapshot]:
        market = item["market"]
        match = item["match"]
        slug = market.slug

        token_id = extract_token_id(market)
        if not token_id:
            print(f"  [skip] {slug}: no token_id")
            return None

        df = fetch_price_history(token_id, hours_back=HISTORY_HOURS)
        if df is None or df.empty:
            print(f"  [skip] {slug}: no usable history")
            return None

        closes = df["close"]
        if len(closes) < BOLLINGER_PERIOD:
            print(f"  [skip] {slug}: only {len(closes)} closes")
            return None

        if closes.tail(BOLLINGER_PERIOD).nunique() <= 1:
            print(f"  [skip] {slug}: flat window")
            return None

        bands = calculate_bollinger_bands(closes)
        if bands is not None and abs(bands["upper"] - bands["lower"]) < 1e-9:
            print(f"  [skip] {slug}: bands collapsed")
            return None

        last_price = float(closes.iloc[-1])
        touch = detect_band_touch(last_price, bands)

        if market_is_near_resolved(last_price):
            print(f"  [skip] {slug}: near-resolved (px={last_price:.4f})")
            return None

        if bands is not None and not pd.isna(bands["upper"]) and not pd.isna(bands["lower"]):
            px = last_price
            up, lo, mid = bands["upper"], bands["lower"], bands["middle"]
            dist_up_pct = (up - px) / px * 100.0
            dist_lo_pct = (px - lo) / px * 100.0
            dist_mid_pct = (px - mid) / mid * 100.0
            band_width_pct = (up - lo) / mid * 100.0 if mid else 0.0
            touch_tag = touch if touch else "-"
            print(
                f"  \'' {slug[:45]:<45}  "
                f"px={px:.4f}  "
                f"to_up={dist_up_pct:+.2f}%  "
                f"to_lo={dist_lo_pct:+.2f}%  "
                f"vs_mid={dist_mid_pct:+.2f}%  "
                f"width={band_width_pct:.2f}%  "
                f"touch={touch_tag}"
            )

        score_line = ""
        bp_flag = False
        server = None
        receiver = None

        view = self._get_view(market, match)
        if view is not None:
            score_line = getattr(view, "score_line", "") or ""
            bp_flag = bool(getattr(view, "break_point", False))
            server_raw = getattr(view, "server", None)
            outcomes = list(getattr(market, "outcomes", []) or [])
            if isinstance(server_raw, int) and 0 <= server_raw < len(outcomes):
                server = outcomes[server_raw]
            elif isinstance(server_raw, str):
                if server_raw.isdigit() and len(outcomes) > 0:
                    idx = int(server_raw)
                    if 0 <= idx < len(outcomes):
                        server = outcomes[idx]
                else:
                    server = server_raw
            if server and len(outcomes) == 2:
                receiver = outcomes[1] if outcomes[0] == server else outcomes[0]

        prev = self.last_score.get(slug)
        if prev is None:
            self.last_score[slug] = score_line
            self.last_point_time[slug] = datetime.now()
        elif prev != score_line:
            self.last_score[slug] = score_line
            self.last_score_change_ts[slug] = datetime.now()
            self.last_point_time[slug] = datetime.now()

        role = self.favorite_role.get(slug, "unknown")
        if role == "unknown":
            prices = getattr(market, "prices", None)
            if isinstance(prices, (list, tuple)) and len(prices) >= 2:
                try:
                    p0, p1 = float(prices[0]), float(prices[1])
                    role = "favorite" if p0 >= p1 else "underdog"
                    self.favorite_role[slug] = role
                except Exception:
                    pass

        markov = None
        if "C5_MARKOV_FAIRVALUE" in ENABLED_CASES:
            markov = estimate_markov_fair_prob(score_line, server, market.question)

        return MarketSnapshot(
            market_slug=slug,
            question=market.question,
            tour=item.get("tour", "UNKNOWN"),
            outcomes=list(getattr(market, "outcomes", []) or []),
            token_id=token_id,
            current_price=last_price,
            bands=bands,
            touch=touch,
            score_line=score_line,
            last_score_change_ts=self.last_score_change_ts.get(slug),
            break_point_flag=bp_flag,
            server=server,
            receiver=receiver,
            last_point_time=self.last_point_time.get(slug),
            recent_prices=[float(x) for x in closes.tail(BOLLINGER_PERIOD * 2).tolist()],
            role=role,
            markov_fair_prob=markov,
        )

    def match_markets_to_live(self, discovered) -> List:
        live = self._get_live_matches()
        if len(live) == 0:
            print("  [match] empty live list → forcing refresh")
            live = self._get_live_matches(force=True)

        singles = []
        for m in live:
            if isinstance(m, dict):
                is_doubles = m.get("is_doubles", False)
                draw = (m.get("draw") or "").lower()
                if is_doubles or draw == "doubles":
                    continue
                singles.append(m)
            else:
                singles.append(m)

        print(f"  [match] LiveTennis: {len(live)} total → {len(singles)} singles")

        for m in singles[:8]:
            if isinstance(m, dict):
                players = m.get("players") or {}
                p1 = p2 = "?"
                if isinstance(players, dict):
                    p1_data = players.get("p1") or players.get("player1") or {}
                    p2_data = players.get("p2") or players.get("player2") or {}
                    if isinstance(p1_data, dict):
                        p1 = p1_data.get("name") or p1_data.get("full_name") or str(p1_data)[:40]
                    if isinstance(p2_data, dict):
                        p2 = p2_data.get("name") or p2_data.get("full_name") or str(p2_data)[:40]
                tour = m.get("tour") or m.get("tournament") or m.get("tier") or "?"
                print(f"    • [{tour}] {p1} vs {p2}")
            else:
                print(f"    • {m}")

        paired = []
        unmatched = 0
        for entry in discovered:
            m = entry["market"]
            tour = entry["tour"]
            try:
                d = match_market(m, singles)
                if d and getattr(d, "match", None):
                    paired.append({
                        "market": m,
                        "match": d.match,
                        "confidence": getattr(d, "confidence", None),
                        "tour": tour,
                    })
                    print(f"  [match OK] {m.slug}")
                else:
                    unmatched += 1
                    print(f"  [no match] {m.slug}")
            except Exception as e:
                unmatched += 1
                print(f"  [match error] {m.slug}: {e}")

        print(f"  [match] result → {len(paired)} paired, {unmatched} unmatched")
        return paired

    def scan_once(self):
        self.scan_count += 1
        print(f"\n[{datetime.now()}] scan start (cycle {self.scan_count})")
        discovered = discover_tennis_markets_all()
        print(f"  discovered (singles only): {len(discovered)}")
        if not discovered:
            return
        paired = self.match_markets_to_live(discovered)
        print(f"  matched live: {len(paired)}")

        for item in paired:
            s = self.build_snapshot(item)
            if s is None:
                continue
            if self.scan_count < 2:
                continue

            # Only act when there is a band touch
            if s.touch is None:
                continue

            # Run cases only to detect C6 (and for console logging if desired)
            signals = run_all_cases(s)
            c6_signal = None
            for sig in signals:
                if sig.case_id == "C6_FAVORITE_LONGSHOT":
                    c6_signal = sig
                    break

            # Send the single band-touch ntfy
            self.send_band_touch_alert(s, c6_signal)

    def run(self):
        self.initialize()
        while True:
            try:
                self.scan_once()
            except KeyboardInterrupt:
                print("\nStopped.")
                return
            except Exception as e:
                print(f"! scan error: {e}")
            print(f"[{datetime.now()}] sleeping {POLL_INTERVAL_SECONDS}s")
            time.sleep(POLL_INTERVAL_SECONDS)


# ============================================================================
# FLASK WRAPPER
# ============================================================================

import threading
from flask import Flask

app = Flask(__name__)
_scanner_thread: Optional[threading.Thread] = None


def _start_scanner_once():
    global _scanner_thread
    if _scanner_thread is not None and _scanner_thread.is_alive():
        return

    def _wrapped_run():
        import sys, traceback
        try:
            scanner = TennisBollingerScanner()
            scanner.run()
        except Exception as e:
            print(f"!! SCANNER THREAD CRASHED: {e}", file=sys.stderr)
            traceback.print_exc()

    _scanner_thread = threading.Thread(target=_wrapped_run, daemon=True)
    _scanner_thread.start()
    print(f"[{datetime.now()}] Scanner thread started")


@app.route("/")
def root():
    _start_scanner_once()
    return "Tennis scanner is running (band-touch ntfy only)", 200


@app.route("/scanner-status")
def scanner_status():
    if _scanner_thread is None:
        return {"scanner": "not started"}, 500
    if not _scanner_thread.is_alive():
        return {"scanner": "dead"}, 500
    return {"scanner": "alive"}, 200


@app.route("/health")
def health():
    _start_scanner_once()
    alive = _scanner_thread is not None and _scanner_thread.is_alive()
    return {"status": "ok" if alive else "starting",
            "scanner_thread_alive": alive}, 200


_start_scanner_once()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
