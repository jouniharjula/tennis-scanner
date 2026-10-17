#!/usr/bin/env python3


#!/usr/bin/env python3
import sys
# Render runs us without a TTY → Python block-buffers stdout and logs vanish.
# Force line-buffering + reconfigure stderr so print() is always visible.
for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(line_buffering=True)
    except Exception:
        pass

"""
Deribit Options & Kraken Supply/Demand Trade Map Scanner  (DUAL-SYSTEM EDITION)
================================================================================
Signal-only scanner. Emits ntfy alerts for BB-wick entries and composite
threshold crossings across two independent systems:

  SYSTEM A - SHORT_TERM   : BB5 fade, 4H mean-reversion, funding flushes
                            weights: TM=0.35 COMP=0.45 DVOL=0.10 4HV=0.10
                            aggressive GEX, funding-sensitive, range-favoring
                            cooldown 4h, thresholds 80/20

  SYSTEM B - POSITION     : Daily BB, S/D structure, max-pain pull, GEX regime
                            weights: TM=0.65 COMP=0.15 DVOL=0.10 4HV=0.10
                            conservative GEX, skew-emphasized, trend-favoring
                            cooldown 24h, thresholds 85/15

Both share: Kraken fetchers, Deribit analyzer, S/D engine, funding cache,
RV engine, regime detector, ADX/ATR, momentum, contrarian.

No order placement, no position tracking.
- _QUIET_MODE = True            -> console/ntfy gated off except monitor
- Only allowed output: TM/COMP monitor at 15min, showing BOTH systems
================================================================================
"""

from collections import defaultdict
from datetime import datetime, timedelta, timezone
import math
import time
import requests
import json
import os
import threading
import random
import itertools
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple, Callable
import urllib.request
import urllib.error
from flask import Flask


# ==============================================================================
# MODIFICATION FLAGS..
# ==============================================================================
_QUIET_MODE = True
TM_COMP_MONITOR_ENABLED = True
TM_COMP_MONITOR_INTERVAL_SEC = 900
TM_COMP_MONITOR_TITLE = "TM/COMP Monitor (Dual System)"
TM_COMP_MONITOR_PRIORITY = "default"
TM_COMP_MONITOR_TAGS = ["bar_chart"]


def _quiet_print(*args, **kwargs):
    if not _QUIET_MODE:
        print(*args, **kwargs)


# ==============================================================================
# CONFIGURATION
# ==============================================================================
NTFY_TOPIC = os.environ.get("NTFY_TOPIC", "trademap-jl-7x9k2")
NTFY_BASE_URL = os.environ.get("NTFY_BASE_URL", "https://ntfy.sh")
NTFY_USERNAME = os.environ.get("NTFY_USERNAME", "")
NTFY_PASSWORD = os.environ.get("NTFY_PASSWORD", "")

MAX_NTFY_LENGTH = 3900
NTFY_MIN_GAP_SEC = 1.5
NTFY_INTER_CHUNK_DELAY = 2.0
NTFY_INTER_COIN_DELAY = 1.5
NTFY_MAX_ATTEMPTS = 3
NTFY_CIRCUIT_BREAKER_THRESHOLD = 300.0

# ============================================================================
# RELAY CONFIGURATION (exact same as TENNIS.py — same PA webapp)
# ============================================================================
PA_RELAY_URL = os.environ.get(
    "PA_RELAY_URL",
    "https://jharjula.pythonanywhere.com/relay",
)

# ==============================================================================
# SYSTEM PROFILES — the core of the dual-system design
# ==============================================================================
@dataclass
class SystemProfile:
    name: str
    weight_tm: float
    weight_comp: float
    weight_daily_vol: float
    weight_4h_vol: float
    gex_short_gamma: float
    gex_long_gamma: float
    gex_transitional: float
    funding_mult_extreme: float
    funding_mult_mild: float
    funding_dampen_extreme: float
    funding_dampen_mild: float
    regime_mult_strong_trend: float
    regime_mult_transitional: float
    regime_mult_range: float
    wick_boost_max: float
    skew_boost_max: float
    rv_bonus_3: float
    rv_bonus_2: float
    rv_penalty_3: float
    rv_penalty_2: float
    threshold_high: float
    threshold_low: float
    cooldown_sec: int
    bb5_upper_final_max: float
    bb5_lower_final_min: float
    timeframes: List[str] = field(default_factory=list)
    entry_rule: str = ""


SHORT_TERM = SystemProfile(
    name="SHORT",
    weight_tm=0.35, weight_comp=0.45,
    weight_daily_vol=0.10, weight_4h_vol=0.10,
    gex_short_gamma=1.35, gex_long_gamma=0.80, gex_transitional=1.00,
    funding_mult_extreme=1.20, funding_mult_mild=1.05,
    funding_dampen_extreme=0.85, funding_dampen_mild=0.95,
    regime_mult_strong_trend=0.75,
    regime_mult_transitional=0.95,
    regime_mult_range=1.15,
    wick_boost_max=7.0,
    skew_boost_max=5.0,
    rv_bonus_3=+10.0, rv_bonus_2=+5.0,
    rv_penalty_3=-10.0, rv_penalty_2=-5.0,
    threshold_high=80.0, threshold_low=20.0,
    cooldown_sec=4 * 3600,
    bb5_upper_final_max=15.0, bb5_lower_final_min=85.0,
    timeframes=["4h_bb", "15m_vol", "funding_change"],
    entry_rule="bb5_fade",
)

POSITION = SystemProfile(
    name="POSITION",
    weight_tm=0.65, weight_comp=0.15,
    weight_daily_vol=0.10, weight_4h_vol=0.10,
    gex_short_gamma=1.15, gex_long_gamma=0.90, gex_transitional=1.00,
    funding_mult_extreme=1.10, funding_mult_mild=1.02,
    funding_dampen_extreme=0.92, funding_dampen_mild=0.98,
    regime_mult_strong_trend=0.55,
    regime_mult_transitional=0.85,
    regime_mult_range=1.05,
    wick_boost_max=5.0,
    skew_boost_max=7.0,
    rv_bonus_3=+5.0, rv_bonus_2=+2.5,
    rv_penalty_3=-5.0, rv_penalty_2=-2.5,
    threshold_high=85.0, threshold_low=15.0,
    cooldown_sec=24 * 3600,
    bb5_upper_final_max=25.0, bb5_lower_final_min=75.0,
    timeframes=["daily_bb", "8h_bb", "sd_levels", "max_pain", "gex_regime"],
    entry_rule="structure_position",
)

SYSTEMS: Dict[str, SystemProfile] = {
    "SHORT": SHORT_TERM,
    "POSITION": POSITION,
}

SHARED_WEIGHT_PIERCES = 0.5
SHARED_WEIGHT_DEPARTURE = 0.3
SHARED_WEIGHT_RECENCY = 0.1
SHARED_WEIGHT_CLUSTER = 0.1
RV_LOOKBACK_DAYS = 180
RV_MIN_COMMON_CANDLES = 120
RV_Z_ALERT_THRESHOLD_1 = 2.0
RV_Z_ALERT_THRESHOLD_2 = 4.0
RV_VOTE_MARGIN_3 = 3
RV_VOTE_MARGIN_2 = 2
RV_CACHE_MAX_AGE_SEC = 3600


# ==============================================================================
# 4H BOLLINGER BAND CONFIG (shared)
# ==============================================================================
BB5_PERIOD = 20
BB5_STDDEV = 2.0
BB5_CHECK_INTERVAL = 900
BB5_ALERT_COOLDOWN = 1800
BB5_OHLC_INTERVAL = 240

BB5_SYMBOLS = {
    "BTC": "XXBTZUSD",
    "ETH": "XETHZUSD",
    "SOL": "SOLUSD",
    "XMR": "XXMRZUSD",
}
_bb5_last_alert = {}


# ==============================================================================
# RELATIVE VALUE ENGINE (shared)
# ==============================================================================
RELATIVE_VALUE_ENABLED = True
_rv_cache = {}
_rv_cache_lock = threading.Lock()
_rv_cache_ts = 0.0


def _rv_mean(data):
    return sum(data) / len(data) if data else 0.0


def _rv_covariance(x, y):
    n = len(x)
    if n < 2:
        return 0.0
    mx, my = _rv_mean(x), _rv_mean(y)
    return sum((x[i] - mx) * (y[i] - my) for i in range(n)) / (n - 1)


def _rv_variance(x):
    return _rv_covariance(x, x)


def _rv_std_dev(x):
    return math.sqrt(_rv_variance(x))


def _rv_calculate_z_score(logs1, logs2, beta, current_p1, current_p2):
    historical_spread = [logs1[i] - (beta * logs2[i]) for i in range(len(logs1))]
    spread_mean = _rv_mean(historical_spread)
    spread_std = _rv_std_dev(historical_spread)
    if spread_std == 0:
        return 0.0
    current_spread = math.log(current_p1) - beta * math.log(current_p2)
    return (current_spread - spread_mean) / spread_std


def _rv_fetch_latest_ticker(pair_code):
    _kraken_rate_limit()
    url = f"{KRAKEN_BASE_URL}/0/public/Ticker"
    try:
        resp = requests.get(url, params={"pair": pair_code}, timeout=10, proxies=_PROXIES)
        data = resp.json()
        if data.get("error"):
            return None
        k = list(data["result"].keys())[0]
        return float(data["result"][k]["c"][0])
    except Exception:
        return None


def _rv_build_pairs_for_symbols(symbols):
    price_data = {}
    for symbol in symbols:
        if symbol in SKIP_SYMBOLS:
            continue
        pair_code = BB5_SYMBOLS.get(symbol)
        if not pair_code:
            continue
        candles = fetch_kraken_ohlc_bb(pair_code, interval=BB5_OHLC_INTERVAL, limit=200)
        if candles and len(candles) >= RV_MIN_COMMON_CANDLES:
            price_data[symbol] = {c["time"]: c["close"] for c in candles}
        time.sleep(0.3)
    if len(price_data) < 2:
        return []
    labels = list(price_data.keys())
    pairs = []
    for a1, a2 in itertools.combinations(labels, 2):
        d1, d2 = price_data[a1], price_data[a2]
        common = sorted(set(d1.keys()) & set(d2.keys()))
        if len(common) < RV_MIN_COMMON_CANDLES:
            continue
        c1 = [d1[t] for t in common]
        c2 = [d2[t] for t in common]
        l1 = [math.log(p) for p in c1]
        l2 = [math.log(p) for p in c2]
        v2 = _rv_variance(l2)
        if v2 == 0:
            continue
        beta = _rv_covariance(l1, l2) / v2
        pairs.append({"asset1": a1, "asset2": a2, "pair": f"{a1}/{a2}",
                      "beta": beta, "logs1": l1, "logs2": l2})
    return pairs, price_data


def _rv_compute_live_z_scores(pairs, price_data):
    live_z_map = {}
    for p in pairs:
        a1, a2 = p["asset1"], p["asset2"]
        c1 = BB5_SYMBOLS.get(a1)
        c2 = BB5_SYMBOLS.get(a2)
        if not c1 or not c2:
            continue
        p1 = _rv_fetch_latest_ticker(c1)
        p2 = _rv_fetch_latest_ticker(c2)
        if p1 is None or p2 is None:
            continue
        live_z_map[p["pair"]] = _rv_calculate_z_score(
            p["logs1"], p["logs2"], p["beta"], p1, p2
        )
        time.sleep(0.2)
    return live_z_map


def _rv_compute_relative_value_rank(live_z_map):
    if not live_z_map:
        return {}
    votes = defaultdict(list)
    over_under = defaultdict(lambda: [0, 0])
    for pair_name, z in live_z_map.items():
        if "/" not in pair_name:
            continue
        a1, a2 = pair_name.split("/", 1)
        votes[a1].append(z)
        votes[a2].append(-z)
        if z > 0:
            over_under[a1][0] += 1
            over_under[a2][1] += 1
        elif z < 0:
            over_under[a1][1] += 1
            over_under[a2][0] += 1
    summary = {}
    for coin, contribs in votes.items():
        n = len(contribs)
        if n == 0:
            continue
        mz = sum(contribs) / n
        sc = sorted(contribs)
        med = sc[n // 2] if n % 2 else (sc[n // 2 - 1] + sc[n // 2]) / 2.0
        over, under = over_under.get(coin, [0, 0])
        comp = 0.5 * mz + 0.5 * med
        net = over - under
        verdict = "OVERVALUED" if comp > 0 else ("UNDERVALUED" if comp < 0 else "FAIR")
        summary[coin] = {
            "mean_z": mz, "median_z": med, "composite": comp,
            "pairs": n, "over": over, "under": under,
            "net_votes": net, "verdict": verdict,
        }
    return summary


def _rv_refresh_cache():
    global _rv_cache, _rv_cache_ts
    if not RELATIVE_VALUE_ENABLED:
        return
    symbols = list(BB5_SYMBOLS.keys())
    if len(symbols) < 2:
        return
    try:
        result = _rv_build_pairs_for_symbols(symbols)
        if not result or len(result) < 2:
            return
        pairs, price_data = result
        if not pairs:
            return
        live_z = _rv_compute_live_z_scores(pairs, price_data)
        if not live_z:
            return
        summary = _rv_compute_relative_value_rank(live_z)
        with _rv_cache_lock:
            _rv_cache = summary
            _rv_cache_ts = time.time()
    except Exception as e:
        _quiet_print(f"  [RV] Cache refresh failed: {e}")


def _rv_get_relative_value(symbol):
    with _rv_cache_lock:
        return _rv_cache.get(symbol)


def apply_relative_value_adjustment(symbol, score, bb5_threshold_type,
                                    profile: SystemProfile):
    if not RELATIVE_VALUE_ENABLED:
        return score, 0.0, "RV disabled"
    if symbol not in BB5_SYMBOLS:
        return score, 0.0, "not BB5 symbol"
    rv = _rv_get_relative_value(symbol)
    if rv is None:
        return score, 0.0, "no RV data"
    verdict = rv["verdict"]
    over, under = rv["over"], rv["under"]
    if verdict == "OVERVALUED":
        margin = over - under
    elif verdict == "UNDERVALUED":
        margin = under - over
    else:
        return score, 0.0, "FAIR"

    adj = 0.0
    if bb5_threshold_type == "UPPER":
        if verdict == "OVERVALUED" and margin >= RV_VOTE_MARGIN_3:
            adj = profile.rv_penalty_3
        elif verdict == "OVERVALUED" and margin >= RV_VOTE_MARGIN_2:
            adj = profile.rv_penalty_2
        elif verdict == "UNDERVALUED" and margin >= RV_VOTE_MARGIN_3:
            adj = profile.rv_bonus_3
        elif verdict == "UNDERVALUED" and margin >= RV_VOTE_MARGIN_2:
            adj = profile.rv_bonus_2
    elif bb5_threshold_type == "LOWER":
        if verdict == "UNDERVALUED" and margin >= RV_VOTE_MARGIN_3:
            adj = profile.rv_bonus_3
        elif verdict == "UNDERVALUED" and margin >= RV_VOTE_MARGIN_2:
            adj = profile.rv_bonus_2
        elif verdict == "OVERVALUED" and margin >= RV_VOTE_MARGIN_3:
            adj = profile.rv_penalty_3
        elif verdict == "OVERVALUED" and margin >= RV_VOTE_MARGIN_2:
            adj = profile.rv_penalty_2

    if adj == 0.0:
        return score, 0.0, f"{verdict} (margin {margin})"
    return clamp(score + adj, 0.0, 100.0), adj, f"{verdict} m{margin} -> {adj:+.1f}"


def _rv_worker():
    if not RELATIVE_VALUE_ENABLED:
        return
    time.sleep(15)
    while True:
        try:
            _rv_refresh_cache()
        except Exception as e:
            _quiet_print(f"  [RV] worker error: {e}")
        time.sleep(RV_CACHE_MAX_AGE_SEC)


# ==============================================================================
# HELPERS
# ==============================================================================
KRAKEN_BASE_URL = "https://api.kraken.com"
_KRAKEN_PAIR_HEALTH: Dict[str, bool] = {}
KRAKEN_MIN_INTERVAL_SEC = 2.5
KRAKEN_JITTER_SEC = 0.4
_kraken_last_call_ts = 0.0
_kraken_rate_lock = threading.Lock()


def _detect_pyaw_proxy():
    forced = os.environ.get("USE_PYAW_PROXY", "").strip()
    if forced == "1":
        return True
    if forced == "0":
        return False
    if os.environ.get("PYTHONANYWHERE_DOMAIN") or os.environ.get("PYTHONANYWHERE_SITE"):
        return True
    return False


_USE_PYAW_PROXY = _detect_pyaw_proxy()
_PROXIES = (
    {"http": "http://proxy.server:3128", "https": "http://proxy.server:3128"}
    if _USE_PYAW_PROXY else None
)


def _kraken_rate_limit():
    global _kraken_last_call_ts
    with _kraken_rate_lock:
        now = time.time()
        gap = now - _kraken_last_call_ts
        if gap < KRAKEN_MIN_INTERVAL_SEC:
            time.sleep(KRAKEN_MIN_INTERVAL_SEC - gap + random.uniform(0, KRAKEN_JITTER_SEC))
        _kraken_last_call_ts = time.time()


def clamp(v, lo=0.0, hi=1.0):
    return max(lo, min(hi, v))


def norm_pdf(x):
    return math.exp(-0.5 * x * x) / math.sqrt(2 * math.pi)


def bs_gamma(S, K, T, r, sigma):
    if T <= 0 or sigma <= 0 or S <= 0:
        return 0.0
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
    return norm_pdf(d1) / (S * sigma * math.sqrt(T))


def _fmt(x, digits=4):
    if x is None:
        return ""
    if isinstance(x, bool):
        return str(x)
    if isinstance(x, (int, float)):
        try:
            if math.isnan(x) or math.isinf(x):
                return ""
        except (TypeError, ValueError):
            pass
        return f"{x:.{digits}f}"
    return str(x)


# ==============================================================================
# NTFY
# ==============================================================================
_ntfy_last_post_ts = 0.0
_ntfy_circuit_open = False
_ntfy_send_lock = threading.Lock()


# ============================================================================
# RELAY CLIENT (exact same as TENNIS.py — same PA webapp)
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
             tags: str = "", target: str = "ntfy") -> bool:
        """target: 'ntfy' (default) or 'discord' — same PA endpoint."""
        self._wait_for_slot()

        payload = json.dumps({
            "title": title,
            "body": body,
            "priority": priority,
            "tags": tags,
            "target": target,
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


def _ntfy_throttle():
    global _ntfy_last_post_ts
    now = time.time()
    gap = now - _ntfy_last_post_ts
    if gap < NTFY_MIN_GAP_SEC:
        time.sleep(NTFY_MIN_GAP_SEC - gap)
    _ntfy_last_post_ts = time.time()


def _sanitize_ascii(text):
    if text is None:
        return ""
    if isinstance(text, bytes):
        text = text.decode("utf-8", "ignore")
    reps = {
        "\u2014": "-", "\u2013": "-", "\u2018": "'", "\u2019": "'",
        "\u201c": '"', "\u201d": '"', "\u2026": "...",
        "\u2192": "->", "\u2190": "<-", "\u2191": "^", "\u2193": "v",
        "\u00d7": "x", "\u0394": "d",
    }
    for k, v in reps.items():
        text = text.replace(k, v)
    return text.encode("ascii", "ignore").decode("ascii")


def _ntfy_post_send(title, text, priority="default", tags=None, force=False):
    """
    Deliver via PythonAnywhere relay (same endpoint / payload shape as TENNIS.py).
    """
    if _QUIET_MODE and not force:
        return None
    global _ntfy_circuit_open
    if _ntfy_circuit_open:
        _quiet_print("  [SKIP] ntfy circuit open")
        return None

    clean_title = _sanitize_ascii(title or "Trade Map")
    clean_body = _sanitize_ascii(text or "")
    if isinstance(tags, (list, tuple)):
        tags_str = _sanitize_ascii(",".join(str(t) for t in tags if t))
    else:
        tags_str = _sanitize_ascii(tags or "")

    try:
        _ntfy_throttle()
        ok = RELAY.send(
            title=clean_title,
            body=clean_body,
            priority=priority or "default",
            tags=tags_str,
        )
        if ok:
            return True
        _quiet_print("  [relay] delivery failed — will not open circuit (retry next cycle)")
        return None
    except Exception as e:
        _quiet_print(f"  [relay] unexpected error: {e}")
        return None


def send_ntfy_message(content, title="Trade Map", priority="default", tags=None,
                      system: Optional[str] = None, force=False):
    """If system is given, prefixes title with [SHORT] / [POSITION]."""
    if _QUIET_MODE and not force:
        return
    if not content:
        return
    content = _sanitize_ascii(content)
    if system:
        title = f"[{system}] {title}"

    with _ntfy_send_lock:
        if len(content) <= MAX_NTFY_LENGTH:
            _ntfy_post_send(title, content, priority=priority, tags=tags, force=force)
            return
        chunks = []
        cur = ""
        for line in content.split("\n"):
            if len(cur) + len(line) + 1 > MAX_NTFY_LENGTH:
                if cur:
                    chunks.append(cur.rstrip())
                    cur = line + "\n"
                else:
                    chunks.append(line[:MAX_NTFY_LENGTH])
                    cur = line[MAX_NTFY_LENGTH:] + "\n"
            else:
                cur += line + "\n"
        if cur.strip():
            chunks.append(cur.rstrip())
        for i, ch in enumerate(chunks, 1):
            t = title if len(chunks) == 1 else f"{title} ({i}/{len(chunks)})"
            _ntfy_post_send(t, ch, priority=priority, tags=tags, force=force)
            if i < len(chunks):
                time.sleep(NTFY_INTER_CHUNK_DELAY)


# ==============================================================================
# KRAKEN FETCHERS (shared)
# ==============================================================================
def fetch_kraken_ohlc(pair, interval=1440, days=360):
    if _KRAKEN_PAIR_HEALTH.get(pair) is False:
        return []
    _kraken_rate_limit()
    url = f"{KRAKEN_BASE_URL}/0/public/OHLC"
    since = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())
    params = {"pair": pair, "interval": interval, "since": since}
    try:
        resp = requests.get(url, params=params, timeout=10, proxies=_PROXIES)
        data = resp.json()
        if data.get("error"):
            _KRAKEN_PAIR_HEALTH[pair] = False
            return []
        if "result" in data and data["result"]:
            k = list(data["result"].keys())[0]
            out = []
            for c in data["result"][k]:
                out.append({
                    "time": c[0],
                    "time_str": datetime.fromtimestamp(c[0], tz=timezone.utc).strftime("%Y-%m-%d"),
                    "open": float(c[1]), "high": float(c[2]),
                    "low": float(c[3]), "close": float(c[4]),
                    "volume": float(c[6]),
                })
            _KRAKEN_PAIR_HEALTH[pair] = True
            return out
    except Exception as e:
        _quiet_print(f"Kraken OHLC error {pair}: {e}")
    return []


def fetch_kraken_ohlc_bb(pair, interval=240, limit=100):
    if _KRAKEN_PAIR_HEALTH.get(pair) is False:
        return []
    _kraken_rate_limit()
    url = f"{KRAKEN_BASE_URL}/0/public/OHLC"
    params = {"pair": pair, "interval": interval}
    try:
        resp = requests.get(url, params=params, timeout=15, proxies=_PROXIES)
        data = resp.json()
        if data.get("error"):
            _KRAKEN_PAIR_HEALTH[pair] = False
            return []
        if "result" not in data or not data["result"]:
            return []
        k = [kk for kk in data["result"].keys() if kk != "last"][0]
        out = []
        for c in data["result"][k]:
            out.append({
                "time": c[0], "open": float(c[1]), "high": float(c[2]),
                "low": float(c[3]), "close": float(c[4]), "volume": float(c[6]),
            })
        out.sort(key=lambda x: x["time"])
        _KRAKEN_PAIR_HEALTH[pair] = True
        return out[-limit:] if len(out) > limit else out
    except Exception as e:
        _quiet_print(f"Kraken OHLC error {pair}: {e}")
        return []


# ==============================================================================
# SYMBOL UNIVERSE
# ==============================================================================
ADDITIONAL_SYMBOLS = {
    "BTC": "XXBTZUSD",
    "ETH": "XETHZUSD",
    "SOL": "SOLUSD",
    "XMR": "XXMRZUSD",
}
KRAKEN_PAIR_MAP = {
    "BTC": "XXBTZUSD", "ETH": "XETHZUSD", "SOL": "SOLUSD",
}
for _s, _p in ADDITIONAL_SYMBOLS.items():
    KRAKEN_PAIR_MAP.setdefault(_s, _p)

SKIP_SYMBOLS = {
    s.strip().upper()
    for s in os.environ.get("SKIP_COINS", "").split(",")
    if s.strip()
}

# ==============================================================================
# SUPPLY / DEMAND ENGINE (shared)
# ==============================================================================
LOOKBACK_DAYS = 360
CLUSTER_TOLERANCE_PCT = 0.005
HIGH_QUALITY_SCORE_THRESHOLD = 65
CLOSE_THRESHOLD_PCT_DEFAULT = 0.00750
CLOSE_THRESHOLD_PCT_BY_COIN = {"BTC": 0.010, "ETH": 0.010, "SOL": 0.010,
                                "XMR": 0.020, "LTC": 0.020}


def _close_threshold_for(symbol):
    return CLOSE_THRESHOLD_PCT_BY_COIN.get(symbol, CLOSE_THRESHOLD_PCT_DEFAULT)


def calculate_cluster_density(target_price, other_levels, tol=CLUSTER_TOLERANCE_PCT):
    if not other_levels:
        return 0.0
    cnt = 0
    dist_sum = 0.0
    for op in other_levels:
        if abs(op - target_price) < 1e-10:
            continue
        dp = abs(op - target_price) / target_price
        if dp <= tol:
            cnt += 1
            dist_sum += dp
    if cnt == 0:
        return 0.0
    cnt_ratio = min(1.0, cnt / 5.0) * 60.0
    avg_d = dist_sum / cnt
    prox = max(0.0, 1.0 - avg_d / tol) * 40.0
    return min(100.0, cnt_ratio + prox)


def detect_supply_levels(candles):
    levels = []
    n = len(candles)
    for i in range(2, n - 3):
        ch = candles[i]["high"]
        if ch >= candles[i-1]["high"] and ch >= candles[i+1]["high"]:
            pierces = sum(1 for j in range(i + 1, min(i + 360, n))
                          if candles[j]["high"] >= ch)
            fe = min(i + 21, n)
            dep = ch - min(candles[j]["low"] for j in range(i + 1, fe)) if i + 1 < fe else 0.0
            levels.append({"price": ch, "time": candles[i]["time"],
                           "pierces": pierces, "departure": dep})
    dedup = []
    for lv in levels:
        found = False
        for d in dedup:
            if abs(lv["price"] - d["price"]) / d["price"] <= 0.002:
                d["pierces"] = max(d["pierces"], lv["pierces"])
                d["departure"] = max(d["departure"], lv["departure"])
                found = True
                break
        if not found:
            dedup.append(lv)
    return dedup


def detect_demand_levels(candles):
    levels = []
    n = len(candles)
    for i in range(2, n - 3):
        cl = candles[i]["low"]
        if cl <= candles[i-1]["low"] and cl <= candles[i+1]["low"]:
            pierces = sum(1 for j in range(i + 1, min(i + 360, n))
                          if candles[j]["low"] <= cl)
            fe = min(i + 21, n)
            dep = max(candles[j]["high"] for j in range(i + 1, fe)) - cl if i + 1 < fe else 0.0
            levels.append({"price": cl, "time": candles[i]["time"],
                           "pierces": pierces, "departure": dep})
    dedup = []
    for lv in levels:
        found = False
        for d in dedup:
            if abs(lv["price"] - d["price"]) / d["price"] <= 0.002:
                d["pierces"] = max(d["pierces"], lv["pierces"])
                d["departure"] = max(d["departure"], lv["departure"])
                found = True
                break
        if not found:
            dedup.append(lv)
    return dedup


def calculate_sd_score(level, spot, now_ts, others):
    p = level["pierces"]
    p_score = 100.0 if p <= 0 else (85 if p <= 2 else (70 if p <= 5 else (55 if p <= 10 else 25)))
    d_pct = level["departure"] / spot if spot > 0 else 0
    d_score = 100.0 if d_pct >= 0.10 else (80 if d_pct >= 0.05 else (60 if d_pct >= 0.02 else 20))
    age = (now_ts - level["time"]) / 86400.0
    r_score = 100.0 if age <= 1 else (85 if age <= 3 else (70 if age <= 7 else (40 if age <= 30 else 10)))
    c_score = calculate_cluster_density(level["price"], others)
    return min(100.0, p_score*0.5 + d_score*0.3 + r_score*0.1 + c_score*0.1)


def get_supply_demand_levels(symbol, spot):
    pair = KRAKEN_PAIR_MAP.get(symbol, f"{symbol}USD")
    candles = fetch_kraken_ohlc(pair)
    if not candles:
        return None, None
    now_t = candles[-1]["time"]
    rs = detect_supply_levels(candles)
    rd = detect_demand_levels(candles)
    sp = [s["price"] for s in rs]
    for s in rs:
        s["score"] = calculate_sd_score(s, spot, now_t, [p for p in sp if p != s["price"]])
    dp = [d["price"] for d in rd]
    for d in rd:
        d["score"] = calculate_sd_score(d, spot, now_t, [p for p in dp if p != d["price"]])
    vs = [s for s in rs if s["score"] > HIGH_QUALITY_SCORE_THRESHOLD and s["price"] >= spot]
    vd = [d for d in rd if d["score"] > HIGH_QUALITY_SCORE_THRESHOLD and d["price"] <= spot]
    fs = min(vs, key=lambda x: abs(x["price"] - spot)) if vs else None
    fd = min(vd, key=lambda x: abs(x["price"] - spot)) if vd else None
    return fs, fd


# ==============================================================================
# BB / WICK ENGINE (shared)
# ==============================================================================
BB_PERIOD = 20
BB_STD = 2.0
WICK_THRESHOLD = 1.8
SECONDS_8H = 8 * 3600


def calculate_bollinger_bands(candles, period=BB_PERIOD, mult=BB_STD):
    if len(candles) < period:
        return None, None, None
    closes = [c["close"] for c in candles[-period:]]
    sma = sum(closes) / period
    var = sum((x - sma) ** 2 for x in closes) / period
    std = var ** 0.5
    return sma + mult * std, sma, sma - mult * std


def get_candle_wick_info(candle):
    o, h, l, c = candle["open"], candle["high"], candle["low"], candle["close"]
    body = abs(c - o)
    if body == 0:
        return 1.0, 1.0
    return (h - max(o, c)) / body, (min(o, c) - l) / body


def aggregate_candles_8h_aligned(c4):
    if not c4:
        return []
    i = 0
    while i < len(c4) and c4[i]["time"] % SECONDS_8H != 0:
        i += 1
    trimmed = c4[i:]
    if len(trimmed) < BB_PERIOD + 2:
        return []
    if len(trimmed) % 2:
        trimmed = trimmed[:-1]
    if len(trimmed) % 2 or len(trimmed) < BB_PERIOD + 2:
        return []
    out = []
    for j in range(0, len(trimmed), 2):
        g = trimmed[j:j+2]
        if g[0]["time"] % SECONDS_8H != 0:
            return []
        out.append({
            "time": g[0]["time"], "open": g[0]["open"],
            "high": max(c["high"] for c in g),
            "low": min(c["low"] for c in g),
            "close": g[-1]["close"],
            "volume": sum(c.get("volume", 0.0) for c in g),
        })
    return out


def get_bb_wick_boost_for_timeframe(symbol, interval, touch_pts=10, wick_pts=5,
                                     label="BB", prefetched_4h=None):
    pair = KRAKEN_PAIR_MAP.get(symbol, f"{symbol}USD")
    if interval == 240:
        candles = prefetched_4h if prefetched_4h is not None else \
            fetch_kraken_ohlc_bb(pair, interval=240, limit=100)
    elif interval == 480:
        c4 = prefetched_4h if prefetched_4h is not None else \
            fetch_kraken_ohlc_bb(pair, interval=240, limit=100)
        if len(c4) < BB_PERIOD + 2:
            return 0.0, f"{label} insufficient"
        candles = aggregate_candles_8h_aligned(c4)
        if not candles:
            return 0.0, f"{label} aggregation skipped"
    elif interval == 1440:
        candles = fetch_kraken_ohlc_bb(pair, interval=1440, limit=100)
    else:
        return 0.0, f"{label} unsupported"

    if len(candles) < BB_PERIOD + 2:
        return 0.0, f"{label} insufficient"

    closed = candles[:-1]
    live = candles[-1]
    last = closed[-1]
    up, mid, lo = calculate_bollinger_bands(closed)
    if up is None:
        return 0.0, f"{label} BB unavailable"

    uw, lw = get_candle_wick_info(last)
    ua = uw > WICK_THRESHOLD
    la = lw > WICK_THRESHOLD

    if live["high"] >= up:
        if ua:
            return -float(touch_pts + wick_pts), f"-{touch_pts+wick_pts} {label} UBB+wick ({uw:.2f})"
        return -float(touch_pts), f"-{touch_pts} {label} UBB ({uw:.2f})"
    if live["low"] <= lo:
        if la:
            return float(touch_pts + wick_pts), f"+{touch_pts+wick_pts} {label} LBB+wick ({lw:.2f})"
        return float(touch_pts), f"+{touch_pts} {label} LBB ({lw:.2f})"
    return 0.0, f"{label} inside"


# ==============================================================================
# VOLUME ENGINE (shared)
# ==============================================================================
def compute_volume_vs_ma10(symbol, interval=1440):
    pair = ADDITIONAL_SYMBOLS.get(symbol)
    if not pair:
        return None
    candles = fetch_kraken_ohlc_bb(pair, interval=interval, limit=20)
    if not candles or len(candles) < 12:
        return None
    last = candles[-2]
    idx = len(candles) - 2
    start = idx - 9
    if start < 0:
        return None
    window = [c["volume"] for c in candles[start:idx+1]]
    if len(window) != 10:
        return None
    ma10 = sum(window) / 10.0
    if ma10 <= 0:
        return None
    pct = (last["volume"] / ma10) * 100.0
    uw, lw = get_candle_wick_info(last)
    ua, la = uw > WICK_THRESHOLD, lw > WICK_THRESHOLD
    if ua and la:
        tag = "BOTH"
    elif ua:
        tag = "UPPER"
    elif la:
        tag = "LOWER"
    else:
        tag = "NONE"
    pattern = detect_candle_patterns(last)
    return {
        "symbol": symbol, "prev_vol": last["volume"], "ma10": ma10,
        "pct_of_ma10": pct, "upper_wick": uw, "lower_wick": lw,
        "upper_active": ua, "lower_active": la,
        "wick_tag": tag, "pattern_tag": pattern["tag"],
    }


def compute_daily_volume_vs_ma10(symbol):
    return compute_volume_vs_ma10(symbol, interval=1440)


def compute_4h_volume_vs_ma10(symbol):
    return compute_volume_vs_ma10(symbol, interval=240)


DOJI_BODY_MAX_RANGE_PCT = 0.10
SS_WICK_BODY_MIN_RATIO = 2.0
SS_OPPOSITE_WICK_MAX_RATIO = 0.5
SS_WICK_MIN_RANGE_PCT = 0.50


def detect_candle_patterns(candle):
    try:
        o, h, l, c = float(candle["open"]), float(candle["high"]), \
                     float(candle["low"]), float(candle["close"])
    except (KeyError, TypeError, ValueError):
        return {"is_doji": False, "is_shooting_star": False, "is_hammer": False, "tag": "-"}
    tr = h - l
    if tr <= 0:
        return {"is_doji": False, "is_shooting_star": False, "is_hammer": False, "tag": "-"}
    body = abs(c - o)
    uw = h - max(o, c)
    lw = min(o, c) - l
    is_doji = (body / tr) <= DOJI_BODY_MAX_RANGE_PCT
    is_ss = (uw >= SS_WICK_MIN_RANGE_PCT * tr
             and uw >= SS_WICK_BODY_MIN_RATIO * max(body, 1e-12)
             and lw <= SS_OPPOSITE_WICK_MAX_RATIO * uw)
    is_hm = (lw >= SS_WICK_MIN_RANGE_PCT * tr
             and lw >= SS_WICK_BODY_MIN_RATIO * max(body, 1e-12)
             and uw <= SS_OPPOSITE_WICK_MAX_RATIO * lw)
    parts = []
    if is_ss: parts.append("SS")
    if is_hm: parts.append("HM")
    if is_doji: parts.append("DJ")
    return {
        "is_doji": is_doji, "is_shooting_star": is_ss, "is_hammer": is_hm,
        "tag": "+".join(parts) if parts else "-",
    }


# ==============================================================================
# EXTENDED COMPOSITE (shared) — pure price action + S/D
# ==============================================================================
def calculate_sd_component(symbol, spot, fs, fd):
    thr = _close_threshold_for(symbol)
    if fs and fd:
        ds = abs(fs["price"] - spot) / spot
        dd = abs(spot - fd["price"]) / spot
        sc = ds <= thr
        dc = dd <= thr
        if sc and dc:
            return 50.0, "between 1S/1D"
        if sc:
            return 40.0, "at 1S supply"
        if dc:
            return 60.0, "at 1D demand"
        return 50.0, "neutral"
    if fs:
        ds = abs(fs["price"] - spot) / spot
        if ds <= thr:
            return 40.0, "at 1S only"
        return 50.0, "only 1S, not close"
    if fd:
        dd = abs(spot - fd["price"]) / spot
        if dd <= thr:
            return 60.0, "at 1D only"
        return 50.0, "only 1D, not close"
    return 50.0, "no levels"


def calculate_composite_score_extended(symbol, spot):
    fs, fd = get_supply_demand_levels(symbol, spot)
    sd_score, sd_reason = calculate_sd_component(symbol, spot, fs, fd)

    pair = KRAKEN_PAIR_MAP.get(symbol, f"{symbol}USD")
    pref4h = fetch_kraken_ohlc_bb(pair, interval=240, limit=100)

    b4, r4 = get_bb_wick_boost_for_timeframe(symbol, 240, 10, 5, "4H", prefetched_4h=pref4h)
    bd, rd = get_bb_wick_boost_for_timeframe(symbol, 1440, 10, 5, "Daily")
    b8, r8 = get_bb_wick_boost_for_timeframe(symbol, 480, 10, 5, "8H", prefetched_4h=pref4h)

    bb_boost = b4 + bd + b8
    bb_reason = f"4H {b4:+.1f} | D {bd:+.1f} | 8H {b8:+.1f}"
    comp = clamp(sd_score + bb_boost, 0.0, 100.0)
    return comp, sd_score, sd_reason, bb_boost, bb_reason


# ==============================================================================
# REGIME DETECTOR (shared)
# ==============================================================================
REGIME_DETECTOR_ENABLED = True
REGIME_ADX_PERIOD = 14
REGIME_ADX_THRESHOLDS_BY_COIN = {
    "BTC": (16.0, 23.0), "ETH": (16.0, 23.0), "SOL": (18.0, 26.0),
}
REGIME_ADX_DEFAULT_RANGE = 18.0
REGIME_ADX_DEFAULT_TREND = 25.0
ATR_PERIOD_SHORT = 14
ATR_PERIOD_LONG = 50
ATR_RATIO_EXPANDING = 1.25
ATR_RATIO_COMPRESSED = 0.85


def _is_valid_candle(c):
    if not isinstance(c, dict):
        return False
    for k in ("open", "high", "low", "close"):
        v = c.get(k)
        if v is None:
            return False
        try:
            if math.isnan(v) or math.isinf(v) or v <= 0:
                return False
        except (TypeError, ValueError):
            return False
    return c["high"] >= c["low"]


def _compute_adx_daily(symbol, limit=100):
    pair = ADDITIONAL_SYMBOLS.get(symbol)
    if not pair:
        return None, "no pair"
    candles = fetch_kraken_ohlc_bb(pair, interval=1440, limit=limit)
    if not candles or len(candles) < REGIME_ADX_PERIOD * 3:
        return None, "insufficient"
    candles = [c for c in candles[:-1] if _is_valid_candle(c)]
    if len(candles) < REGIME_ADX_PERIOD * 3:
        return None, "insufficient after sanitize"

    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    closes = [c["close"] for c in candles]
    n = len(candles)
    period = REGIME_ADX_PERIOD
    tr = [0.0] * n
    pdm = [0.0] * n
    mdm = [0.0] * n
    for i in range(1, n):
        h, l, pc = highs[i], lows[i], closes[i-1]
        tr[i] = max(h - l, abs(h - pc), abs(pc - l))
        up = highs[i] - highs[i-1]
        dn = lows[i-1] - lows[i]
        if up > dn and up > 0:
            pdm[i] = up
        if dn > up and dn > 0:
            mdm[i] = dn

    def wilder(s, p):
        out = [0.0] * len(s)
        if len(s) <= p:
            return out
        acc = sum(s[1:p+1])
        out[p] = acc
        for i in range(p+1, len(s)):
            acc = acc - acc / p + s[i]
            out[i] = acc
        return out

    tr_s = wilder(tr, period)
    pdm_s = wilder(pdm, period)
    mdm_s = wilder(mdm, period)
    dx = [0.0] * n
    for i in range(period, n):
        if tr_s[i] <= 0:
            continue
        pdi = 100.0 * pdm_s[i] / tr_s[i]
        mdi = 100.0 * mdm_s[i] / tr_s[i]
        denom = pdi + mdi
        if denom > 0:
            dx[i] = 100.0 * abs(pdi - mdi) / denom
    first = 2 * period - 1
    if n <= first:
        return None, "no warmup"
    seed = dx[period:first+1]
    if not seed:
        return None, "seed empty"
    adx = sum(seed) / len(seed)
    for i in range(first+1, n):
        adx = (adx * (period - 1) + dx[i]) / period
    if math.isnan(adx) or math.isinf(adx):
        return None, "NaN"
    return float(adx), f"ADX {adx:.1f}"


def _compute_atr(candles, period):
    if len(candles) < period + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["high"], candles[i]["low"], candles[i-1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(pc - l)))
    if len(trs) < period:
        return None
    atr = sum(trs[:period]) / period
    for i in range(period, len(trs)):
        atr = (atr * (period - 1) + trs[i]) / period
    return atr if not (math.isnan(atr) or math.isinf(atr)) else None


def _compute_atr_ratio_daily(symbol, limit=100):
    pair = ADDITIONAL_SYMBOLS.get(symbol)
    if not pair:
        return None, "no pair"
    candles = fetch_kraken_ohlc_bb(pair, interval=1440, limit=limit)
    if not candles or len(candles) < ATR_PERIOD_LONG + 2:
        return None, "insufficient"
    candles = [c for c in candles[:-1] if _is_valid_candle(c)]
    if len(candles) < ATR_PERIOD_LONG + 2:
        return None, "insufficient after sanitize"
    a_s = _compute_atr(candles, ATR_PERIOD_SHORT)
    a_l = _compute_atr(candles, ATR_PERIOD_LONG)
    if not a_s or not a_l or a_l <= 0:
        return None, "unavailable"
    ratio = a_s / a_l
    if math.isnan(ratio) or math.isinf(ratio):
        return None, "NaN"
    return float(ratio), f"ATR x{ratio:.2f}"


def _regime_multiplier(adx, atr_ratio, symbol, profile: SystemProfile):
    if not REGIME_DETECTOR_ENABLED:
        return 1.00, "REGIME disabled"
    range_thr, trend_thr = REGIME_ADX_THRESHOLDS_BY_COIN.get(
        symbol, (REGIME_ADX_DEFAULT_RANGE, REGIME_ADX_DEFAULT_TREND)
    )
    if adx is None:
        adx_factor = 1.00
        adx_label = "ADX N/A"
    else:
        if adx >= trend_thr:
            adx_factor = profile.regime_mult_strong_trend
            adx_label = f"TREND {adx:.1f}"
        elif adx <= range_thr:
            adx_factor = profile.regime_mult_range
            adx_label = f"RANGE {adx:.1f}"
        else:
            span = max(trend_thr - range_thr, 1e-6)
            t = (adx - range_thr) / span
            adx_factor = (profile.regime_mult_range +
                          t * (profile.regime_mult_strong_trend - profile.regime_mult_range))
            adx_label = f"MID {adx:.1f}"
    if atr_ratio is None:
        atr_factor = 1.00
        atr_label = "ATR N/A"
    else:
        r = float(atr_ratio)
        if r >= ATR_RATIO_EXPANDING:
            atr_factor = profile.regime_mult_strong_trend
            atr_label = f"EXP x{r:.2f}"
        elif r <= ATR_RATIO_COMPRESSED:
            atr_factor = profile.regime_mult_range
            atr_label = f"COMP x{r:.2f}"
        else:
            span = max(ATR_RATIO_EXPANDING - ATR_RATIO_COMPRESSED, 1e-6)
            t = (r - ATR_RATIO_COMPRESSED) / span
            atr_factor = (profile.regime_mult_range +
                          t * (profile.regime_mult_strong_trend - profile.regime_mult_range))
            atr_label = f"NORM x{r:.2f}"
    combined = 0.5 * adx_factor + 0.5 * atr_factor
    combined = max(profile.regime_mult_strong_trend,
                   min(profile.regime_mult_range, combined))
    return combined, f"{adx_label} | {atr_label} -> x{combined:.2f}"


# ==============================================================================
# FUNDING (shared) — percentile-based
# ==============================================================================
FUNDING_PERCENTILE_WINDOW_DAYS = 30
FUNDING_PERCENTILE_MIN_SAMPLES = 30
FUNDING_PCTL_EMA_ALPHA = 0.35
FUNDING_PCTL_EMA_MAX_AGE_SEC = 3600
_funding_pctl_smooth_cache = {}

FUNDING_PCTL_EXTREME = 90.0
FUNDING_PCTL_MILD = 75.0
FUNDING_FALLBACK_EXTREME = 0.0005
FUNDING_FALLBACK_MILD = 0.0001


def _fetch_funding_history_deribit(currency, days=30):
    if currency not in ("BTC", "ETH"):
        return []
    try:
        instrument = f"{currency}-PERPETUAL"
        now_ms = int(time.time() * 1000)
        start_ms = now_ms - days * 24 * 3600 * 1000
        url = "https://www.deribit.com/api/v2/public/get_funding_rate_history"
        params = {"instrument_name": instrument,
                  "start_timestamp": start_ms, "end_timestamp": now_ms}
        r = requests.get(url, params=params, timeout=15, proxies=_PROXIES)
        r.raise_for_status()
        data = r.json()
        result = data.get("result")
        if not isinstance(result, list):
            return []
        out = []
        for entry in result:
            if not isinstance(entry, dict):
                continue
            v = entry.get("funding_rate", entry.get("interest_8h"))
            if v is None:
                continue
            try:
                out.append(float(v))
            except (TypeError, ValueError):
                continue
        return out
    except Exception:
        return []


def _funding_percentile_for_symbol(currency, smooth=True):
    hist = _fetch_funding_history_deribit(currency)
    if len(hist) < FUNDING_PERCENTILE_MIN_SAMPLES:
        return None, None, len(hist)
    current = hist[-1]
    sample = hist[:-1]
    if not sample:
        return None, current, 0
    below = sum(1 for x in sample if x < current)
    equal = sum(1 for x in sample if x == current)
    raw = 100.0 * (below + 0.5 * equal) / len(sample)
    if not smooth:
        return raw, current, len(hist)
    now = time.time()
    prev = _funding_pctl_smooth_cache.get(currency)
    if prev is None or (now - prev[1]) > FUNDING_PCTL_EMA_MAX_AGE_SEC:
        smoothed = raw
    else:
        smoothed = FUNDING_PCTL_EMA_ALPHA * raw + (1.0 - FUNDING_PCTL_EMA_ALPHA) * prev[0]
    _funding_pctl_smooth_cache[currency] = (smoothed, now)
    return smoothed, current, len(hist)


def _funding_direction_multiplier(crowd_long, crowd_short, is_extreme,
                                  composite_direction, profile: SystemProfile):
    if composite_direction == 0:
        return 1.00
    same_side = (composite_direction > 0 and crowd_long) or \
                (composite_direction < 0 and crowd_short)
    if same_side:
        return profile.funding_dampen_extreme if is_extreme else profile.funding_dampen_mild
    return profile.funding_mult_extreme if is_extreme else profile.funding_mult_mild


def _funding_multiplier_from_percentile(percentile, composite_direction,
                                        current_rate, profile: SystemProfile):
    if percentile is None:
        if current_rate is None:
            return 1.00, "FUND N/A"
        fr = float(current_rate)
        is_pos = fr > 0
        is_extreme = abs(fr) >= FUNDING_FALLBACK_EXTREME
        is_mild = abs(fr) >= FUNDING_FALLBACK_MILD
        if not is_mild:
            return 1.00, f"FUND flat ({fr*100:+.4f}%)"
        mult = _funding_direction_multiplier(is_pos, not is_pos, is_extreme,
                                              composite_direction, profile)
        return mult, f"FUND fb ({fr*100:+.4f}%)"
    p = float(percentile)
    deviation = abs(p - 50.0)
    crowd_long = p >= 50.0
    crowd_short = not crowd_long
    if deviation >= (FUNDING_PCTL_EXTREME - 50.0):
        is_extreme = True
    elif deviation >= (FUNDING_PCTL_MILD - 50.0):
        is_extreme = False
    else:
        return 1.00, f"FUND normal (p{p:.0f})"
    mult = _funding_direction_multiplier(crowd_long, crowd_short, is_extreme,
                                          composite_direction, profile)
    side = "long" if crowd_long else "short"
    tag = "ext" if is_extreme else "mild"
    return mult, f"FUND p{p:.0f} ({side},{tag})"


def _fetch_funding_rate_deribit(currency):
    if currency not in ("BTC", "ETH"):
        return None
    try:
        instrument = f"{currency}-PERPETUAL"
        now_ms = int(time.time() * 1000)
        start_ms = now_ms - 8 * 3600 * 1000
        url = "https://www.deribit.com/api/v2/public/get_funding_rate_value"
        params = {"instrument_name": instrument,
                  "start_timestamp": start_ms, "end_timestamp": now_ms}
        r = requests.get(url, params=params, timeout=10, proxies=_PROXIES)
        r.raise_for_status()
        data = r.json()
        result = data.get("result")
        if result is None:
            return None
        latest = result[-1] if isinstance(result, list) and result else result
        if isinstance(latest, dict):
            v = latest.get("funding_rate")
            return float(v) if v is not None else None
        return float(latest)
    except Exception:
        return None


# ==============================================================================
# DERIBIT OPTIONS ANALYZER (shared) — needed for TM score
# ==============================================================================
DERIBIT_USD_COINS = {"BTC", "ETH"}
DERIBIT_USDC_COINS = {"SOL", "AVAX", "TRX", "HYPE", "XRP"}
MIN_MEANINGFUL_OI = {"BTC": 25000, "ETH": 50000, "SOL": 10000,
                      "AVAX": 5000, "TRX": 5000, "HYPE": 1000, "XRP": 5000}
OI_THRESHOLDS = {"BTC": 100000, "ETH": 500000, "SOL": 50000,
                 "AVAX": 25000, "TRX": 25000, "HYPE": 5000, "XRP": 50000}


class DeribitMaxPainAnalyzer:
    def __init__(self):
        self.base_url = "https://www.deribit.com/api/v2"
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0"})
        if _PROXIES:
            self.session.proxies.update(_PROXIES)

    def fetch_all_option_summaries(self, target_currency):
        tc = target_currency.upper()
        if tc in DERIBIT_USD_COINS:
            qc = tc
            prefix = f"{tc}-"
        else:
            qc = "USDC"
            prefix = f"{tc}_"
        url = f"{self.base_url}/public/get_book_summary_by_currency?currency={qc}&kind=option"
        try:
            r = self.session.get(url, timeout=15)
            if r.status_code == 200:
                data = r.json()
                return [it for it in data.get("result", [])
                        if it["instrument_name"].startswith(prefix)]
        except Exception:
            pass
        return []

    def fetch_index_price(self, currency):
        c = currency.upper()
        idx = f"{c.lower()}_usd" if c in DERIBIT_USD_COINS else f"{c.lower()}_usdc"
        url = f"{self.base_url}/public/get_index_price?index_name={idx}"
        try:
            r = self.session.get(url, timeout=10)
            if r.status_code == 200:
                return r.json().get("result", {}).get("index_price")
        except Exception:
            pass
        return None

    def parse_instrument(self, name):
        p = name.split("-")
        if len(p) != 4:
            return None, None, None
        try:
            return p[1], float(p[2]), "call" if p[3] == "C" else "put"
        except ValueError:
            return None, None, None

    def get_closest_3_expiries(self, currency):
        summaries = self.fetch_all_option_summaries(currency)
        spot = self.fetch_index_price(currency)
        if not summaries or not spot:
            return None, spot, summaries
        breakdown = defaultdict(lambda: defaultdict(lambda: {"call_oi": 0.0, "put_oi": 0.0}))
        for book in summaries:
            oi = book.get("open_interest", 0.0)
            if oi <= 0:
                continue
            exp, strike, ot = self.parse_instrument(book["instrument_name"])
            if exp and strike:
                breakdown[exp][strike][f"{ot}_oi"] += oi

        def pdate(e):
            try:
                return datetime.strptime(e, "%d%b%y").replace(tzinfo=timezone.utc)
            except ValueError:
                return datetime.max

        now = datetime.now(timezone.utc)
        futures = []
        for e in breakdown:
            dt = pdate(e).replace(hour=8, minute=0, second=0)
            if dt > now:
                futures.append((dt, e))
        futures.sort(key=lambda x: x[0])
        results = []
        for dt, e in futures[:3]:
            oi_map = breakdown[e]
            strikes = sorted(oi_map.keys())
            pain = {}
            for s in strikes:
                pain[s] = sum(max(0.0, sk - s) * d["call_oi"]
                              + max(0.0, s - sk) * d["put_oi"]
                              for sk, d in oi_map.items())
            mp = min(pain, key=pain.get)
            call_oi = sum(d["call_oi"] for d in oi_map.values())
            put_oi = sum(d["put_oi"] for d in oi_map.values())
            hours = max(0.0, (dt - now).total_seconds() / 3600.0)
            pdp = (mp - spot) / spot
            nd = clamp(pdp / 0.02, -1.0, 1.0)
            mp_s = 0.5 + nd * 0.5
            min_oi = MIN_MEANINGFUL_OI.get(currency, 10000)
            full_oi = OI_THRESHOLDS.get(currency, 50000)
            oi_gate = clamp((call_oi + put_oi - min_oi) / max(full_oi - min_oi, 1), 0.1, 1.0)
            gmp = 0.5 + (mp_s - 0.5) * oi_gate
            pcr = put_oi / call_oi if call_oi > 0 else 0.0
            pc_s = clamp(0.5 + ((0.7 - pcr) / 0.6) * 0.5, 0.0, 1.0)
            t_s = 0.5 + (gmp - 0.5) * (1.0 - clamp(hours / 48.0, 0.0, 1.0))
            score = 0.85 * gmp + 0.10 * pc_s + 0.05 * t_s
            if score >= 0.65: bias = "Bullish"
            elif score <= 0.35: bias = "Bearish"
            elif score >= 0.55: bias = "Mildly Bullish"
            elif score <= 0.45: bias = "Mildly Bearish"
            else: bias = "Neutral"
            total_oi = call_oi + put_oi
            pain_mass = sum(max(0.0, s - mp) * d["call_oi"]
                            + max(0.0, mp - s) * d["put_oi"]
                            for s, d in oi_map.items())
            results.append({
                "exp_str": e, "max_pain": mp, "bias": bias,
                "total_oi": total_oi, "pain_mass": pain_mass,
                "hours_to_exp": hours, "call_oi": call_oi, "put_oi": put_oi,
            })
        return results, spot, summaries


def calculate_gex_and_walls(currency, spot, summaries):
    if not spot or not summaries:
        return None, None, None
    now_ts = datetime.now(timezone.utc).timestamp()
    strike_gex = defaultdict(float)
    chain = []
    for book in summaries:
        oi = book.get("open_interest", 0.0)
        iv = book.get("mark_iv", 0.0) / 100.0
        if oi <= 0 or iv <= 0:
            continue
        exp, strike, ot = DeribitMaxPainAnalyzer().parse_instrument(book["instrument_name"])
        if not exp or not strike:
            continue
        try:
            exp_dt = datetime.strptime(f"{exp} 08:00:00", "%d%b%y %H:%M:%S").replace(tzinfo=timezone.utc)
            T = max((exp_dt.timestamp() - now_ts) / (365.25 * 86400), 1e-6)
        except ValueError:
            T = 0.01
        g = bs_gamma(spot, strike, T, 0.0, iv)
        sign = 1.0 if ot == "call" else -1.0
        gex = sign * g * oi * (spot ** 2) * 0.01
        strike_gex[strike] += gex
        chain.append({"strike": strike, "type": ot, "oi": oi, "iv": iv, "T": T})
    if not strike_gex:
        return None, None, None
    cw = max(strike_gex, key=strike_gex.get) if currency in ("BTC", "ETH") else None
    pw = min(strike_gex, key=strike_gex.get) if currency in ("BTC", "ETH") else None

    def net_gex(s):
        return sum((1 if o["type"] == "call" else -1) * bs_gamma(s, o["strike"], o["T"], 0.0, o["iv"])
                   * o["oi"] * (s ** 2) * 0.01 for o in chain)
    flip = None
    if currency in ("BTC", "ETH"):
        lo, hi = spot * 0.5, spot * 1.5
        gl, gh = net_gex(lo), net_gex(hi)
        if gl * gh <= 0:
            for _ in range(50):
                mid = (lo + hi) / 2
                gm = net_gex(mid)
                if abs(hi - lo) < 0.1 or abs(gm) < 1e-4:
                    flip = mid
                    break
                if gl * gm < 0:
                    hi = mid
                else:
                    lo = mid
                    gl = gm
    return cw, pw, flip


def calculate_mm_edges(max_pain):
    if not max_pain:
        return None, None
    hw = max_pain * 0.015
    return max_pain + hw, max_pain - hw


def _fetch_trademap_conclusion_score(symbol):
    try:
        analyzer = DeribitMaxPainAnalyzer()
        exp_results, spot, summaries = analyzer.get_closest_3_expiries(symbol)
        if not spot or not exp_results:
            return None
        cw, pw, flip = calculate_gex_and_walls(symbol, spot, summaries)
        ue, le = calculate_mm_edges(exp_results[0]["max_pain"])
        fs, fd = get_supply_demand_levels(symbol, spot)
        pair = KRAKEN_PAIR_MAP.get(symbol, f"{symbol}USD")
        p4 = fetch_kraken_ohlc_bb(pair, interval=240, limit=100)
        bb, _ = get_bb_wick_boost_for_timeframe(symbol, 240, 10, 5, "4H", prefetched_4h=p4)

        thr = _close_threshold_for(symbol)
        if ue and le:
            if spot > ue:
                e = 10.0
            elif spot < le:
                e = 90.0
            elif abs(spot - ue) / spot <= thr:
                e = 20.0
            elif abs(spot - le) / spot <= thr:
                e = 80.0
            else:
                e = 50.0
        else:
            e = 50.0

        mps = [x["max_pain"] for x in exp_results]
        above = sum(1 for mp in mps if mp > spot)
        below = sum(1 for mp in mps if mp < spot)
        tot = above + below
        a = 50.0 if tot == 0 else (above / tot) * 100.0

        if symbol in ("BTC", "ETH") and cw and pw:
            d_cw = abs(cw - spot) / spot
            d_pw = abs(spot - pw) / spot
            if d_cw <= thr and d_pw <= thr:
                w = 50.0
            elif d_cw <= thr:
                w = 15.0
            elif d_pw <= thr:
                w = 85.0
            else:
                w = 50.0
        else:
            w = 50.0

        if fs and fd:
            ds = abs(fs["price"] - spot) / spot
            dd = abs(spot - fd["price"]) / spot
            if ds <= thr and dd <= thr:
                sd = 50.0
            elif ds <= thr:
                sd = 10.0
            elif dd <= thr:
                sd = 90.0
            else:
                sd = 50.0
        elif fs:
            sd = 10.0 if abs(fs["price"] - spot) / spot <= thr else 50.0
        elif fd:
            sd = 90.0 if abs(spot - fd["price"]) / spot <= thr else 50.0
        else:
            sd = 50.0

        if symbol == "SOL":
            comp = 0.20 * e + 0.40 * a + 0.00 * w + 0.40 * sd
        else:
            comp = 0.15 * e + 0.30 * a + 0.15 * w + 0.40 * sd
        comp = clamp(comp + bb, 0.0, 100.0)
        return float(comp)
    except Exception:
        return None


# ==============================================================================
# CONTEXT CACHE (skew / gamma flip / funding) — shared
# ==============================================================================
_cascade_context_cache: Dict[str, dict] = {}


def _cascade_fetch_option_summary(currency):
    url = "https://www.deribit.com/api/v2/public/get_book_summary_by_currency"
    params = {"currency": currency, "kind": "option"}
    r = requests.get(url, params=params, timeout=20, proxies=_PROXIES)
    r.raise_for_status()
    return r.json()["result"]


def _cascade_fetch_index_price(currency):
    url = "https://www.deribit.com/api/v2/public/get_index_price"
    params = {"index_name": f"{currency.lower()}_usd"}
    r = requests.get(url, params=params, timeout=10, proxies=_PROXIES)
    r.raise_for_status()
    return r.json()["result"]["index_price"]


def _cascade_parse_options(raw):
    rows = []
    for o in raw:
        name = o["instrument_name"]
        p = name.split("-")
        if len(p) != 4:
            continue
        _, exp, strike, ot = p
        try:
            edt = datetime.strptime(exp, "%d%b%y").replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        iv = o.get("mark_iv")
        und = o.get("underlying_price")
        if iv is None or und is None:
            continue
        rows.append({"instrument": name, "expiry": edt,
                     "strike": float(strike), "type": ot,
                     "mark_iv": float(iv), "underlying": float(und),
                     "oi": float(o.get("open_interest") or 0),
                     "volume": float(o.get("volume") or 0)})
    return rows


def _cascade_days_to_expiry(expiry, ref=None):
    ref = ref or datetime.now(timezone.utc)
    return (expiry - ref).total_seconds() / 86400.0


def _cascade_compute_skew(rows, target_days=30):
    if not rows:
        return None
    spot = rows[0]["underlying"]
    expiries = {r["expiry"]: _cascade_days_to_expiry(r["expiry"]) for r in rows}
    if not expiries:
        return None
    be = min(expiries, key=lambda e: abs(expiries[e] - target_days))
    calls = [r for r in rows if r["expiry"] == be and r["type"] == "C" and r["strike"] > spot]
    puts = [r for r in rows if r["expiry"] == be and r["type"] == "P" and r["strike"] < spot]
    if not calls or not puts:
        return None
    c = min(calls, key=lambda r: abs(r["strike"] - spot * 1.10))
    p = min(puts, key=lambda r: abs(r["strike"] - spot * 0.90))
    return {"skew": round(c["mark_iv"] - p["mark_iv"], 2)}


def _cascade_compute_gex(rows, spot):
    if not rows:
        return None, []
    gex_by = defaultdict(float)
    for r in rows:
        T = max(_cascade_days_to_expiry(r["expiry"]) / 365.0, 1e-6)
        sigma = r["mark_iv"] / 100.0
        g = bs_gamma(spot, r["strike"], T, 0.0, sigma)
        sign = -1 if r["type"] == "C" else 1
        fr = _gex_freshness_multiplier(r.get("volume", 0.0), r.get("oi", 0.0))
        if fr <= 0:
            continue
        gex_by[r["strike"]] += g * r["oi"] * sign * (spot ** 2) * 0.01 * fr
    ks = sorted(gex_by.keys())
    cum = 0.0
    prev_cum = None
    prev_k = None
    flip = None
    for k in ks:
        prev_cum = cum
        prev_k = k
        cum += gex_by[k]
        if prev_cum is not None and prev_cum * cum < 0 and flip is None and cum != prev_cum:
            flip = prev_k + (0 - prev_cum) * (k - prev_k) / (cum - prev_cum)
    total = sum(gex_by.values())
    return {"gamma_flip": round(flip, 2) if flip else None,
            "total_gex": total}, []


GEX_FRESHNESS_LADDER = [
    (2.00, 1.50), (1.00, 1.30), (0.50, 1.10), (0.20, 1.00), (0.00, 0.70),
]


def _gex_freshness_multiplier(volume, oi):
    if oi is None or oi <= 0:
        return 0.0
    if volume is None or volume <= 0:
        return 1.00
    r = volume / oi
    for th, m in GEX_FRESHNESS_LADDER:
        if r >= th:
            return m
    return 0.70


def build_cascade_context_cache():
    global _cascade_context_cache
    _cascade_context_cache = {}
    for sym in ("BTC", "ETH"):
        ctx = {"skew": None, "gamma_flip": None, "spot": None, "funding_rate": None}
        try:
            raw = _cascade_fetch_option_summary(sym)
            rows = _cascade_parse_options(raw)
            if rows:
                spot = _cascade_fetch_index_price(sym)
                ctx["spot"] = spot
                sk = _cascade_compute_skew(rows, target_days=30)
                if sk:
                    ctx["skew"] = sk["skew"]
                gex, _ = _cascade_compute_gex(rows, spot)
                if gex:
                    ctx["gamma_flip"] = gex.get("gamma_flip")
            ctx["funding_rate"] = _fetch_funding_rate_deribit(sym)
        except Exception as e:
            _quiet_print(f"  [CTX] {sym} failed: {e}")
        _cascade_context_cache[sym] = ctx


# ==============================================================================
# LIVE COMP / SPOT SAMPLER (shared)
# ==============================================================================
LIVE_COMP_SAMPLE_SEC = 900
LIVE_COMP_CACHE_MAX_AGE_SEC = LIVE_COMP_SAMPLE_SEC + 30
LIVE_COMP_PREFER_CACHE = True
LIVE_COMP_SAMPLER_ENABLED = True

_live_comp_cache: Dict[str, dict] = {}
_live_comp_cache_lock = threading.Lock()


def _compute_live_comp_and_spot(symbol, force_fresh=False):
    if LIVE_COMP_PREFER_CACHE and not force_fresh:
        with _live_comp_cache_lock:
            e = _live_comp_cache.get(symbol)
        if e and (time.time() - e.get("ts", 0.0)) <= LIVE_COMP_CACHE_MAX_AGE_SEC:
            return float(e["comp"]), float(e["spot"])
    pair = ADDITIONAL_SYMBOLS.get(symbol)
    if not pair:
        return 50.0, 0.0
    try:
        candles = fetch_kraken_ohlc_bb(pair, interval=240, limit=5)
        spot = candles[-1]["close"] if candles else 0.0
        comp, _, _, _, _ = calculate_composite_score_extended(symbol, spot)
        comp = float(comp)
        spot = float(spot)

        adx, _ = _compute_adx_daily(symbol, limit=100)
        if adx is None:
            adx, _ = _compute_adx_daily(symbol, limit=60)
        atr_r, _ = _compute_atr_ratio_daily(symbol, limit=100)
        if atr_r is None:
            atr_r, _ = _compute_atr_ratio_daily(symbol, limit=60)

        funding_pctl, funding_rate, _ = (None, None, 0)
        if symbol in ("BTC", "ETH"):
            funding_pctl, funding_rate, _ = _funding_percentile_for_symbol(symbol)

        with _live_comp_cache_lock:
            _live_comp_cache[symbol] = {
                "comp": comp, "spot": spot,
                "adx": adx, "atr_ratio": atr_r,
                "funding_pctl": funding_pctl, "funding_rate": funding_rate,
                "ts": time.time(),
            }
        return comp, spot
    except Exception as e:
        _quiet_print(f"  [LIVE-COMP] {symbol} failed: {e}")
        return 50.0, 0.0


def _get_cached_regime_and_funding(symbol):
    with _live_comp_cache_lock:
        e = _live_comp_cache.get(symbol)
    if not e:
        return None, None, None, None, None
    return e.get("adx"), e.get("atr_ratio"), e.get("funding_pctl"), e.get("funding_rate"), e


def live_comp_sampler_worker():
    if not LIVE_COMP_SAMPLER_ENABLED:
        return
    time.sleep(5)
    while True:
        try:
            for sym in list(ADDITIONAL_SYMBOLS.keys()):
                if sym in SKIP_SYMBOLS:
                    continue
                try:
                    _compute_live_comp_and_spot(sym, force_fresh=True)
                except Exception:
                    pass
        except Exception as e:
            _quiet_print(f"  [SAMPLER] error: {e}")
        time.sleep(LIVE_COMP_SAMPLE_SEC)


# ==============================================================================
# FINAL COMPOSITE — per-system
# ==============================================================================
FINAL_COMPOSITE_PRIMARY_COINS = {"BTC", "ETH", "HYPE", "XRP", "SOL"}


def _volume_score_from_pct_and_comp(pct, comp):
    if pct is None or comp is None:
        return 50.0
    if pct > 100.0 and comp > 69.0:
        return 90.0
    if pct > 100.0 and comp < 31.0:
        return 10.0
    return 50.0


def _daily_wick_boost(wick_tag, comp_score, daily_pattern_tag,
                     wick_boost_max: float):
    if not wick_tag:
        return 0.0
    bear_patterns = {"SS", "DJ", "SS+DJ"}
    bull_patterns = {"DJ", "HM", "HM+DJ"}
    pat = (daily_pattern_tag or "").strip().upper() if daily_pattern_tag else ""
    if comp_score is not None and comp_score < 40.0 and pat in bear_patterns:
        return -wick_boost_max
    if comp_score is not None and comp_score > 60.0 and pat in bull_patterns:
        return +wick_boost_max
    return 0.0


def _skew_boost_from_value(skew_value, gex_amplifier, skew_boost_max: float):
    if skew_value is None:
        return 0.0
    is_ext_bear = skew_value <= -3.0
    is_ext_bull = skew_value >= 3.0
    is_ext = is_ext_bear or is_ext_bull
    amp = float(gex_amplifier) if gex_amplifier is not None else 1.0
    is_neg = amp > 1.0
    is_pos = amp < 1.0
    if is_ext:
        if is_neg:
            if is_ext_bear:
                return -skew_boost_max
            return +skew_boost_max
        elif is_pos:
            if is_ext_bear:
                return +skew_boost_max
            return -skew_boost_max
    if skew_value <= -1.0:
        return -min(3.0, skew_boost_max)
    if skew_value >= 1.0:
        return +min(3.0, skew_boost_max)
    return 0.0


def _gex_amplifier_from_regime(spot, gamma_flip, profile: SystemProfile):
    if not spot or not gamma_flip:
        return 1.00, "UNKNOWN"
    dist = abs(spot - gamma_flip) / spot
    if dist <= 0.005:
        return profile.gex_transitional, f"TRANS ({dist*100:.2f}%)"
    if spot < gamma_flip:
        return profile.gex_short_gamma, "SHORT GAMMA"
    return profile.gex_long_gamma, "LONG GAMMA"


def compute_final_composite_for_symbol(
        symbol, trademap_score, trademap_score_btc,
        extended_composite_score, extended_composite_score_btc,
        daily_vol_pct, h4_vol_pct, daily_wick_tag, daily_pattern_tag,
        skew_value, gex_amplifier, funding_rate, funding_percentile,
        adx_value, atr_ratio,
        profile: SystemProfile) -> dict:
    daily_vol_score = _volume_score_from_pct_and_comp(daily_vol_pct, extended_composite_score)
    h4_vol_score = _volume_score_from_pct_and_comp(h4_vol_pct, extended_composite_score)

    tm_used = trademap_score_btc if symbol not in FINAL_COMPOSITE_PRIMARY_COINS else trademap_score
    if tm_used is None:
        tm_used = 50.0
    if extended_composite_score is None:
        extended_composite_score = 50.0

    base = (profile.weight_tm * tm_used
            + profile.weight_comp * extended_composite_score
            + profile.weight_daily_vol * daily_vol_score
            + profile.weight_4h_vol * h4_vol_score)

    if base > 50.5:
        direction = 1
    elif base < 49.5:
        direction = -1
    else:
        direction = 0

    amp = float(gex_amplifier) if gex_amplifier is not None else 1.0
    amplified = 50.0 + (base - 50.0) * amp

    funding_mult, funding_label = _funding_multiplier_from_percentile(
        funding_percentile, direction, funding_rate, profile
    )
    funded = 50.0 + (amplified - 50.0) * funding_mult

    regime_mult, regime_label = _regime_multiplier(adx_value, atr_ratio, symbol, profile)
    regime_base = 50.0 + (funded - 50.0) * regime_mult

    wick_boost = _daily_wick_boost(daily_wick_tag, extended_composite_score,
                                    daily_pattern_tag, profile.wick_boost_max)
    skew_boost = _skew_boost_from_value(skew_value, gex_amplifier, profile.skew_boost_max)

    final = clamp(regime_base + wick_boost + skew_boost, 0.0, 100.0)

    return {
        "system": profile.name,
        "symbol": symbol,
        "trademap_score_used": tm_used,
        "extended_composite_score": extended_composite_score,
        "daily_vol_score": daily_vol_score,
        "h4_vol_score": h4_vol_score,
        "daily_vol_pct_of_ma10": daily_vol_pct,
        "h4_vol_pct_of_ma10": h4_vol_pct,
        "daily_wick_tag": daily_wick_tag,
        "daily_pattern_tag": daily_pattern_tag,
        "wick_boost": wick_boost,
        "skew_value": skew_value,
        "skew_boost": skew_boost,
        "gex_amplifier": amp,
        "funding_rate": funding_rate,
        "funding_percentile": funding_percentile,
        "funding_multiplier": funding_mult,
        "funding_label": funding_label,
        "adx_value": adx_value,
        "atr_ratio": atr_ratio,
        "regime_multiplier": regime_mult,
        "regime_label": regime_label,
        "base_score": base,
        "amplified_base": amplified,
        "funded_base": funded,
        "regime_base": regime_base,
        "composite_direction": direction,
        "final_score": final,
    }


# ==============================================================================
# FINAL COMPOSITE ALERTS — per-system
# ==============================================================================
_final_composite_last_alert_ts: Dict[str, Dict[str, float]] = {
    "SHORT": {}, "POSITION": {}
}
FINAL_COMPOSITE_STAGGER_SEC = 5.0


def _direction_from_final(final, profile: SystemProfile):
    if final > profile.threshold_high:
        return "HIGH"
    if final < profile.threshold_low:
        return "LOW"
    return None


def _cooldown_active(system: str, symbol: str, profile: SystemProfile):
    last = _final_composite_last_alert_ts[system].get(symbol)
    if last is None:
        return False
    return (time.time() - last) < profile.cooldown_sec


def _build_alert_msg(symbol, final, direction, detail, profile: SystemProfile):
    skew_v = detail.get("skew_value")
    skew_s = f"{skew_v:+.2f}" if skew_v is not None else "N/A"
    fr = detail.get("funding_rate")
    fr_s = f"{fr*100:+.4f}%" if fr is not None else "N/A"
    fp = detail.get("funding_percentile")
    fp_s = f"{fp:.0f}" if fp is not None else "N/A"
    adx_v = detail.get("adx_value")
    adx_s = f"{adx_v:.1f}" if adx_v is not None else "N/A"
    atr_v = detail.get("atr_ratio")
    atr_s = f"x{atr_v:.2f}" if atr_v is not None else "N/A"

    head = "BULLISH" if direction == "HIGH" else "BEARISH"
    thr = profile.threshold_high if direction == "HIGH" else profile.threshold_low
    return (
        f"FINAL COMPOSITE ALERT - {symbol} [{profile.name}]\n"
        f"Direction: {head}\n"
        f"Final: {final:.2f}/100 (thr {'>' if direction=='HIGH' else '<'} {thr:.0f})\n"
        f"\n"
        f"Components ({profile.name} weights):\n"
        f"  TM x{profile.weight_tm:.2f}  = {detail['trademap_score_used']:.1f}\n"
        f"  COMP x{profile.weight_comp:.2f} = {detail['extended_composite_score']:.1f}\n"
        f"  DVOL x{profile.weight_daily_vol:.2f} = {detail['daily_vol_score']:.0f}\n"
        f"  4HV x{profile.weight_4h_vol:.2f} = {detail['h4_vol_score']:.0f}\n"
        f"  Base: {detail['base_score']:.2f}\n"
        f"  GEX x{detail['gex_amplifier']:.2f} -> {detail['amplified_base']:.2f}\n"
        f"  Fund p{fp_s} (x{detail['funding_multiplier']:.2f}) -> {detail['funded_base']:.2f}\n"
        f"  ADX={adx_s} ATR={atr_s} Regime x{detail['regime_multiplier']:.2f} -> {detail['regime_base']:.2f}\n"
        f"  Wick {detail['wick_boost']:+.0f} ({detail['daily_wick_tag']})\n"
        f"  Skew {skew_s} -> {detail['skew_boost']:+.0f}\n"
        f"\n"
        f"Cooldown: {profile.cooldown_sec // 60} min\n"
        f"Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"
    )


def _fire_alert(symbol, final, direction, detail, profile: SystemProfile, source="scan"):
    title = (f"COMP {symbol} > {profile.threshold_high:.0f}"
             if direction == "HIGH"
             else f"COMP {symbol} < {profile.threshold_low:.0f}")
    tags = (["chart_with_upwards_trend", "rotating_light"] if direction == "HIGH"
            else ["chart_with_downwards_trend", "rotating_light"])
    msg = _build_alert_msg(symbol, final, direction, detail, profile)
    if source:
        msg += f"\nSource: {source}"

    _quiet_print(f"\n[{profile.name}] {symbol} {final:.2f} -> {direction}")
    send_ntfy_message(msg, title=title, priority="high", tags=tags, system=profile.name)
    _final_composite_last_alert_ts[profile.name][symbol] = time.time()
    time.sleep(FINAL_COMPOSITE_STAGGER_SEC)


# ==============================================================================
# FULL SCAN (both systems)
# ==============================================================================
def run_dual_scan_and_alert(daily_rows, h4_rows, trademap_scores,
                             send_report=True, send_alerts=True):
    btc_tm = trademap_scores.get("BTC")
    btc_comp, _ = _compute_live_comp_and_spot("BTC")

    report_lines = [
        "FINAL COMPOSITE - DUAL SYSTEM",
        f"Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}",
        "Thresholds: SHORT 80/20   POSITION 85/15",
        "```",
        f"{'SYM':<6} | {'SHORT':>6} {'S-DIR':>6} | {'POS':>6} {'P-DIR':>6} | "
        f"{'TM':>6} | {'COMP':>6} | {'DVOL%':>6} | {'4HV%':>5} | "
        f"{'WK':>4} | {'GEX':>5} | {'FP':>4} | {'ADX':>5} | {'ATR':>5}",
        "-" * 130,
    ]

    pending_short = []
    pending_pos = []

    for symbol in ADDITIONAL_SYMBOLS:
        if symbol in SKIP_SYMBOLS:
            continue

        daily_row = next((r for r in daily_rows if r.get("symbol") == symbol), None)
        h4_row = next((r for r in h4_rows if r.get("symbol") == symbol), None)

        if not daily_row or daily_row.get("error"):
            continue

        daily_pct = daily_row.get("pct_of_ma10")
        daily_wick = daily_row.get("wick_tag")
        daily_pat = daily_row.get("pattern_tag")
        h4_pct = h4_row.get("pct_of_ma10") if (h4_row and not h4_row.get("error")) else None

        live_comp, _ = _compute_live_comp_and_spot(symbol)
        tm_own = trademap_scores.get(symbol)

        adx, atr_ratio, fpctl, frate, _ = _get_cached_regime_and_funding(symbol)

        ctx = _cascade_context_cache.get(symbol, {}) or {}
        skew_v = ctx.get("skew")

        row_short = "-"
        row_pos = "-"
        gex_amp = 1.0

        for profile in (SHORT_TERM, POSITION):
            gex_amp, _ = _gex_amplifier_from_regime(ctx.get("spot"),
                                                    ctx.get("gamma_flip"),
                                                    profile)
            detail = compute_final_composite_for_symbol(
                symbol=symbol,
                trademap_score=tm_own,
                trademap_score_btc=btc_tm,
                extended_composite_score=live_comp,
                extended_composite_score_btc=btc_comp,
                daily_vol_pct=daily_pct,
                h4_vol_pct=h4_pct,
                daily_wick_tag=daily_wick,
                daily_pattern_tag=daily_pat,
                skew_value=skew_v,
                gex_amplifier=gex_amp,
                funding_rate=frate,
                funding_percentile=fpctl,
                adx_value=adx,
                atr_ratio=atr_ratio,
                profile=profile,
            )
            direction = _direction_from_final(detail["final_score"], profile)

            if profile.name == "SHORT":
                row_short = f"{detail['final_score']:5.1f} {direction or '   '}"
                if direction and send_alerts:
                    pending_short.append((symbol, detail["final_score"], direction, detail))
            else:
                row_pos = f"{detail['final_score']:5.1f} {direction or '   '}"
                if direction and send_alerts:
                    pending_pos.append((symbol, detail["final_score"], direction, detail))

        report_lines.append(
            f"{symbol:<6} | {row_short:>13} | {row_pos:>13} | "
            f"{(tm_own or 50):6.1f} | {live_comp:6.1f} | "
            f"{(daily_pct or 0):5.0f}% | {(h4_pct or 0):4.0f}% | "
            f"{(daily_row.get('pattern_tag') or '-'):>4} | "
            f"x{gex_amp:.2f} | {fpctl if fpctl is not None else 0:4.0f} | "
            f"{adx if adx is not None else 0:5.1f} | "
            f"x{atr_ratio if atr_ratio is not None else 1:.2f}"
        )

    report_lines.append("```")

    if pending_short:
        report_lines.append(f"\nSHORT signals ({len(pending_short)}): "
                             + ", ".join(f"{s}({f:.0f})" for s, f, _, _ in pending_short))
    if pending_pos:
        report_lines.append(f"\nPOSITION signals ({len(pending_pos)}): "
                             + ", ".join(f"{s}({f:.0f})" for s, f, _, _ in pending_pos))

    if send_report:
        send_ntfy_message("\n".join(report_lines),
                          title="Dual System Report", priority="default",
                          tags=["bar_chart"], force=not _QUIET_MODE)

    for symbol, final, direction, detail in pending_short:
        if _cooldown_active("SHORT", symbol, SHORT_TERM):
            continue
        _fire_alert(symbol, final, direction, detail, SHORT_TERM, source="scan")

    for symbol, final, direction, detail in pending_pos:
        if _cooldown_active("POSITION", symbol, POSITION):
            continue
        _fire_alert(symbol, final, direction, detail, POSITION, source="scan")


# ==============================================================================
# BB5 SCANNER — short-term trigger
# ==============================================================================
def calculate_bollinger_bands_5m(closes, period=BB5_PERIOD, mult=BB5_STDDEV):
    if len(closes) < period:
        return None
    w = closes[-period:]
    sma = sum(w) / period
    var = sum((x - sma) ** 2 for x in w) / period
    std = var ** 0.5
    return sma, sma + mult * std, sma - mult * std


def _get_latest_final_for_system(symbol, profile: SystemProfile):
    live_comp, spot = _compute_live_comp_and_spot(symbol, force_fresh=True)
    daily_row = compute_daily_volume_vs_ma10(symbol)
    h4_row = compute_4h_volume_vs_ma10(symbol)
    daily_pct = daily_row.get("pct_of_ma10") if daily_row else None
    h4_pct = h4_row.get("pct_of_ma10") if (h4_row and not h4_row.get("error")) else None
    daily_wick = daily_row.get("wick_tag") if daily_row else None
    daily_pat = daily_row.get("pattern_tag") if daily_row else None
    adx, atr, fpctl, frate, _ = _get_cached_regime_and_funding(symbol)
    ctx = _cascade_context_cache.get(symbol, {}) or {}
    gex_amp, _ = _gex_amplifier_from_regime(ctx.get("spot"), ctx.get("gamma_flip"), profile)
    tm_own = _fetch_trademap_conclusion_score(symbol)
    tm_btc = _fetch_trademap_conclusion_score("BTC")
    btc_comp, _ = _compute_live_comp_and_spot("BTC")
    detail = compute_final_composite_for_symbol(
        symbol=symbol, trademap_score=tm_own, trademap_score_btc=tm_btc,
        extended_composite_score=live_comp, extended_composite_score_btc=btc_comp,
        daily_vol_pct=daily_pct, h4_vol_pct=h4_pct,
        daily_wick_tag=daily_wick, daily_pattern_tag=daily_pat,
        skew_value=ctx.get("skew"), gex_amplifier=gex_amp,
        funding_rate=frate, funding_percentile=fpctl,
        adx_value=adx, atr_ratio=atr,
        profile=profile,
    )
    return detail["final_score"], detail


def check_bb5_touch_for_symbol(symbol):
    pair = BB5_SYMBOLS.get(symbol)
    if not pair:
        return
    candles = fetch_kraken_ohlc_bb(pair, interval=BB5_OHLC_INTERVAL, limit=100)
    if len(candles) < BB5_PERIOD + 2:
        return
    closed = candles[:-1]
    if len(closed) < BB5_PERIOD:
        return
    closes_for = [float(c["close"]) for c in closed[:-1]]
    bb = calculate_bollinger_bands_5m(closes_for)
    if bb is None:
        return
    sma, upper, lower = bb
    last = closed[-1]
    high = float(last["high"]); low = float(last["low"]); close = float(last["close"])
    hit = "UPPER" if high >= upper else ("LOWER" if low <= lower else None)
    if not hit:
        return
    now = time.time()
    if now - _bb5_last_alert.get(symbol, 0) < BB5_ALERT_COOLDOWN:
        return

    final, detail = _get_latest_final_for_system(symbol, SHORT_TERM)
    adjusted, adj, reason = apply_relative_value_adjustment(symbol, final, hit, SHORT_TERM)
    if adj != 0.0:
        _quiet_print(f"  [BB5-RV] {symbol} {hit}: {final:.2f} -> {adjusted:.2f} ({reason})")

    gate_fail = (hit == "UPPER" and adjusted >= SHORT_TERM.bb5_upper_final_max) or \
                (hit == "LOWER" and adjusted <= SHORT_TERM.bb5_lower_final_min)
    if gate_fail:
        return

    _bb5_last_alert[symbol] = now

    title = f"{symbol} wicked {hit} BB (4hr) [SHORT]"
    body = (f"{symbol} ({pair}) 4H candle\n"
            f"H: {high:.6g}  L: {low:.6g}  C: {close:.6g}\n"
            f"Upper: {upper:.6g}  SMA20: {sma:.6g}  Lower: {lower:.6g}\n"
            f"SHORT COMP: {adjusted:.2f} (raw {final:.2f}{', RV '+format(adj,'+.0f') if adj else ''})\n"
            f"Candle: {datetime.fromtimestamp(int(last['time']), tz=timezone.utc):%Y-%m-%d %H:%M UTC}")
    send_ntfy_message(body, title=title, priority="high",
                      tags=["chart_with_upwards_trend"] if hit == "LOWER"
                           else ["chart_with_downwards_trend"],
                      system="SHORT")


def bb5_scanner_worker():
    time.sleep(10)
    while True:
        try:
            for s in BB5_SYMBOLS:
                if s in SKIP_SYMBOLS:
                    continue
                try:
                    check_bb5_touch_for_symbol(s)
                except Exception as e:
                    _quiet_print(f"  [BB5] {s}: {e}")
                time.sleep(1)
        except Exception as e:
            _quiet_print(f"  [BB5] cycle: {e}")
        time.sleep(BB5_CHECK_INTERVAL)


# ==============================================================================
# DAILY BB TRIGGER — position system
# ==============================================================================
_daily_bb_last_alert: Dict[str, float] = {}
DAILY_BB_COOLDOWN_SEC = 24 * 3600


def check_daily_bb_for_symbol(symbol):
    """Position-system trigger: daily BB wick poke (no close confirmation)."""
    pair = BB5_SYMBOLS.get(symbol) or KRAKEN_PAIR_MAP.get(symbol)
    if not pair:
        return
    candles = fetch_kraken_ohlc_bb(pair, interval=1440, limit=60)
    if len(candles) < BB_PERIOD + 2:
        return

    closed = candles[:-1]
    live = candles[-1]

    if len(closed) < BB_PERIOD:
        return
    up, mid, lo = calculate_bollinger_bands(closed)
    if up is None:
        return

    lh = live["high"]
    ll = live["low"]

    hit = "UPPER" if lh >= up else ("LOWER" if ll <= lo else None)
    if not hit:
        return

    now = time.time()
    if now - _daily_bb_last_alert.get(symbol, 0) < DAILY_BB_COOLDOWN_SEC:
        return

    final, detail = _get_latest_final_for_system(symbol, POSITION)
    adjusted, adj, reason = apply_relative_value_adjustment(symbol, final, hit, POSITION)

    gate_fail = (hit == "UPPER" and adjusted >= POSITION.bb5_upper_final_max) or \
                (hit == "LOWER" and adjusted <= POSITION.bb5_lower_final_min)
    if gate_fail:
        return

    _daily_bb_last_alert[symbol] = now

    title = f"{symbol} daily {hit} BB wick [POSITION]"
    body = (f"{symbol} daily candle (forming)\n"
            f"H: {lh:.6g}  L: {ll:.6g}  C: {live['close']:.6g}\n"
            f"Upper: {up:.6g}  SMA20: {mid:.6g}  Lower: {lo:.6g}\n"
            f"POSITION COMP: {adjusted:.2f} (raw {final:.2f}"
            f"{', RV '+format(adj,'+.0f') if adj else ''})\n"
            f"Candle: {datetime.fromtimestamp(int(live['time']), tz=timezone.utc):%Y-%m-%d %H:%M UTC} (forming)")
    send_ntfy_message(body, title=title, priority="high",
                      tags=["chart_with_upwards_trend"] if hit == "LOWER"
                           else ["chart_with_downwards_trend"],
                      system="POSITION")


def daily_bb_scanner_worker():
    time.sleep(30)
    while True:
        try:
            for s in BB5_SYMBOLS:
                if s in SKIP_SYMBOLS:
                    continue
                try:
                    check_daily_bb_for_symbol(s)
                except Exception as e:
                    _quiet_print(f"  [DBB] {s}: {e}")
                time.sleep(1)
        except Exception as e:
            _quiet_print(f"  [DBB] cycle: {e}")
        time.sleep(3600)


# ==============================================================================
# MOMENTUM + CONTRARIAN (shared)
# ==============================================================================
MOMENTUM_ENABLED = True
MOMENTUM_ALERT_THRESHOLD = 65
MOMENTUM_STRONG_THRESHOLD = 80
MOM_W_4H_ROC = 0.30
MOM_W_4H_EMA_SLOPE = 0.20
MOM_W_4H_RSI = 0.10
MOM_W_D_ROC = 0.20
MOM_W_D_EMA_SLOPE = 0.15
MOM_W_VOL_EXPANSION = 0.05
MOM_RSI_LONG_MIN = 52
MOM_RSI_SHORT_MAX = 48
MOM_MIN_RR_RATIO = 1.5
MOM_HORIZON_HOURS = 48
_momentum_cache = {}


def _ema(values, period):
    if not values or len(values) < period:
        return None
    k = 2.0 / (period + 1)
    e = values[0]
    for v in values[1:]:
        e = v * k + e * (1 - k)
    return e


def _rsi(closes, period=14):
    if len(closes) < period + 1:
        return None
    g, l = 0.0, 0.0
    for i in range(1, period + 1):
        d = closes[i] - closes[i-1]
        if d >= 0:
            g += d
        else:
            l -= d
    ag, al = g / period, l / period
    for i in range(period + 1, len(closes)):
        d = closes[i] - closes[i-1]
        g_ = d if d > 0 else 0.0
        l_ = -d if d < 0 else 0.0
        ag = (ag * (period - 1) + g_) / period
        al = (al * (period - 1) + l_) / period
    if al == 0:
        return 100.0
    return 100.0 - 100.0 / (1.0 + ag / al)


def _atr_from_candles(candles, period=14):
    if len(candles) < period + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["high"], candles[i]["low"], candles[i-1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(pc - l)))
    if len(trs) < period:
        return None
    atr = sum(trs[:period]) / period
    for i in range(period, len(trs)):
        atr = (atr * (period - 1) + trs[i]) / period
    return atr


def _roc(closes, n):
    if len(closes) <= n or closes[-n-1] == 0:
        return None
    return (closes[-1] - closes[-n-1]) / closes[-n-1] * 100.0


def _norm_score(v, scale):
    if v is None:
        return 50.0
    return 50.0 + (max(-scale, min(scale, v)) / scale) * 50.0


def compute_momentum_for_symbol(symbol):
    pair = ADDITIONAL_SYMBOLS.get(symbol)
    if not pair:
        return None
    d = fetch_kraken_ohlc_bb(pair, interval=1440, limit=100)
    h = fetch_kraken_ohlc_bb(pair, interval=240, limit=100)
    if not d or len(d) < 55 or not h or len(h) < 55:
        return None
    d = d[:-1]
    h = h[:-1]
    dc = [c["close"] for c in d]
    hc = [c["close"] for c in h]
    hv = [c.get("volume", 0.0) for c in h]
    d10 = _roc(dc, 10)
    de20 = _ema(dc, 20)
    de50 = _ema(dc, 50)
    ds = ((de20 - de50) / de50 * 100.0) if (de20 and de50 and de50 > 0) else None
    h6 = _roc(hc, 6)
    h18 = _roc(hc, 18)
    he20 = _ema(hc, 20)
    he50 = _ema(hc, 50)
    hs = ((he20 - he50) / he50 * 100.0) if (he20 and he50 and he50 > 0) else None
    hrsi = _rsi(hc, 14)
    hatr = _atr_from_candles(h, 14)
    ve = None
    if len(hv) >= 21:
        avg = sum(hv[-21:-1]) / 20.0
        if avg > 0:
            ve = hv[-1] / avg

    score = (MOM_W_4H_ROC * _norm_score(h18, 6.0)
             + MOM_W_4H_EMA_SLOPE * _norm_score(hs, 2.0)
             + MOM_W_4H_RSI * (hrsi if hrsi is not None else 50.0)
             + MOM_W_D_ROC * _norm_score(d10, 8.0)
             + MOM_W_D_EMA_SLOPE * _norm_score(ds, 4.0)
             + MOM_W_VOL_EXPANSION * (50.0 + max(0.0, min(1.0, (ve or 1.0) - 1.0)) * 50.0))

    direction = "NONE"
    conf = "WEAK"
    long_ok = ((h18 or 0) > 0 and (d10 or 0) > -1.0 and (he20 or 0) > (he50 or 0)
               and (hrsi or 50) >= MOM_RSI_LONG_MIN)
    short_ok = ((h18 or 0) < 0 and (d10 or 0) < 1.0 and (he20 or 0) < (he50 or 0)
                and (hrsi or 50) <= MOM_RSI_SHORT_MAX)
    if score >= MOMENTUM_ALERT_THRESHOLD and long_ok:
        direction = "LONG"
    elif score <= (100 - MOMENTUM_ALERT_THRESHOLD) and short_ok:
        direction = "SHORT"
    if score >= MOMENTUM_STRONG_THRESHOLD and long_ok:
        conf = "STRONG"
    elif direction == "LONG":
        conf = "MODERATE"
    elif score <= (100 - MOMENTUM_STRONG_THRESHOLD) and short_ok:
        conf = "STRONG"
    elif direction == "SHORT":
        conf = "MODERATE"

    rr = None
    if hatr and hatr > 0 and h6 is not None:
        exp_pct = h6 * (MOM_HORIZON_HOURS / 24.0)
        atr_pct = (hatr / hc[-1]) * 100.0
        horiz_atr = atr_pct * math.sqrt(MOM_HORIZON_HOURS / 4.0)
        if horiz_atr > 0:
            rr = abs(exp_pct) / horiz_atr
    if direction != "NONE" and (rr is None or rr < MOM_MIN_RR_RATIO):
        direction = "NONE"

    return {"symbol": symbol, "score": round(score, 1), "direction": direction,
            "conf": conf, "rr": round(rr, 2) if rr is not None else None,
            "h4_rsi": hrsi, "h4_candles": h, "h4_close": hc[-1] if hc else None,
            "h4_atr_pct": (hatr / hc[-1] * 100.0) if (hatr and hc) else None,
            "d_roc_10": d10, "h4_roc_18": h18, "h4_roc_6": h6}


def compute_all_momentum():
    out = []
    for s in ADDITIONAL_SYMBOLS:
        if s in SKIP_SYMBOLS:
            continue
        try:
            r = compute_momentum_for_symbol(s)
            if r:
                out.append(r)
        except Exception as e:
            _quiet_print(f"  [MOM] {s}: {e}")
    out.sort(key=lambda x: x["score"], reverse=True)
    _momentum_cache.clear()
    _momentum_cache.update({r["symbol"]: r for r in out})
    return out


def run_momentum_engine_and_report(send=True):
    if not MOMENTUM_ENABLED:
        return []
    try:
        results = compute_all_momentum()
        actionable = [r for r in results if r["direction"] != "NONE"]
        lines = [f"MOMENTUM SUMMARY - {datetime.now(timezone.utc):%Y-%m-%d %H:%M UTC}",
                 f"Actionable: {len(actionable)} / {len(results)}"]
        if actionable:
            lines.append("```")
            lines.append(f"{'SYM':<5} {'SCORE':>6} {'DIR':>6} {'CONF':>8} {'RR':>5}")
            for r in actionable:
                rr_s = f"{r['rr']:.2f}" if r["rr"] is not None else "-"
                lines.append(f"{r['symbol']:<5} {r['score']:6.1f} {r['direction']:>6} "
                             f"{r['conf']:>8} {rr_s:>5}")
            lines.append("```")
        if send:
            send_ntfy_message("\n".join(lines), title="Momentum Summary",
                              tags=["zap"], force=not _QUIET_MODE)
        return results
    except Exception as e:
        _quiet_print(f"  [MOM] FAILED: {e}")
        return []


# ==============================================================================
# FREQUENCY COUNTER (high / low touch density vs current price)
# ==============================================================================
FREQ_LOOKBACK_DAYS = 90
FREQ_TOLERANCE_PCT = 0.01
_freq_cache: Dict[str, dict] = {}
_freq_cache_lock = threading.Lock()
_freq_cache_ts = 0.0
FREQ_CACHE_MAX_AGE_SEC = 1100


def compute_high_low_frequency(symbol: str, current_price: float) -> Tuple[int, int]:
    if current_price is None or current_price <= 0:
        return 0, 0
    pair = KRAKEN_PAIR_MAP.get(symbol) or ADDITIONAL_SYMBOLS.get(symbol)
    if not pair:
        return 0, 0
    with _freq_cache_lock:
        entry = _freq_cache.get(symbol)
        if entry and (time.time() - entry.get("ts", 0.0)) < FREQ_CACHE_MAX_AGE_SEC:
            cached_px = entry.get("price", 0.0)
            if cached_px > 0 and abs(current_price - cached_px) / cached_px < 0.0005:
                return entry["high"], entry["low"]

    candles = fetch_kraken_ohlc(pair, interval=1440, days=FREQ_LOOKBACK_DAYS + 5)
    if not candles:
        return 0, 0
    cutoff = time.time() - FREQ_LOOKBACK_DAYS * 86400
    relevant = [c for c in candles if c["time"] >= cutoff]
    if not relevant:
        return 0, 0
    tol = FREQ_TOLERANCE_PCT
    high_cnt = sum(1 for c in relevant
                   if abs(c["high"] - current_price) / current_price <= tol)
    low_cnt = sum(1 for c in relevant
                  if abs(c["low"] - current_price) / current_price <= tol)
    with _freq_cache_lock:
        _freq_cache[symbol] = {
            "high": high_cnt, "low": low_cnt,
            "price": current_price, "ts": time.time(),
        }
    return high_cnt, low_cnt


# ==============================================================================
# TM/COMP MONITOR — the ONLY allowed output
# ==============================================================================
def _compute_tm_and_dual_comp_for_symbol(symbol):
    tm = _fetch_trademap_conclusion_score(symbol)
    live_comp, spot = _compute_live_comp_and_spot(symbol, force_fresh=True)
    daily_row = compute_daily_volume_vs_ma10(symbol)
    h4_row = compute_4h_volume_vs_ma10(symbol)
    daily_pct = daily_row.get("pct_of_ma10") if daily_row else None
    h4_pct = h4_row.get("pct_of_ma10") if (h4_row and not h4_row.get("error")) else None
    daily_wick = daily_row.get("wick_tag") if daily_row else None
    daily_pat = daily_row.get("pattern_tag") if daily_row else None
    adx, atr, fpctl, frate, _ = _get_cached_regime_and_funding(symbol)
    ctx = _cascade_context_cache.get(symbol, {}) or {}
    btc_tm = _fetch_trademap_conclusion_score("BTC")
    btc_comp, _ = _compute_live_comp_and_spot("BTC")

    results = {}
    for profile in (SHORT_TERM, POSITION):
        gex_amp, _ = _gex_amplifier_from_regime(ctx.get("spot"), ctx.get("gamma_flip"), profile)
        d = compute_final_composite_for_symbol(
            symbol=symbol, trademap_score=tm, trademap_score_btc=btc_tm,
            extended_composite_score=live_comp, extended_composite_score_btc=btc_comp,
            daily_vol_pct=daily_pct, h4_vol_pct=h4_pct,
            daily_wick_tag=daily_wick, daily_pattern_tag=daily_pat,
            skew_value=ctx.get("skew"), gex_amplifier=gex_amp,
            funding_rate=frate, funding_percentile=fpctl,
            adx_value=adx, atr_ratio=atr, profile=profile,
        )
        raw = d["final_score"]
        rv_adj, _, _ = apply_relative_value_adjustment(symbol, raw, "LOWER", profile)
        results[profile.name] = rv_adj
    return tm, results, spot


def _tm_comp_monitor_worker():
    if not TM_COMP_MONITOR_ENABLED:
        return
    def _out(m):
        print(m)
    _out(f"[TM/COMP MONITOR - DUAL] started ({TM_COMP_MONITOR_INTERVAL_SEC}s)")
    time.sleep(20)
    while True:
        try:
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            lines = [f"TM/COMP - DUAL SYSTEM - {ts}", "```",
                     f"{'SYM':<6} | {'TM':>6} | {'SHORT':>7} | {'POS':>7} | "
                     f"{'H-FQ':>4} | {'L-FQ':>4}"]
            for sym in ADDITIONAL_SYMBOLS:
                if sym in SKIP_SYMBOLS:
                    continue
                tm, comps, spot = _compute_tm_and_dual_comp_for_symbol(sym)
                tm_v = tm if tm is not None else 50.0
                short_v = comps.get("SHORT", 50.0)
                pos_v = comps.get("POSITION", 50.0)
                h_fq, l_fq = compute_high_low_frequency(sym, spot)
                line = (f"{sym:<6} | {tm_v:>6.2f} | {short_v:>7.2f} | {pos_v:>7.2f} | "
                        f"{h_fq:>4} | {l_fq:>4}")
                lines.append(line)
                _out(line)
                time.sleep(0.3)
            lines.append("```")
            send_ntfy_message("\n".join(lines), title=TM_COMP_MONITOR_TITLE,
                              priority=TM_COMP_MONITOR_PRIORITY,
                              tags=TM_COMP_MONITOR_TAGS, force=True)
        except Exception as e:
            print(f"[TM/COMP] error: {e}")
        time.sleep(TM_COMP_MONITOR_INTERVAL_SEC)


# ==============================================================================
# LIVE BB WICK ALERT WORKER  (integrated from WORKER.py)
# ==============================================================================
# Monitors the CURRENTLY-FORMING 4H candle for a touch of the upper/lower
# Bollinger Band (BB_PERIOD=20, BB_STD=2.0, computed from CLOSED candles).
# Fires to Discord via the same PA relay as send_discord_message().
# Independent thread — does NOT interfere with any main-script scan logic.
# ==============================================================================

WORKER_ALERT_ENABLED        = True
WORKER_ALERT_TIMEFRAME      = 60         # 4H in minutes
WORKER_ALERT_BB_PERIOD      = 20
WORKER_ALERT_BB_STD         = 2.0
WORKER_ALERT_WICK_THRESHOLD = 0.4          # informational only (last closed candle)
WORKER_ALERT_COOLDOWN       = 6000         # 100 min per pair / per side
WORKER_ALERT_CHECK_INTERVAL = 400          # 400s between poll cycles
WORKER_ALERT_LOOKBACK_DAYS  = 60

WORKER_ALERT_PAIRS = {
    "XXBTZUSD": "BTC/USD",
    "XETHZUSD": "ETH/USD",
    "XXMRZUSD": "XMR/USD",
}

_worker_alert_last_ts: Dict[str, float] = {}          # key = "{pair}_{side}"
_worker_alert_last_live_candle: Dict[str, int] = {}   # key = "{pair}_{side}"
_worker_alert_verified_pairs: Dict[str, str] = {}     # filled on startup


def _worker_alert_bb(candles, period=WORKER_ALERT_BB_PERIOD,
                     std_dev=WORKER_ALERT_BB_STD):
    if len(candles) < period:
        return None, None, None
    closes = [c["close"] for c in candles[-period:]]
    sma = sum(closes) / period
    var = sum((x - sma) ** 2 for x in closes) / period
    std = var ** 0.5
    return sma + std_dev * std, sma, sma - std_dev * std


def _worker_alert_wick_info(candle):
    o = float(candle["open"]); h = float(candle["high"])
    l = float(candle["low"]);  c = float(candle["close"])
    body = abs(c - o)
    if body == 0:
        return 1.0, 1.0
    return (h - max(o, c)) / body, (min(o, c) - l) / body


def _worker_alert_verify_pairs():
    """Ping Kraken once per pair to confirm connectivity; keep working pairs."""
    global _worker_alert_verified_pairs
    ok = {}
    for pair_code, pair_label in WORKER_ALERT_PAIRS.items():
        _quiet_print(f"  [WORKER-ALERT] testing {pair_code} ({pair_label})...")
        candles = fetch_kraken_ohlc_bb(
            pair_code, interval=WORKER_ALERT_TIMEFRAME,
            limit=WORKER_ALERT_BB_PERIOD + 5,
        )
        if candles and len(candles) >= WORKER_ALERT_BB_PERIOD + 1:
            ok[pair_code] = pair_label
            _quiet_print(f"    ✅ {pair_code}: {len(candles)} candles")
        else:
            _quiet_print(f"    ❌ {pair_code}: insufficient / failed")
        time.sleep(0.4)
    _worker_alert_verified_pairs = ok
    return ok


def _worker_alert_send_discord(msg):
    """Route through the same PA relay path as send_discord_message()."""
    if not msg:
        return
    try:
        RELAY.send(
            title="BB Live Touch",
            body=_sanitize_ascii(msg),
            priority="high",
            tags="discord",
            target="discord",
        )
    except Exception as e:
        print(f"  [WORKER-ALERT] relay error: {e}")


def _worker_alert_check_pair(pair_code, pair_name):
    """
    Check the forming 4H candle against BBs built from CLOSED candles only.
    Fires on touch (high >= upper or low <= lower).  Adds a note about the
    last CLOSED 4H candle's wick status (informational).
    """
    try:
        candles = fetch_kraken_ohlc_bb(
            pair_code, interval=WORKER_ALERT_TIMEFRAME,
            limit=max(WORKER_ALERT_BB_PERIOD * 4, 100),
        )
        if not candles or len(candles) < WORKER_ALERT_BB_PERIOD + 2:
            _quiet_print(f"  [WORKER-ALERT] {pair_name}: insufficient candles")
            return

        closed = candles[:-1]
        live = candles[-1]
        last_closed = closed[-1]

        upper_bb, middle_bb, lower_bb = _worker_alert_bb(closed)
        if upper_bb is None:
            return

        now = time.time()
        live_high = float(live["high"])
        live_low = float(live["low"])
        live_open = float(live["open"])
        live_close = float(live["close"])
        live_time = int(live["time"])

        uw, lw = _worker_alert_wick_info(last_closed)
        upper_wick_active = uw > WORKER_ALERT_WICK_THRESHOLD
        lower_wick_active = lw > WORKER_ALERT_WICK_THRESHOLD

        def _emit(side, wick_active, wick_ratio, bb_value):
            key = f"{pair_code}_{side}"
            if key in _worker_alert_last_ts and \
                    (now - _worker_alert_last_ts[key]) <= WORKER_ALERT_COOLDOWN:
                return
            if _worker_alert_last_live_candle.get(key) == live_time:
                return

            wick_note = (
                f"Last closed 4H {'upper' if side=='upper' else 'lower'} "
                f"wick: {'ACTIVE' if wick_active else 'not active'} "
                f"(ratio {wick_ratio:.2f})"
            )
            arrow = "🟢 UPPER" if side == "upper" else "🔴 LOWER"
            msg = (
                f"🚨 **BB TOUCH: {pair_name}**\n"
                f"{arrow} Bollinger Band touched on LIVE 4H candle\n"
                f"```\n"
                f"Live O: ${live_open:,.2f}  H: ${live_high:,.2f}\n"
                f"     L: ${live_low:,.2f}  C: ${live_close:,.2f}\n"
                f"{'Upper' if side=='upper' else 'Lower'} BB: ${bb_value:,.2f}\n"
                f"Candle: {datetime.fromtimestamp(live_time, tz=timezone.utc):%Y-%m-%d %H:%M UTC} (forming)\n"
                f"{wick_note}\n"
                f"```"
            )
            print(f"\n[WORKER-ALERT {datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S UTC}]\n{msg}")
            _worker_alert_send_discord(msg)
            _worker_alert_last_ts[key] = now
            _worker_alert_last_live_candle[key] = live_time

        if live_high >= upper_bb:
            _emit("upper", upper_wick_active, uw, upper_bb)
        if live_low <= lower_bb:
            _emit("lower", lower_wick_active, lw, lower_bb)

    except Exception as e:
        _quiet_print(f"  [WORKER-ALERT] {pair_name}: {e}")


def worker_live_bb_alert_worker():
    if not WORKER_ALERT_ENABLED:
        return
    print("=" * 60)
    print("LIVE BB TOUCH ALERT WORKER (integrated)")
    print(f"  Timeframe:     {WORKER_ALERT_TIMEFRAME}m (4H)")
    print(f"  BB:            {WORKER_ALERT_BB_PERIOD}/{WORKER_ALERT_BB_STD}")
    print(f"  Cooldown:      {WORKER_ALERT_COOLDOWN // 60} min per pair / side")
    print(f"  Poll interval: {WORKER_ALERT_CHECK_INTERVAL}s")
    print(f"  Pairs:         {list(WORKER_ALERT_PAIRS.values())}")
    print("=" * 60)
    time.sleep(8)
    verified = _worker_alert_verify_pairs()
    if not verified:
        print("  [WORKER-ALERT] no working pairs — worker idle.")
        return
    _worker_alert_send_discord(
        f"⚙️ **BB Live Touch Alert Worker Started**\n"
        f"Watching `{', '.join(verified.values())}` on **4H** candles.\n"
        f"Alerts fire on live-candle band touch (no close wait)."
    )
    check_count = 0
    while True:
        try:
            check_count += 1
            for pair_code, pair_name in verified.items():
                _worker_alert_check_pair(pair_code, pair_name)
                time.sleep(0.6)
            if check_count % 20 == 0:
                _quiet_print(
                    f"[WORKER-ALERT {datetime.now(timezone.utc):%H:%M:%S UTC}] "
                    f"active (cycle #{check_count})"
                )
            time.sleep(WORKER_ALERT_CHECK_INTERVAL)
        except Exception as e:
            print(f"  [WORKER-ALERT] loop error: {e}")
            time.sleep(WORKER_ALERT_CHECK_INTERVAL)


# ==============================================================================
# MAIN (original)
# ==============================================================================
def main():
    print("=" * 70)
    print(f"  DUAL-SYSTEM SCANNER (SIGNAL-ONLY) - {datetime.now(timezone.utc):%Y-%m-%d %H:%M UTC}")
    print(f"  QUIET={_QUIET_MODE}")
    print(f"  SYSTEMS: SHORT(w={SHORT_TERM.weight_tm}/{SHORT_TERM.weight_comp}) "
          f"POSITION(w={POSITION.weight_tm}/{POSITION.weight_comp})")
    print("=" * 70)

    threading.Thread(target=live_comp_sampler_worker, daemon=True).start()
    threading.Thread(target=bb5_scanner_worker, daemon=True).start()
    threading.Thread(target=daily_bb_scanner_worker, daemon=True).start()
    if WORKER_ALERT_ENABLED:
        threading.Thread(target=worker_live_bb_alert_worker, daemon=True).start()
    if RELATIVE_VALUE_ENABLED:
        threading.Thread(target=_rv_worker, daemon=True).start()
    if TM_COMP_MONITOR_ENABLED:
        threading.Thread(target=_tm_comp_monitor_worker, daemon=True).start()

    # ---- start trade-engine block in parallel (independent) ----
    threading.Thread(target=trade_engine_main, daemon=True).start()

    while True:
        print(f"\n[{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S UTC}] CYCLE")

        try:
            build_cascade_context_cache()
        except Exception as e:
            print(f"  [STEP1] {e}")

        daily_rows = []
        for s in ADDITIONAL_SYMBOLS:
            if s in SKIP_SYMBOLS:
                continue
            r = compute_daily_volume_vs_ma10(s)
            daily_rows.append(r if r else {"symbol": s, "error": True})

        h4_rows = []
        for s in ADDITIONAL_SYMBOLS:
            if s in SKIP_SYMBOLS:
                continue
            r = compute_4h_volume_vs_ma10(s)
            h4_rows.append(r if r else {"symbol": s, "error": True})

        tm_scores = {}
        for c in ["BTC", "ETH", "HYPE", "XRP", "SOL"]:
            tm_scores[c] = _fetch_trademap_conclusion_score(c)
            time.sleep(0.5)
        if tm_scores.get("BTC") is None:
            tm_scores["BTC"] = 50.0
        for c in ADDITIONAL_SYMBOLS:
            if c in SKIP_SYMBOLS or c in tm_scores:
                continue
            tm_scores[c] = tm_scores["BTC"]

        try:
            run_dual_scan_and_alert(daily_rows, h4_rows, tm_scores,
                                     send_report=True, send_alerts=True)
        except Exception as e:
            print(f"  [SCAN] {e}")

        try:
            run_momentum_engine_and_report(send=True)
        except Exception as e:
            print(f"  [MOM] {e}")

        print("  Next cycle in 60m")
        time.sleep(3600)


# ==============================================================================
# ==============================================================================
# TRADE ENGINE BLOCK (independent, runs in parallel)
# ==============================================================================
# ==============================================================================
"""
UNIFIED BB + RELATIVE-VALUE + LADDER TRADING MONITOR
=====================================================
- Relative value: ranks all coins by over/under valuation votes
  MO = ranked[0] (highest composite) IF over  votes >= RV_MIN_VOTES (3)
  MU = ranked[-1] (lowest composite)  IF under votes >= RV_MIN_VOTES (3)
- Bollinger bands on 5-MINUTE candles for BTC & ETH trigger ladder scripts
- BB_ALERT_COOLDOWN=300s per coin on top of per-candle dedup
- MARKET_BIAS final gate: 0=bearish (only MO), 1=bullish (only MU), 2=neutral (both)
- Each coin may invoke at most ONE ladder script, ONCE, ever
- Max 2 ladder executions per script run (one per coin)

RV CACHING:
  - 180 days of 1H candles fetched ONCE at startup for each RV coin
  - Betas pre-computed and cached in RV_PAIRS_CACHE
  - Every 6h the history is re-fetched to keep betas fresh
  - Each main loop iteration only pulls live tickers (\'8 HTTP calls)

GRACEFUL VENUE DEGRADATION:
  - ASTER_ENABLED / DERIBIT_ENABLED flags toggle each venue
  - If Aster fails (e.g. PythonAnywhere tunnel block), Deribit still runs
  - If Deribit fails, Aster still runs
  - Coin is locked only if AT LEAST ONE venue succeeds
  - If BOTH venues fail, coin is NOT locked and retries next loop
  - Discord message reports per-venue status on every execution

BRIDGE TO MAIN SCRIPT:
  - Final composite score (SHORT system) is pulled live via get_main_final_composite()
  - Additional gate applied AFTER the original 3 gates, BEFORE ladder execution:
      UPPER + MO  (BTC or ETH)  -> score must be < 30
      LOWER + MU  (BTC or ETH)  -> score must be > 70
"""

from eth_account import Account

# ============================================================================
# MASTER CONFIGURATION (trade engine)
# ============================================================================

# -------- MARKET BIAS (USER TOGGLE) --------
# 0 = BEARISH  -> only UPPER band + MO trades allowed
# 1 = BULLISH  -> only LOWER band + MU trades allowed
# 2 = NEUTRAL  -> both directions allowed
MARKET_BIAS = 2

NUM_ORDERS = 1
TP_OFF = 0.03

# -------- Venue enable flags --------
ASTER_ENABLED   = True
DERIBIT_ENABLED = True

# -------- Bollinger config --------
TIMEFRAME = 240               # 240 MINUTE candles on Kraken
BB_PERIOD_TE = 20
BB_STD_TE = 2.0
CHECK_INTERVAL_SECONDS = 100
LOOKBACK_DAYS_TE = 5
BB_ALERT_COOLDOWN = 300

# -------- Relative value config --------
RV_TIMEFRAME = 60
RV_LOOKBACK_DAYS_TE = 180
RV_Z_THRESHOLD_1 = 2
RV_Z_THRESHOLD_2 = 4
RV_MIN_VOTES = 3
RV_REFRESH_SECONDS = 6 * 3600

RV_CANDIDATE_PAIRS = {
    "XXBTZUSD": "BTC",
    "XETHZUSD": "ETH",
    "SOLUSD":   "SOL",
    "XXMRZUSD": "XMR",
}

BB_PAIRS = {
    "XXBTZUSD": "BTC/USD",
    "XETHZUSD": "ETH/USD",
}

# -------- Discord --------
DISCORD_WEBHOOK_URL = os.environ.get(
    "DISCORD_WEBHOOK_URL",
    "https://discord.com/api/webhooks/1545725722419666986/JKIvjJpCR0Hrp2H6DFvPM0VmSz1HZ-zciVKgmCtbcRg-U3B5Jq9DvJiOlcZZ-DF1CA3T"
)
DISCORD_RATE_LIMIT_DELAY = 5
_last_discord_send = 0.0


def send_discord_message(content):
    """Send Discord notification via the same PA relay (target=discord)."""
    global _last_discord_send
    if not content:
        return
    delta = time.time() - _last_discord_send
    if delta < DISCORD_RATE_LIMIT_DELAY:
        time.sleep(DISCORD_RATE_LIMIT_DELAY - delta)
    try:
        ok = RELAY.send(
            title="Trade Map",
            body=_sanitize_ascii(content),
            priority="default",
            tags="discord",
            target="discord",
        )
        _last_discord_send = time.time()
        if not ok:
            print("  ⚠️ Discord relay delivery failed")
    except Exception as e:
        print(f"  ⚠️ Discord relay error: {e}")


# ============================================================================
# LOCKOUT STATE
# ============================================================================
COIN_SCRIPT_LOCKOUT = {"BTC": False, "ETH": False}
SCRIPT_EXECUTED_COUNT = 0
MAX_SCRIPT_EXECUTIONS = 2

_bb_alerted = {}
_last_bb_fire = {}


def coin_locked(coin):
    return COIN_SCRIPT_LOCKOUT.get(coin, True)


def lock_coin(coin):
    COIN_SCRIPT_LOCKOUT[coin] = True


# ============================================================================
# KRAKEN DATA (trade-engine local)
# ============================================================================
def te_fetch_ohlc(pair, interval, days):
    url = f"{KRAKEN_BASE_URL}/0/public/OHLC"
    since = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())
    all_candles, current_since = [], since
    try:
        while True:
            r = requests.get(url, params={"pair": pair, "interval": interval,
                                          "since": current_since}, timeout=30)
            data = r.json()
            if data.get("error"):
                return []
            if "result" not in data or not data["result"]:
                return []
            rk = [k for k in data["result"].keys() if k != "last"][0]
            candles = data["result"][rk]
            if not candles:
                break
            all_candles.extend(candles)
            if len(candles) < 720:
                break
            current_since = int(candles[-1][0]) + interval * 60
            time.sleep(0.15)
    except Exception as e:
        print(f"  ⚠️ te_fetch_ohlc {pair}: {e}")
        return []
    seen, unique = set(), []
    for c in all_candles:
        if c[0] not in seen:
            seen.add(c[0])
            unique.append(c)
    unique.sort(key=lambda x: x[0])
    return [{
        "time":     c[0],
        "time_str": datetime.fromtimestamp(c[0], tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "open":     float(c[1]), "high": float(c[2]), "low": float(c[3]),
        "close":    float(c[4]), "vwap": float(c[5]),
        "volume":   float(c[6]), "count": int(c[7]),
    } for c in unique]


def te_fetch_latest_ticker(pair):
    try:
        r = requests.get(f"{KRAKEN_BASE_URL}/0/public/Ticker",
                         params={"pair": pair}, timeout=10)
        data = r.json()
        if data.get("error"):
            return None
        rk = list(data["result"].keys())[0]
        return float(data["result"][rk]["c"][0])
    except Exception:
        return None


# ============================================================================
# BOLLINGER BANDS (trade-engine local)
# ============================================================================
def te_calculate_bollinger_bands(candles, period=BB_PERIOD_TE, std_dev=BB_STD_TE):
    if len(candles) < period:
        return None, None, None
    closes = [c["close"] for c in candles[-period:]]
    sma = sum(closes) / period
    var = sum((x - sma) ** 2 for x in closes) / period
    std = var ** 0.5
    return sma + std_dev * std, sma, sma - std_dev * std


# ============================================================================
# RELATIVE VALUE MODULE (trade-engine)
# ============================================================================
def rv_mean(x):  return sum(x) / len(x) if x else 0.0

def rv_covariance(x, y):
    n = len(x)
    if n < 2: return 0.0
    mx, my = rv_mean(x), rv_mean(y)
    return sum((x[i]-mx)*(y[i]-my) for i in range(n)) / (n-1)

def rv_variance(x):  return rv_covariance(x, x)
def rv_std(x):       return math.sqrt(rv_variance(x))

def calc_live_z(logs1, logs2, beta, p1, p2):
    spread = [logs1[i] - beta * logs2[i] for i in range(len(logs1))]
    m, s = rv_mean(spread), rv_std(spread)
    if s == 0:
        return 0.0
    cur = math.log(p1) - beta * math.log(p2)
    return (cur - m) / s


def build_rv_pairs(price_data):
    labels = list(price_data.keys())
    combos = list(itertools.combinations(labels, 2))
    pairs = []
    for a1, a2 in combos:
        d1, d2 = price_data[a1], price_data[a2]
        common = sorted(set(d1.keys()) & set(d2.keys()))
        if len(common) < 120:
            continue
        c1 = [d1[t] for t in common]
        c2 = [d2[t] for t in common]
        l1 = [math.log(p) for p in c1]
        l2 = [math.log(p) for p in c2]
        v2 = rv_variance(l2)
        if v2 == 0:
            continue
        beta = rv_covariance(l1, l2) / v2
        pairs.append({
            "asset1": a1, "asset2": a2,
            "pair": f"{a1}/{a2}",
            "beta": beta,
            "logs1": l1,
            "logs2": l2,
        })
    return pairs


RV_PRICE_DATA = {}
RV_PAIRS_CACHE = []
_last_rv_refresh = 0.0


def rv_build_cache():
    global RV_PRICE_DATA, RV_PAIRS_CACHE, _last_rv_refresh

    print("\n" + "="*70)
    print(f"RV CACHE REFRESH (RV_TIMEFRAME={RV_TIMEFRAME}m, "
          f"{RV_LOOKBACK_DAYS_TE}d lookback)")
    print("="*70)

    new_price_data = {}
    for code, label in RV_CANDIDATE_PAIRS.items():
        candles = te_fetch_ohlc(code, RV_TIMEFRAME, RV_LOOKBACK_DAYS_TE)
        if candles:
            new_price_data[label] = {c["time"]: c["close"] for c in candles}
            print(f"  ✅ {label:<4} ({code}): {len(candles)} candles")
        else:
            print(f"  ❌ {label:<4} ({code}): failed")
        time.sleep(0.2)

    if len(new_price_data) < 2:
        print("  ⚠️ Insufficient price data — keeping old cache if any.")
        return False

    RV_PRICE_DATA = new_price_data
    RV_PAIRS_CACHE = build_rv_pairs(RV_PRICE_DATA)
    _last_rv_refresh = time.time()

    print(f"  ✅ RV cache built: {len(RV_PAIRS_CACHE)} pairs "
          f"from {len(RV_PRICE_DATA)} coins.")
    return True


def compute_relative_value_summary(live_z_map):
    if not live_z_map:
        return {}
    votes = {}
    over_under = {}
    for pair_name, z in live_z_map.items():
        if "/" not in pair_name:
            continue
        a1, a2 = pair_name.split("/", 1)
        votes.setdefault(a1, []).append(z)
        votes.setdefault(a2, []).append(-z)
        over_under.setdefault(a1, [0, 0])
        over_under.setdefault(a2, [0, 0])
        if z > 0:
            over_under[a1][0] += 1
            over_under[a2][1] += 1
        elif z < 0:
            over_under[a1][1] += 1
            over_under[a2][0] += 1

    summary = {}
    for coin, contribs in votes.items():
        n = len(contribs)
        if n == 0:
            continue
        mean_z = sum(contribs) / n
        sc = sorted(contribs)
        median_z = sc[n // 2] if n % 2 else (sc[n//2 - 1] + sc[n//2]) / 2.0
        composite = 0.5 * mean_z + 0.5 * median_z
        over, under = over_under.get(coin, [0, 0])
        summary[coin] = {
            "mean_z": mean_z, "median_z": median_z,
            "composite": composite, "pairs": n,
            "over": over, "under": under,
        }
    return summary


def get_mo_mu_candidates(summary):
    if not summary:
        return None, None
    ranked = sorted(summary.items(), key=lambda kv: -kv[1]["composite"])
    top_over_coin,  top_over_stats  = ranked[0]
    top_under_coin, top_under_stats = ranked[-1]

    mo = None
    if top_over_stats["over"] >= RV_MIN_VOTES and top_over_stats["composite"] > 0:
        mo = top_over_coin

    mu = None
    if top_under_stats["under"] >= RV_MIN_VOTES and top_under_stats["composite"] < 0:
        mu = top_under_coin

    return mo, mu


def run_relative_value_scan():
    global _last_rv_refresh

    if (not RV_PAIRS_CACHE) or (time.time() - _last_rv_refresh > RV_REFRESH_SECONDS):
        rv_build_cache()

    if not RV_PAIRS_CACHE:
        print("\n  ⚠️ RV cache empty — skipping RV scan.")
        return None, None, {}

    print("\n" + "="*70)
    print(f"RELATIVE VALUE SCAN (cached 1H history, live tickers)")
    print(f"   cache age: {int(time.time() - _last_rv_refresh)}s | "
          f"pairs: {len(RV_PAIRS_CACHE)}")
    print("="*70)

    label_to_kraken = {label: code for code, label in RV_CANDIDATE_PAIRS.items()}
    live_z_map = {}

    for p in RV_PAIRS_CACHE:
        a1, a2 = p["asset1"], p["asset2"]
        code1 = label_to_kraken.get(a1)
        code2 = label_to_kraken.get(a2)
        p1 = te_fetch_latest_ticker(code1)
        p2 = te_fetch_latest_ticker(code2)
        if p1 is None or p2 is None:
            continue
        z = calc_live_z(p["logs1"], p["logs2"], p["beta"], p1, p2)
        live_z_map[p["pair"]] = z

    summary = compute_relative_value_summary(live_z_map)
    mo, mu = get_mo_mu_candidates(summary)

    ranked = sorted(summary.items(), key=lambda kv: -kv[1]["composite"])
    print(f"\n  {'COIN':<6}{'COMPOSITE':>11}{'OVER':>7}{'UNDER':>7}  VERDICT")
    for coin, s in ranked:
        v = "OVERVALUED" if s["composite"] > 0 else ("UNDERVALUED" if s["composite"] < 0 else "FAIR")
        q = ""
        if s["over"]  >= RV_MIN_VOTES and s["composite"] > 0: q = "✔MO-qualified"
        if s["under"] >= RV_MIN_VOTES and s["composite"] < 0: q = "✔MU-qualified"
        print(f"  {coin:<6}{s['composite']:>+11.3f}{s['over']:>7}{s['under']:>7}  {v} {q}")

    if ranked:
        top_over_coin,  top_over_stats  = ranked[0]
        top_under_coin, top_under_stats = ranked[-1]
        print("\n  HEADLINE")
        print(f"    Most OVERVALUED coin : {top_over_coin}  "
              f"(composite {top_over_stats['composite']:+.3f}, "
              f"{top_over_stats['over']} over / {top_over_stats['under']} under votes)")
        print(f"    Most UNDERVALUED coin: {top_under_coin}  "
              f"(composite {top_under_stats['composite']:+.3f}, "
              f"{top_under_stats['over']} over / {top_under_stats['under']} under votes)")

    print(f"\n  → MO (after ≥{RV_MIN_VOTES} vote gate): {mo}")
    print(f"  → MU (after ≥{RV_MIN_VOTES} vote gate): {mu}")
    return mo, mu, summary


# ============================================================================
# ASTER (AsterDex)
# ============================================================================
ASTER_PRIVATE_KEY = "0xc6651f67f803cc8cb8a2d84ae93438cbb0adfd3226dc84cfeab9972b162dc1e2"
ASTER_USER_ADDRESS = "0x8b2383a0acdf441671cf0a6c0e038dab068973d5"
ASTER_BASE_URL = "https://fapi.asterdex.com"

_aster_account = Account.from_key(ASTER_PRIVATE_KEY)
ASTER_SIGNER_ADDRESS = _aster_account.address

ASTER_ORDER_DELAY = 0.5
DERIBIT_TP_DELAY = 5


def aster_round_step(value, step):
    precision = int(round(-math.log10(step)))
    return round(math.floor(value / step) * step, precision)


def aster_round_step_up(value, step):
    precision = int(round(-math.log10(step)))
    return round(math.ceil(value / step) * step, precision)


def aster_sign_params(params):
    sorted_items = sorted(params.items())
    query_string = "&".join(f"{k}={v}" for k, v in sorted_items)
    message = {
        "types": {
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
                {"name": "verifyingContract", "type": "address"},
            ],
            "Message": [{"name": "msg", "type": "string"}],
        },
        "primaryType": "Message",
        "domain": {
            "name": "AsterSignTransaction", "version": "1",
            "chainId": 1666,
            "verifyingContract": "0x0000000000000000000000000000000000000000",
        },
        "message": {"msg": query_string},
    }
    signed = _aster_account.sign_typed_data(full_message=message)
    sig = signed.signature.hex()
    if not sig.startswith("0x"):
        sig = "0x" + sig
    return sig, query_string


def aster_place_limit_order(symbol, side, price, quantity,
                            tif="GTC", reduce_only=False):
    if not ASTER_ENABLED:
        return None
    params = {
        "symbol": symbol, "side": side.upper(), "type": "LIMIT",
        "timeInForce": tif, "quantity": str(quantity), "price": str(price),
        "user": ASTER_USER_ADDRESS, "signer": ASTER_SIGNER_ADDRESS,
        "nonce": str(int(time.time() * 1_000_000)),
    }
    if reduce_only:
        params["reduceOnly"] = "true"
    sig, signed_q = aster_sign_params(params)
    payload = f"{signed_q}&signature={sig}"
    try:
        r = requests.post(f"{ASTER_BASE_URL}/fapi/v3/order",
                          data=payload,
                          headers={"Content-Type": "application/x-www-form-urlencoded"},
                          timeout=10)
        print(f"    [Aster] HTTP {r.status_code}: {r.text[:180]}")
        return r.json()
    except Exception as e:
        print(f"    [Aster] request failed: {e}")
        return None


def aster_get_price(symbol):
    if not ASTER_ENABLED:
        return None
    try:
        r = requests.get(f"{ASTER_BASE_URL}/fapi/v1/ticker/price",
                         params={"symbol": symbol}, timeout=5)
        print(f"    [Aster ticker] HTTP {r.status_code}: {r.text[:180]}")
        data = r.json()
        if "price" in data:
            return float(data["price"])
        print(f"    [Aster ticker] no 'price' key in response")
        return None
    except Exception as e:
        print(f"    [Aster ticker] exception: {e}")
        return None


# ============================================================================
# DERIBIT SHARED HELPERS
# ============================================================================
DERIBIT_CLIENT_ID = "7mMDZbd0"
DERIBIT_CLIENT_SECRET = "aW1qBxVSfBuVL-Nmb6UCb0xZp1Csn47KaknHRyXEORA"
DERIBIT_BASE_URL = "https://www.deribit.com"
DERIBIT_SET_LEVERAGE = False
DERIBIT_LEVERAGE = 5

_deribit_token = {"access_token": None, "expires_at": 0}
DERIBIT_TICK_SIZE = {}


def deribit_get_token():
    now = time.time()
    if _deribit_token["access_token"] and _deribit_token["expires_at"] > now + 30:
        return _deribit_token["access_token"]
    try:
        r = requests.get(f"{DERIBIT_BASE_URL}/api/v2/public/auth",
                         params={"grant_type": "client_credentials",
                                 "client_id": DERIBIT_CLIENT_ID,
                                 "client_secret": DERIBIT_CLIENT_SECRET},
                         timeout=15)
        data = r.json()
        if "error" in data:
            print(f"    ❌ Deribit auth: {data['error']}")
            return None
        res = data.get("result", {})
        tok = res.get("access_token")
        exp = int(res.get("expires_in", 900))
        _deribit_token["access_token"] = tok
        _deribit_token["expires_at"] = now + exp
        return tok
    except Exception as e:
        print(f"    ❌ Deribit auth exc: {e}")
        return None


def deribit_headers():
    t = deribit_get_token()
    if not t:
        return None
    return {"Authorization": f"Bearer {t}", "Content-Type": "application/json"}


def deribit_fetch_meta(instrument):
    if instrument in DERIBIT_TICK_SIZE:
        return
    try:
        r = requests.get(f"{DERIBIT_BASE_URL}/api/v2/public/get_instrument",
                         params={"instrument_name": instrument}, timeout=10)
        d = r.json()
        if "error" in d:
            print(f"    ⚠️ Deribit meta {instrument}: {d['error']}")
            DERIBIT_TICK_SIZE[instrument] = 0.01
            return
        res = d.get("result", {})
        DERIBIT_TICK_SIZE[instrument] = float(res.get("tick_size", 0.01))
        print(f"    ✅ {instrument} tick_size={DERIBIT_TICK_SIZE[instrument]}")
    except Exception as e:
        print(f"    ⚠️ Deribit meta exc {instrument}: {e}")
        DERIBIT_TICK_SIZE[instrument] = 0.01


def deribit_round_tick(instrument, price):
    tick = DERIBIT_TICK_SIZE.get(instrument, 0.01)
    if tick <= 0:
        return round(price, 2)
    rounded = round(price / tick) * tick
    decimals = max(0, -int(round(math.log10(tick))))
    return round(rounded, decimals)


def deribit_entry(instrument, direction, price, amount):
    if not DERIBIT_ENABLED:
        return None
    path = "/api/v2/private/buy" if direction == "buy" else "/api/v2/private/sell"
    h = deribit_headers()
    if not h:
        return None
    payload = {
        "instrument_name": instrument, "amount": amount,
        "type": "limit", "price": price,
        "time_in_force": "good_til_cancelled",
        "label": f"ladder_{int(time.time()*1000)}",
    }
    try:
        r = requests.get(f"{DERIBIT_BASE_URL}{path}", headers=h,
                         params=payload, timeout=15)
        d = r.json()
        if "error" in d:
            print(f"    ⚠️ Deribit entry: {d['error']}")
            return d
        o = d.get("result", {}).get("order", {})
        print(f"    ✅ Deribit entry: {o.get('order_id')} {direction} {amount} @ {price}")
        return d
    except Exception as e:
        print(f"    ⚠️ Deribit entry exc: {e}")
        return None


def deribit_tp(instrument, entry_direction, amount, tp_price):
    if not DERIBIT_ENABLED:
        return None
    close_dir = "sell" if entry_direction == "buy" else "buy"
    h = deribit_headers()
    if not h:
        return None
    payload = {
        "instrument_name": instrument, "amount": amount,
        "type": "take_market", "trigger": "mark_price",
        "trigger_price": tp_price, "reduce_only": "true",
        "label": f"tp_{int(time.time()*1000)}",
    }
    try:
        r = requests.get(f"{DERIBIT_BASE_URL}/api/v2/private/{close_dir}",
                         headers=h, params=payload, timeout=15)
        d = r.json()
        if "error" in d:
            print(f"    ⚠️ Deribit TP: {d['error']}")
            return d
        o = d.get("result", {}).get("order", {})
        print(f"    ✅ Deribit TP: {o.get('order_id')} {close_dir} {amount} @ {tp_price}")
        return d
    except Exception as e:
        print(f"    ⚠️ Deribit TP exc: {e}")
        return None


# ============================================================================
# BTC LADDER — SELL (MO trigger)
# ============================================================================
def run_btc_sell_ladder():
    sym = "BTCUSDT"
    deribit_instrument = "BTC_USDC-PERPETUAL"
    aster_ok = False
    deribit_ok = False

    print("\n" + "="*70)
    print("  BTC SELL LADDER (triggered by MO)")
    print("="*70)

    if ASTER_ENABLED:
        try:
            last = aster_get_price(sym)
            if last is None:
                print("  ⚠️ Aster: could not fetch BTC price — skipping Aster leg.")
                send_discord_message("⚠️ **BTC SELL LADDER — Aster leg skipped** (no price)")
            else:
                print(f"  BTC last (Aster): {last}")
                PRICE_TICK = 0.1; QTY_STEP = 0.001; MIN_NOTIONAL = 5.5
                min_qty = aster_round_step_up(MIN_NOTIONAL / last, QTY_STEP)
                quantity = max(aster_round_step(0.001, QTY_STEP), min_qty)

                rungs = []
                for n in range(1, NUM_ORDERS + 1):
                    sell_offset = 0.0005 + 0.0005 * (n - 1)
                    tp_offset = TP_OFF * (NUM_ORDERS + 1 - n)
                    rungs.append({
                        "n": n,
                        "sell_price": aster_round_step(last * (1 + sell_offset), PRICE_TICK),
                        "tp_price":   aster_round_step(last * (1 - tp_offset), PRICE_TICK),
                    })

                for o in rungs:
                    print(f"  Aster SELL #{o['n']} @ {o['sell_price']}")
                    aster_place_limit_order(sym, "SELL", o["sell_price"], quantity)
                    time.sleep(ASTER_ORDER_DELAY)
                    print(f"  Aster TP   #{o['n']} @ {o['tp_price']}")
                    aster_place_limit_order(sym, "BUY", o["tp_price"], quantity, reduce_only=False)
                    time.sleep(ASTER_ORDER_DELAY)
                aster_ok = True
        except Exception as e:
            print(f"  ❌ Aster leg failed: {e}")
            send_discord_message(f"❌ **BTC SELL LADDER — Aster leg failed**\n`{str(e)[:200]}`")
    else:
        print("  ⏭️ ASTER_ENABLED=False — skipping Aster leg.")

    if DERIBIT_ENABLED:
        try:
            last_ref = aster_get_price(sym)
            if last_ref is None:
                last_ref = te_fetch_latest_ticker("XXBTZUSD")
            if last_ref is None:
                print("  ⚠️ Deribit: no reference price available — skipping Deribit leg.")
                send_discord_message("⚠️ **BTC SELL LADDER — Deribit leg skipped** (no ref price)")
            else:
                deribit_fetch_meta(deribit_instrument)
                if deribit_get_token() is None:
                    print("  ⚠️ Deribit auth failed — skipping Deribit leg.")
                    send_discord_message("⚠️ **BTC SELL LADDER — Deribit auth failed**")
                else:
                    PRICE_TICK = 0.1; QTY_STEP = 0.001; MIN_NOTIONAL = 5.5
                    min_qty = aster_round_step_up(MIN_NOTIONAL / last_ref, QTY_STEP)
                    deribit_qty = max(aster_round_step(0.001, QTY_STEP), min_qty)

                    drungs = []
                    for n in range(1, NUM_ORDERS + 1):
                        sell_offset = 0.0005 + 0.0005 * (n - 1)
                        tp_offset = TP_OFF * (NUM_ORDERS + 1 - n)
                        drungs.append({
                            "n": n,
                            "sell_price": deribit_round_tick(deribit_instrument, last_ref * (1 + sell_offset)),
                            "tp_price":   deribit_round_tick(deribit_instrument, last_ref * (1 - tp_offset)),
                        })

                    print(f"\n  ── Deribit: {NUM_ORDERS} SELL entries ──")
                    for r in drungs:
                        deribit_entry(deribit_instrument, "sell", r["sell_price"], deribit_qty)
                        time.sleep(ASTER_ORDER_DELAY)
                    print(f"  💤 Waiting {DERIBIT_TP_DELAY}s...")
                    time.sleep(DERIBIT_TP_DELAY)
                    print(f"  ── Deribit: {NUM_ORDERS} TP orders ──")
                    for r in drungs:
                        deribit_tp(deribit_instrument, "sell", deribit_qty, r["tp_price"])
                        time.sleep(ASTER_ORDER_DELAY)
                    deribit_ok = True
        except Exception as e:
            print(f"  ❌ Deribit leg failed: {e}")
            send_discord_message(f"❌ **BTC SELL LADDER — Deribit leg failed**\n`{str(e)[:200]}`")
    else:
        print("  ⏭️ DERIBIT_ENABLED=False — skipping Deribit leg.")

    if aster_ok or deribit_ok:
        send_discord_message(
            f"✅ **BTC SELL LADDER EXECUTED** (MO trigger)\n"
            f"Aster:   `{'OK' if aster_ok else 'SKIPPED/FAILED'}`\n"
            f"Deribit: `{'OK' if deribit_ok else 'SKIPPED/FAILED'}` (BTC_USDC-PERPETUAL)"
        )
        return True
    send_discord_message("❌ **BTC SELL LADDER — BOTH VENUES FAILED**")
    return False


# ============================================================================
# BTC LADDER — BUY (MU trigger)
# ============================================================================
def run_btc_buy_ladder():
    sym = "BTCUSDT"
    deribit_instrument = "BTC_USDC-PERPETUAL"
    aster_ok = False
    deribit_ok = False

    print("\n" + "="*70)
    print("  BTC BUY LADDER (triggered by MU)")
    print("="*70)

    if ASTER_ENABLED:
        try:
            last = aster_get_price(sym)
            if last is None:
                print("  ⚠️ Aster: could not fetch BTC price — skipping Aster leg.")
                send_discord_message("⚠️ **BTC BUY LADDER — Aster leg skipped** (no price)")
            else:
                print(f"  BTC last (Aster): {last}")
                PRICE_TICK = 0.1; QTY_STEP = 0.001; MIN_NOTIONAL = 5.5
                min_qty = aster_round_step_up(MIN_NOTIONAL / last, QTY_STEP)
                quantity = max(aster_round_step(0.001, QTY_STEP), min_qty)

                rungs = []
                for n in range(1, NUM_ORDERS + 1):
                    buy_offset = 0.0005 + 0.0005 * (n - 1)
                    tp_offset = TP_OFF * (NUM_ORDERS + 1 - n)
                    rungs.append({
                        "n": n,
                        "buy_price": aster_round_step(last * (1 - buy_offset), PRICE_TICK),
                        "tp_price":  aster_round_step(last * (1 + tp_offset), PRICE_TICK),
                    })

                for o in rungs:
                    print(f"  Aster BUY #{o['n']} @ {o['buy_price']}")
                    aster_place_limit_order(sym, "BUY", o["buy_price"], quantity)
                    time.sleep(ASTER_ORDER_DELAY)
                    print(f"  Aster TP  #{o['n']} @ {o['tp_price']}")
                    aster_place_limit_order(sym, "SELL", o["tp_price"], quantity, reduce_only=False)
                    time.sleep(ASTER_ORDER_DELAY)
                aster_ok = True
        except Exception as e:
            print(f"  ❌ Aster leg failed: {e}")
            send_discord_message(f"❌ **BTC BUY LADDER — Aster leg failed**\n`{str(e)[:200]}`")
    else:
        print("  ⏭️ ASTER_ENABLED=False — skipping Aster leg.")

    if DERIBIT_ENABLED:
        try:
            last_ref = aster_get_price(sym)
            if last_ref is None:
                last_ref = te_fetch_latest_ticker("XXBTZUSD")
            if last_ref is None:
                print("  ⚠️ Deribit: no reference price available — skipping Deribit leg.")
                send_discord_message("⚠️ **BTC BUY LADDER — Deribit leg skipped** (no ref price)")
            else:
                deribit_fetch_meta(deribit_instrument)
                if deribit_get_token() is None:
                    print("  ⚠️ Deribit auth failed — skipping Deribit leg.")
                    send_discord_message("⚠️ **BTC BUY LADDER — Deribit auth failed**")
                else:
                    PRICE_TICK = 0.1; QTY_STEP = 0.001; MIN_NOTIONAL = 5.5
                    min_qty = aster_round_step_up(MIN_NOTIONAL / last_ref, QTY_STEP)
                    deribit_qty = max(aster_round_step(0.001, QTY_STEP), min_qty)

                    drungs = []
                    for n in range(1, NUM_ORDERS + 1):
                        buy_offset = 0.0005 + 0.0005 * (n - 1)
                        tp_offset = TP_OFF * (NUM_ORDERS + 1 - n)
                        drungs.append({
                            "n": n,
                            "buy_price": deribit_round_tick(deribit_instrument, last_ref * (1 - buy_offset)),
                            "tp_price":  deribit_round_tick(deribit_instrument, last_ref * (1 + tp_offset)),
                        })

                    print(f"\n  ── Deribit: {NUM_ORDERS} BUY entries ──")
                    for r in drungs:
                        deribit_entry(deribit_instrument, "buy", r["buy_price"], deribit_qty)
                        time.sleep(ASTER_ORDER_DELAY)
                    print(f"  💤 Waiting {DERIBIT_TP_DELAY}s...")
                    time.sleep(DERIBIT_TP_DELAY)
                    print(f"  ── Deribit: {NUM_ORDERS} TP orders ──")
                    for r in drungs:
                        deribit_tp(deribit_instrument, "buy", deribit_qty, r["tp_price"])
                        time.sleep(ASTER_ORDER_DELAY)
                    deribit_ok = True
        except Exception as e:
            print(f"  ❌ Deribit leg failed: {e}")
            send_discord_message(f"❌ **BTC BUY LADDER — Deribit leg failed**\n`{str(e)[:200]}`")
    else:
        print("  ⏭️ DERIBIT_ENABLED=False — skipping Deribit leg.")

    if aster_ok or deribit_ok:
        send_discord_message(
            f"✅ **BTC BUY LADDER EXECUTED** (MU trigger)\n"
            f"Aster:   `{'OK' if aster_ok else 'SKIPPED/FAILED'}`\n"
            f"Deribit: `{'OK' if deribit_ok else 'SKIPPED/FAILED'}` (BTC_USDC-PERPETUAL)"
        )
        return True
    send_discord_message("❌ **BTC BUY LADDER — BOTH VENUES FAILED**")
    return False


# ============================================================================
# ETH LADDER — SELL (MO trigger)
# ============================================================================
def run_eth_sell_ladder():
    sym = "ETHUSDT"
    deribit_instrument = "ETH_USDC-PERPETUAL"
    TARGET_QTY = 0.05
    aster_ok = False
    deribit_ok = False

    print("\n" + "="*70)
    print("  ETH SELL LADDER (triggered by MO)")
    print("="*70)

    if ASTER_ENABLED:
        try:
            last = aster_get_price(sym)
            if last is None:
                print("  ⚠️ Aster: could not fetch ETH price — skipping Aster leg.")
                send_discord_message("⚠️ **ETH SELL LADDER — Aster leg skipped** (no price)")
            else:
                print(f"  ETH last (Aster): {last}")
                PRICE_TICK = 0.01; QTY_STEP = 0.001
                quantity = aster_round_step(TARGET_QTY, QTY_STEP)

                rungs = []
                for n in range(1, NUM_ORDERS + 1):
                    sell_offset = 0.0005 + 0.02 * (n - 1)
                    tp_offset = TP_OFF * (NUM_ORDERS + 1 - n)
                    rungs.append({
                        "n": n,
                        "sell_price": aster_round_step(last * (1 + sell_offset), PRICE_TICK),
                        "tp_price":   aster_round_step(last * (1 - tp_offset), PRICE_TICK),
                    })

                for o in rungs:
                    print(f"  Aster SELL #{o['n']} @ {o['sell_price']}")
                    aster_place_limit_order(sym, "SELL", o["sell_price"], quantity)
                    time.sleep(ASTER_ORDER_DELAY)
                    print(f"  Aster TP   #{o['n']} @ {o['tp_price']}")
                    aster_place_limit_order(sym, "BUY", o["tp_price"], quantity, reduce_only=False)
                    time.sleep(ASTER_ORDER_DELAY)
                aster_ok = True
        except Exception as e:
            print(f"  ❌ Aster leg failed: {e}")
            send_discord_message(f"❌ **ETH SELL LADDER — Aster leg failed**\n`{str(e)[:200]}`")
    else:
        print("  ⏭️ ASTER_ENABLED=False — skipping Aster leg.")

    if DERIBIT_ENABLED:
        try:
            last_ref = aster_get_price(sym)
            if last_ref is None:
                last_ref = te_fetch_latest_ticker("XETHZUSD")
            if last_ref is None:
                print("  ⚠️ Deribit: no reference price available — skipping Deribit leg.")
                send_discord_message("⚠️ **ETH SELL LADDER — Deribit leg skipped** (no ref price)")
            else:
                deribit_fetch_meta(deribit_instrument)
                if deribit_get_token() is None:
                    print("  ⚠️ Deribit auth failed — skipping Deribit leg.")
                    send_discord_message("⚠️ **ETH SELL LADDER — Deribit auth failed**")
                else:
                    PRICE_TICK = 0.01; QTY_STEP = 0.001
                    deribit_qty = aster_round_step(TARGET_QTY, QTY_STEP)

                    drungs = []
                    for n in range(1, NUM_ORDERS + 1):
                        sell_offset = 0.0005 + 0.002 * (n - 1)
                        tp_offset = TP_OFF * (NUM_ORDERS + 1 - n)
                        drungs.append({
                            "n": n,
                            "sell_price": deribit_round_tick(deribit_instrument, last_ref * (1 + sell_offset)),
                            "tp_price":   deribit_round_tick(deribit_instrument, last_ref * (1 - tp_offset)),
                        })

                    print(f"\n  ── Deribit: {NUM_ORDERS} SELL entries ──")
                    for r in drungs:
                        deribit_entry(deribit_instrument, "sell", r["sell_price"], deribit_qty)
                        time.sleep(ASTER_ORDER_DELAY)
                    print(f"  💤 Waiting {DERIBIT_TP_DELAY}s...")
                    time.sleep(DERIBIT_TP_DELAY)
                    print(f"  ── Deribit: {NUM_ORDERS} TP orders ──")
                    for r in drungs:
                        deribit_tp(deribit_instrument, "sell", deribit_qty, r["tp_price"])
                        time.sleep(ASTER_ORDER_DELAY)
                    deribit_ok = True
        except Exception as e:
            print(f"  ❌ Deribit leg failed: {e}")
            send_discord_message(f"❌ **ETH SELL LADDER — Deribit leg failed**\n`{str(e)[:200]}`")
    else:
        print("  ⏭️ DERIBIT_ENABLED=False — skipping Deribit leg.")

    if aster_ok or deribit_ok:
        send_discord_message(
            f"✅ **ETH SELL LADDER EXECUTED** (MO trigger)\n"
            f"Aster:   `{'OK' if aster_ok else 'SKIPPED/FAILED'}`\n"
            f"Deribit: `{'OK' if deribit_ok else 'SKIPPED/FAILED'}` (ETH_USDC-PERPETUAL)"
        )
        return True
    send_discord_message("❌ **ETH SELL LADDER — BOTH VENUES FAILED**")
    return False


# ============================================================================
# ETH LADDER — BUY (MU trigger)
# ============================================================================
def run_eth_buy_ladder():
    sym = "ETHUSDT"
    deribit_instrument = "ETH_USDC-PERPETUAL"
    TARGET_QTY = 0.05
    aster_ok = False
    deribit_ok = False

    print("\n" + "="*70)
    print("  ETH BUY LADDER (triggered by MU)")
    print("="*70)

    if ASTER_ENABLED:
        try:
            last = aster_get_price(sym)
            if last is None:
                print("  ⚠️ Aster: could not fetch ETH price — skipping Aster leg.")
                send_discord_message("⚠️ **ETH BUY LADDER — Aster leg skipped** (no price)")
            else:
                print(f"  ETH last (Aster): {last}")
                PRICE_TICK = 0.01; QTY_STEP = 0.001
                quantity = aster_round_step(TARGET_QTY, QTY_STEP)

                rungs = []
                for n in range(1, NUM_ORDERS + 1):
                    buy_offset = 0.0005 + 0.0005 * (n - 1)
                    tp_offset = TP_OFF * (NUM_ORDERS + 1 - n)
                    rungs.append({
                        "n": n,
                        "buy_price": aster_round_step(last * (1 - buy_offset), PRICE_TICK),
                        "tp_price":  aster_round_step(last * (1 + tp_offset), PRICE_TICK),
                    })

                for o in rungs:
                    print(f"  Aster BUY #{o['n']} @ {o['buy_price']}")
                    aster_place_limit_order(sym, "BUY", o["buy_price"], quantity)
                    time.sleep(ASTER_ORDER_DELAY)
                    print(f"  Aster TP  #{o['n']} @ {o['tp_price']}")
                    aster_place_limit_order(sym, "SELL", o["tp_price"], quantity, reduce_only=False)
                    time.sleep(ASTER_ORDER_DELAY)
                aster_ok = True
        except Exception as e:
            print(f"  ❌ Aster leg failed: {e}")
            send_discord_message(f"❌ **ETH BUY LADDER — Aster leg failed**\n`{str(e)[:200]}`")
    else:
        print("  ⏭️ ASTER_ENABLED=False — skipping Aster leg.")

    if DERIBIT_ENABLED:
        try:
            last_ref = aster_get_price(sym)
            if last_ref is None:
                last_ref = te_fetch_latest_ticker("XETHZUSD")
            if last_ref is None:
                print("  ⚠️ Deribit: no reference price available — skipping Deribit leg.")
                send_discord_message("⚠️ **ETH BUY LADDER — Deribit leg skipped** (no ref price)")
            else:
                deribit_fetch_meta(deribit_instrument)
                if deribit_get_token() is None:
                    print("  ⚠️ Deribit auth failed — skipping Deribit leg.")
                    send_discord_message("⚠️ **ETH BUY LADDER — Deribit auth failed**")
                else:
                    PRICE_TICK = 0.01; QTY_STEP = 0.001
                    deribit_qty = aster_round_step(TARGET_QTY, QTY_STEP)

                    drungs = []
                    for n in range(1, NUM_ORDERS + 1):
                        buy_offset = 0.0005 + 0.0005 * (n - 1)
                        tp_offset = TP_OFF * (NUM_ORDERS + 1 - n)
                        drungs.append({
                            "n": n,
                            "buy_price": deribit_round_tick(deribit_instrument, last_ref * (1 - buy_offset)),
                            "tp_price":  deribit_round_tick(deribit_instrument, last_ref * (1 + tp_offset)),
                        })

                    print(f"\n  ── Deribit: {NUM_ORDERS} BUY entries ──")
                    for r in drungs:
                        deribit_entry(deribit_instrument, "buy", r["buy_price"], deribit_qty)
                        time.sleep(ASTER_ORDER_DELAY)
                    print(f"  💤 Waiting {DERIBIT_TP_DELAY}s...")
                    time.sleep(DERIBIT_TP_DELAY)
                    print(f"  ── Deribit: {NUM_ORDERS} TP orders ──")
                    for r in drungs:
                        deribit_tp(deribit_instrument, "buy", deribit_qty, r["tp_price"])
                        time.sleep(ASTER_ORDER_DELAY)
                    deribit_ok = True
        except Exception as e:
            print(f"  ❌ Deribit leg failed: {e}")
            send_discord_message(f"❌ **ETH BUY LADDER — Deribit leg failed**\n`{str(e)[:200]}`")
    else:
        print("  ⏭️ DERIBIT_ENABLED=False — skipping Deribit leg.")

    if aster_ok or deribit_ok:
        send_discord_message(
            f"✅ **ETH BUY LADDER EXECUTED** (MU trigger)\n"
            f"Aster:   `{'OK' if aster_ok else 'SKIPPED/FAILED'}`\n"
            f"Deribit: `{'OK' if deribit_ok else 'SKIPPED/FAILED'}` (ETH_USDC-PERPETUAL)"
        )
        return True
    send_discord_message("❌ **ETH BUY LADDER — BOTH VENUES FAILED**")
    return False


# ============================================================================
# MARKET_BIAS GATE
# ============================================================================
def market_bias_allows(side, coin_role):
    if MARKET_BIAS == 0:
        return coin_role == "MO"
    if MARKET_BIAS == 1:
        return coin_role == "MU"
    return True


# ============================================================================
# BRIDGE: pull latest SHORT final composite from main script
# ============================================================================
def get_main_final_composite(coin: str, system: str = "SHORT") -> Optional[float]:
    """
    Bridge to main script. Returns the latest SHORT (or POSITION) final
    composite score for the given coin by calling the existing
    _get_latest_final_for_system helper. This is the value shown in the
    TM/COMP monitor report.
    """
    try:
        profile = SHORT_TERM if system.upper() == "SHORT" else POSITION
        score, _ = _get_latest_final_for_system(coin, profile)
        return float(score)
    except Exception as e:
        print(f"  ⚠️ get_main_final_composite({coin}): {e}")
        return None


# ============================================================================
# TRADE-ENGINE MAIN LOOP
# ============================================================================
def trade_engine_main():
    global SCRIPT_EXECUTED_COUNT

    print("="*70)
    print("  UNIFIED BB + RV + LADDER TRADING MONITOR (parallel block)")
    print("="*70)
    print(f"  ASTER_ENABLED   = {ASTER_ENABLED}")
    print(f"  DERIBIT_ENABLED = {DERIBIT_ENABLED}")
    print(f"  BB timeframe: {TIMEFRAME}-MINUTE candles (Kraken)")
    print(f"  BB cooldown:  {BB_ALERT_COOLDOWN}s per coin")
    print(f"  RV timeframe: {RV_TIMEFRAME}-MINUTE (1H) candles, {RV_LOOKBACK_DAYS_TE}d lookback")
    print(f"  RV refresh:   every {RV_REFRESH_SECONDS // 3600}h")
    print(f"  MARKET_BIAS = {MARKET_BIAS} "
          f"({'BEARISH' if MARKET_BIAS==0 else 'BULLISH' if MARKET_BIAS==1 else 'NEUTRAL'})")
    print(f"  RV min votes required: {RV_MIN_VOTES}")
    print(f"  Max script executions per run: {MAX_SCRIPT_EXECUTIONS}")
    print(f"  COMPOSITE GATE: UPPER+MO <30 | LOWER+MU >70  (from main SHORT final score)")
    print("="*70)

    rv_build_cache()

    send_discord_message(
        f"🚀 **UNIFIED MONITOR STARTED** (parallel with main scanner)\n"
        f"Aster enabled: `{ASTER_ENABLED}`\n"
        f"Deribit enabled: `{DERIBIT_ENABLED}`\n"
        f"BB timeframe: `{TIMEFRAME}-minute candles`\n"
        f"BB cooldown: `{BB_ALERT_COOLDOWN}s` per coin\n"
        f"RV timeframe: `{RV_TIMEFRAME}-minute (1H) candles` ({RV_LOOKBACK_DAYS_TE}d lookback)\n"
        f"MARKET_BIAS: `{MARKET_BIAS}` "
        f"({'BEARISH' if MARKET_BIAS==0 else 'BULLISH' if MARKET_BIAS==1 else 'NEUTRAL'})\n"
        f"RV votes required: `{RV_MIN_VOTES}`\n"
        f"Max ladder executions: `{MAX_SCRIPT_EXECUTIONS}`\n"
        f"Composite gate: UPPER+MO <30 | LOWER+MU >70"
    )

    while True:
        try:
            print(f"\n{'='*70}")
            print(f"⏰ Trade-Engine Loop @ {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
            print(f"   Lockouts: BTC={COIN_SCRIPT_LOCKOUT['BTC']}  ETH={COIN_SCRIPT_LOCKOUT['ETH']}")
            print(f"   Executions so far: {SCRIPT_EXECUTED_COUNT}/{MAX_SCRIPT_EXECUTIONS}")
            print(f"{'='*70}")

            if SCRIPT_EXECUTED_COUNT >= MAX_SCRIPT_EXECUTIONS:
                print("\n🏁 Max script executions reached. Terminating trade engine.")
                send_discord_message("🏁 **Unified monitor TERMINATED** — max executions reached.")
                break
            if COIN_SCRIPT_LOCKOUT["BTC"] and COIN_SCRIPT_LOCKOUT["ETH"]:
                print("\n🏁 Both BTC and ETH have executed. Terminating trade engine.")
                send_discord_message("🏁 **Unified monitor TERMINATED** — both coins locked out.")
                break

            mo, mu, _summary = run_relative_value_scan()

            for pair_code, pair_name in BB_PAIRS.items():
                coin = pair_name.split("/")[0]

                if coin_locked(coin):
                    print(f"\n  🔒 {coin}: script lockout active — skipping BB check")
                    continue
                if SCRIPT_EXECUTED_COUNT >= MAX_SCRIPT_EXECUTIONS:
                    print(f"\n  🛑 Max executions reached — stopping.")
                    break

                candles = te_fetch_ohlc(pair_code, TIMEFRAME, LOOKBACK_DAYS_TE)
                if not candles or len(candles) < BB_PERIOD_TE + 1:
                    print(f"\n  ⚠️ {coin}: insufficient candles for BB.")
                    continue

                upper, mid, lower = te_calculate_bollinger_bands(candles[:-1])
                if upper is None:
                    print(f"\n  ⚠️ {coin}: BB calc failed.")
                    continue

                live = candles[-1]
                price = live["close"]
                candle_time = live["time"]

                touch_side = None
                if price >= upper:
                    touch_side = "upper"
                elif price <= lower:
                    touch_side = "lower"

                print(f"\n  📊 {coin} [{TIMEFRAME}m]: price={price:.2f}  upper={upper:.2f}  "
                      f"lower={lower:.2f}  touch={touch_side}")

                if touch_side is None:
                    continue

                dedup_key = (coin, candle_time, touch_side)
                if _bb_alerted.get(dedup_key):
                    print(f"      🔁 Already processed {coin} {touch_side} for candle "
                          f"{candle_time}.")
                    continue

                last_fire = _last_bb_fire.get(coin, 0)
                if time.time() - last_fire < BB_ALERT_COOLDOWN:
                    remaining = BB_ALERT_COOLDOWN - (time.time() - last_fire)
                    print(f"      ⏸ {coin}: BB cooldown active ({remaining:.0f}s left).")
                    _bb_alerted[dedup_key] = True
                    continue

                _bb_alerted[dedup_key] = True
                _last_bb_fire[coin] = time.time()

                expected_role = "MO" if touch_side == "upper" else "MU"
                is_mo_candidate = (expected_role == "MO" and mo == coin)
                is_mu_candidate = (expected_role == "MU" and mu == coin)

                if not (is_mo_candidate or is_mu_candidate):
                    print(f"      ℹ️ {coin} {touch_side} touch — but coin is NOT {expected_role}.")
                    send_discord_message(
                        f"📉 **{coin} {touch_side.upper()} BAND TOUCH — NO {expected_role}**\n"
                        f"TF: `{TIMEFRAME}m`\n"
                        f"Price: `{price:.2f}` | Upper: `{upper:.2f}` | Lower: `{lower:.2f}`\n"
                        f"Candle: `{live['time_str']}`\n"
                        f"MO now: `{mo}`  |  MU now: `{mu}`\n"
                        f"➡️ No ladder executed."
                    )
                    continue

                if not market_bias_allows(touch_side, expected_role):
                    print(f"      🚫 MARKET_BIAS={MARKET_BIAS} blocks {expected_role} trade.")
                    send_discord_message(
                        f"🚫 **{coin} {touch_side.upper()} BAND TOUCH — {expected_role} MATCH, "
                        f"BLOCKED BY MARKET_BIAS**\n"
                        f"TF: `{TIMEFRAME}m`\n"
                        f"Price: `{price:.2f}` | Upper: `{upper:.2f}` | Lower: `{lower:.2f}`\n"
                        f"Candle: `{live['time_str']}`\n"
                        f"MARKET_BIAS: `{MARKET_BIAS}`\n"
                        f"➡️ Trade NOT executed."
                    )
                    continue

                # ============================================================
                # ADDITIONAL COMPOSITE-SCORE GATE (bridge from main script)
                # ============================================================
                final_score = get_main_final_composite(coin, "SHORT")
                if final_score is None:
                    print(f"      ⚠️ No final composite score available for {coin} — skipping.")
                    send_discord_message(
                        f"⚠️ **{coin} {touch_side.upper()}+{expected_role}** — "
                        f"composite score unavailable, trade skipped."
                    )
                    continue

                gate_ok = False
                if touch_side == "upper" and expected_role == "MO":
                    # UPPER + MO → require SHORT final composite < 30
                    gate_ok = final_score < 30.0
                    gate_desc = f"UPPER+MO requires score < 30 (got {final_score:.1f})"
                elif touch_side == "lower" and expected_role == "MU":
                    # LOWER + MU → require SHORT final composite > 70
                    gate_ok = final_score > 70.0
                    gate_desc = f"LOWER+MU requires score > 70 (got {final_score:.1f})"
                else:
                    gate_ok = False
                    gate_desc = "unexpected side/role combination"

                if not gate_ok:
                    print(f"      🚫 Composite gate FAIL: {gate_desc}")
                    send_discord_message(
                        f"🚫 **{coin} {touch_side.upper()} BAND TOUCH + {expected_role}** — "
                        f"COMPOSITE GATE FAILED\n"
                        f"TF: `{TIMEFRAME}m`\n"
                        f"Price: `{price:.2f}` | Upper: `{upper:.2f}` | Lower: `{lower:.2f}`\n"
                        f"Candle: `{live['time_str']}`\n"
                        f"SHORT final composite: `{final_score:.1f}`\n"
                        f"Required: {gate_desc}\n"
                        f"➡️ Trade NOT executed."
                    )
                    continue

                print(f"      ✅ Composite gate PASSED: {gate_desc}")

                # ============================================================
                # All gates passed → execute ladder
                # ============================================================
                print(f"\n      ⚡ EXECUTING {coin} {expected_role} ladder "
                      f"(touch={touch_side}, bias={MARKET_BIAS}, score={final_score:.1f})")

                send_discord_message(
                    f"🎯 **{coin} {touch_side.upper()} BAND TOUCH + {expected_role} CONFIRMED**\n"
                    f"TF: `{TIMEFRAME}m`\n"
                    f"Price: `{price:.2f}` | Upper: `{upper:.2f}` | Lower: `{lower:.2f}`\n"
                    f"Candle: `{live['time_str']}`\n"
                    f"SHORT final composite: `{final_score:.1f}` (gate passed)\n"
                    f"Executing {coin} {expected_role} ladder..."
                )

                if coin == "BTC" and expected_role == "MO":
                    ok = run_btc_sell_ladder()
                elif coin == "BTC" and expected_role == "MU":
                    ok = run_btc_buy_ladder()
                elif coin == "ETH" and expected_role == "MO":
                    ok = run_eth_sell_ladder()
                elif coin == "ETH" and expected_role == "MU":
                    ok = run_eth_buy_ladder()
                else:
                    ok = False

                if ok:
                    lock_coin(coin)
                    SCRIPT_EXECUTED_COUNT += 1
                    print(f"      🔒 {coin} locked out. "
                          f"Executions: {SCRIPT_EXECUTED_COUNT}/{MAX_SCRIPT_EXECUTIONS}")
                else:
                    print(f"      ⚠️ {coin} ladder did NOT execute — "
                          f"coin NOT locked, will retry next loop.")
                    send_discord_message(
                        f"⚠️ **{coin} ladder failed on BOTH venues** — will retry next loop."
                    )

                if SCRIPT_EXECUTED_COUNT >= MAX_SCRIPT_EXECUTIONS:
                    break

            if (SCRIPT_EXECUTED_COUNT >= MAX_SCRIPT_EXECUTIONS or
                    (COIN_SCRIPT_LOCKOUT["BTC"] and COIN_SCRIPT_LOCKOUT["ETH"])):
                print("\n🏁 All locks exhausted. Terminating trade engine.")
                send_discord_message("🏁 **Unified monitor TERMINATED** — all coins locked out.")
                break

            print(f"\n💤 Trade-engine sleeping {CHECK_INTERVAL_SECONDS}s...")
            time.sleep(CHECK_INTERVAL_SECONDS)

        except KeyboardInterrupt:
            print("\n\n👋 Trade engine stopped by user.")
            send_discord_message("🛑 **Unified monitor STOPPED** (manual)")
            break
        except Exception as e:
            print(f"\n❌ Trade-engine loop error: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(30)


# ============================================================================
# FLASK WRAPPER (exact same structure / routes as TENNIS.py)
# ============================================================================

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
            main()
        except Exception as e:
            print(f"!! SCANNER THREAD CRASHED: {e}", file=sys.stderr)
            traceback.print_exc()

    _scanner_thread = threading.Thread(target=_wrapped_run, daemon=True)
    _scanner_thread.start()
    print(f"[{datetime.now()}] Scanner thread started")


@app.route("/")
def root():
    _start_scanner_once()
    return "Trade Map scanner is running (TM/COMP monitor + dual systems)", 200


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
