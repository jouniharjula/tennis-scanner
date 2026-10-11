#!/usr/bin/env python3
"""
Polymarket ATP + WTA Tennis Real-Time Signal Engine
Six-case alerts for LIVE matches only, with per-case cooldowns.
Observe-only — no orders are placed.
"""

import os
import time
import json
import warnings
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any
from dataclasses import dataclass, field

import pandas as pd

warnings.filterwarnings("ignore", category=DeprecationWarning)

from polymarket_tennis import (
    GammaClient,
    LiveTennisClient,
    discover_tennis_markets,
    match_market,
    build_view,
)

# ============================================================================
# NTFY CONFIGURATION
# ============================================================================

NTFY_TOPIC = os.environ.get("NTFY_TOPIC", "trademap-jl-7x9k2")
NTFY_BASE_URL = os.environ.get("NTFY_BASE_URL", "https://ntfy.sh")
NTFY_USERNAME = os.environ.get("NTFY_USERNAME", "")
NTFY_PASSWORD = os.environ.get("NTFY_PASSWORD", "")

MAX_NTFY_LENGTH = 3900
NTFY_MIN_GAP_SEC = 1.5
NTFY_INTER_CHUNK_DELAY = 2.0
NTFY_MAX_ATTEMPTS = 3
NTFY_CIRCUIT_BREAKER_THRESHOLD = 300.0

# ============================================================================
# SCANNER CONFIGURATION
# ============================================================================

LIVETENNIS_API_KEY = os.environ.get(
    "LIVETENNIS_API_KEY",
    "twjp_fb6099bc479d519a016dc727e174ba60",
)

POLL_INTERVAL_SECONDS = 130
BOLLINGER_PERIOD = 20
BOLLINGER_STD = 2.0
RESAMPLE_INTERVAL = "5min"
HISTORY_HOURS = 3

TOUR_FILTERS = ("atp", "wta")

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

# C4 low-vol FOLLOW requires the score to have moved within this many
# seconds, otherwise the "meaningful" touch is treated as informational
C4_RECENT_ACTIVITY_WINDOW_S = POLL_INTERVAL_SECONDS * 3

# ----------------------------------------------------------------------------
# PER-CASE COOLDOWNS (seconds)
# ----------------------------------------------------------------------------
DEFAULT_ALERT_COOLDOWN_SECONDS = 300
CASE_SPECIFIC_COOLDOWN = {
    "C1_BREAKPOINT_CONTEXT":  600,
    "C2_SCORE_DRIVEN":        600,
    "C3_TIMEBREAK_NOISE":     900,
    "C4_VOLATILITY_ADJUSTED": 180,
    "C5_MARKOV_FAIRVALUE":    900,
    "C6_FAVORITE_LONGSHOT":   600,
}

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
# NTFY CLIENT
# ============================================================================

class NtfyClient:
    def __init__(self):
        self._last_send_ts = 0.0

    def _wait_for_slot(self):
        now = time.monotonic()
        delta = now - self._last_send_ts
        if delta < NTFY_MIN_GAP_SEC:
            time.sleep(NTFY_MIN_GAP_SEC - delta)

    def _auth_header(self) -> Dict[str, str]:
        if NTFY_USERNAME and NTFY_PASSWORD:
            import base64
            token = base64.b64encode(
                f"{NTFY_USERNAME}:{NTFY_PASSWORD}".encode()
            ).decode()
            return {"Authorization": f"Basic {token}"}
        return {}

    def _chunk(self, text: str) -> List[str]:
        if len(text) <= MAX_NTFY_LENGTH:
            return [text]
        chunks = []
        remaining = text
        while remaining:
            cut = min(MAX_NTFY_LENGTH, len(remaining))
            if cut < len(remaining):
                nl = remaining.rfind("\n", 0, cut)
                if nl > MAX_NTFY_LENGTH * 0.5:
                    cut = nl
            chunks.append(remaining[:cut])
            remaining = remaining[cut:].lstrip("\n")
        return chunks

    def _send_once(self, body: str, title: str, priority: str,
                   tags: str) -> bool:
        url = f"{NTFY_BASE_URL.rstrip('/')}/{NTFY_TOPIC}"
        data = body.encode("utf-8")
        headers = {
            "Title": title.encode("utf-8").decode("latin-1", "ignore"),
            "Priority": priority,
            "Tags": tags,
            "Content-Type": "text/plain; charset=utf-8",
        }
        headers.update(self._auth_header())

        req = urllib.request.Request(url, data=data, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                return 200 <= resp.status < 300
        except urllib.error.HTTPError as e:
            print(f"  ! ntfy HTTP {e.code}: {e.reason}")
            return False
        except Exception as e:
            print(f"  ! ntfy error: {e}")
            return False

    def send(self, title: str, body: str, priority: str = "default",
             tags: str = "") -> bool:
        chunks = self._chunk(body)
        total = len(chunks)
        all_ok = True

        for i, chunk in enumerate(chunks, 1):
            part_title = title if total == 1 else f"{title} ({i}/{total})"
            self._wait_for_slot()
            start = time.monotonic()
            ok = False
            for attempt in range(1, NTFY_MAX_ATTEMPTS + 1):
                ok = self._send_once(chunk, part_title, priority, tags)
                if ok:
                    break
                elapsed = time.monotonic() - start
                if elapsed > NTFY_CIRCUIT_BREAKER_THRESHOLD:
                    print(f"  ! ntfy circuit breaker tripped after {elapsed:.0f}s")
                    return False
                time.sleep(1.5 * attempt)

            self._last_send_ts = time.monotonic()
            if not ok:
                all_ok = False

            if i < total:
                time.sleep(NTFY_INTER_CHUNK_DELAY)

        return all_ok


NTFY = NtfyClient()


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
# BOLLINGER BANDS
# ============================================================================

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


# ============================================================================
# MARKOV (rough)
# ============================================================================

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
# SIX CASE DETECTORS
# ============================================================================

def case_1_breakpoint(s: MarketSnapshot) -> Optional[Signal]:
    if not s.break_point_flag or s.touch is None:
        return None
    if s.touch == "upper":
        return Signal(
            case_id="C1_BREAKPOINT_CONTEXT",
            direction="fade",
            title="BREAK POINT + UPPER TOUCH",
            reason=(f"Server ({s.server}) odds spiked to upper band while "
                    "facing break point. Market overrating the save."),
            trade_action=(f"FADE the server ({s.server}). "
                          f"Consider buying {s.receiver} at current price."),
            confidence="HIGH",
            details={"server": s.server, "receiver": s.receiver},
        )
    return Signal(
        case_id="C1_BREAKPOINT_CONTEXT",
        direction="fade",
        title="BREAK POINT + LOWER TOUCH",
        reason=(f"Receiver ({s.receiver}) odds dropped to lower band while "
                "holding break point. Market overrating the miss."),
        trade_action=f"FADE the market. Consider buying {s.receiver}.",
        confidence="HIGH",
        details={"server": s.server, "receiver": s.receiver},
    )


def case_2_score_driven(s: MarketSnapshot) -> Optional[Signal]:
    if s.touch is None:
        return None
    if s.last_score_change_ts is None:
        return Signal(
            case_id="C2_SCORE_DRIVEN", direction="info",
            title=f"{s.touch.upper()} TOUCH — no score context",
            reason="Band touch but no recorded score change yet.",
            trade_action="WAIT for next scan to establish score context.",
            confidence="LOW",
        )
    since = (datetime.now() - s.last_score_change_ts).total_seconds()
    if since <= POLL_INTERVAL_SECONDS + 10:
        return Signal(
            case_id="C2_SCORE_DRIVEN", direction="follow",
            title=f"{s.touch.upper()} TOUCH — REAL SCORE MOVE",
            reason=f"Score changed {since:.0f}s ago. Event-driven repricing.",
            trade_action=(f"FOLLOW the move. Buy the player whose odds "
                          f"touched the {s.touch} band."),
            confidence="HIGH",
            details={"seconds_since_score_change": since},
        )
    return Signal(
        case_id="C2_SCORE_DRIVEN", direction="fade",
        title=f"{s.touch.upper()} TOUCH — NO RECENT SCORE CHANGE",
        reason=f"Score unchanged for {since:.0f}s but odds touched a band.",
        trade_action=f"FADE. Buy the OPPOSITE side of the {s.touch} touch.",
        confidence="MEDIUM",
        details={"seconds_since_score_change": since},
    )


def case_3_timebreak(s: MarketSnapshot) -> Optional[Signal]:
    if s.touch is None or s.last_point_time is None:
        return None
    gap = (datetime.now() - s.last_point_time).total_seconds()
    if gap >= CHANGEOVER_THRESHOLD_S:
        return Signal(
            case_id="C3_TIMEBREAK_NOISE", direction="fade",
            title="BAND TOUCH DURING CHANGEOVER",
            reason=f"No point for {gap:.0f}s. Band touches here are noise.",
            trade_action="DO NOT CHASE. Wait for play to resume.",
            confidence="HIGH",
            details={"gap_seconds": gap},
        )
    return None


def case_4_volatility(s: MarketSnapshot) -> Optional[Signal]:
    if s.touch is None or len(s.recent_prices) < BOLLINGER_PERIOD:
        return None
    tail = pd.Series(s.recent_prices[-BOLLINGER_PERIOD:])
    q1, q3 = float(tail.quantile(0.25)), float(tail.quantile(0.75))
    iqr = q3 - q1

    if iqr < MIN_IQR:
        return None

    # High-vol regime: informational only, unchanged
    if iqr >= VOL_HIGH_IQR:
        return Signal(
            case_id="C4_VOLATILITY_ADJUSTED", direction="info",
            title=f"{s.touch.upper()} TOUCH IN HIGH-VOL REGIME",
            reason=f"IQR={iqr:.4f} (high). Bands too narrow.",
            trade_action="IGNORE unless confirmed by C2 or C1.",
            confidence="LOW",
            details={"iqr": iqr, "regime": "high"},
        )

    # Low-vol regime: require recent score activity for a FOLLOW signal
    if iqr <= VOL_LOW_IQR:
        recently_active = (
            s.last_score_change_ts is not None
            and (datetime.now() - s.last_score_change_ts).total_seconds()
                <= C4_RECENT_ACTIVITY_WINDOW_S
        )
        if not recently_active:
            return Signal(
                case_id="C4_VOLATILITY_ADJUSTED", direction="info",
                title=f"{s.touch.upper()} TOUCH IN LOW-VOL (STALE)",
                reason=(f"IQR={iqr:.4f} low but no recent score activity. "
                        "Quiet market — touch is likely noise."),
                trade_action="IGNORE — no score context to justify FOLLOW.",
                confidence="LOW",
                details={"iqr": iqr, "regime": "low-stale"},
            )
        return Signal(
            case_id="C4_VOLATILITY_ADJUSTED", direction="follow",
            title=f"{s.touch.upper()} TOUCH IN LOW-VOL REGIME",
            reason=f"IQR={iqr:.4f} (low). Band touch is meaningful.",
            trade_action=f"FOLLOW. Buy the {s.touch} band touch side.",
            confidence="MEDIUM",
            details={"iqr": iqr, "regime": "low-active"},
        )

    return None


def case_5_markov(s: MarketSnapshot) -> Optional[Signal]:
    if s.markov_fair_prob is None or s.touch is None:
        return None
    divergence = s.current_price - s.markov_fair_prob
    if s.touch == "upper" and divergence > MARKOV_DIVERGENCE:
        return Signal(
            case_id="C5_MARKOV_FAIRVALUE", direction="fade",
            title="UPPER TOUCH ABOVE MARKOV FAIR VALUE",
            reason=(f"Market {s.current_price:.3f} vs Markov "
                    f"{s.markov_fair_prob:.3f} (+{divergence:.3f})."),
            trade_action="FADE. Buy the opposite outcome.",
            confidence="HIGH",
            details={"divergence": divergence},
        )
    if s.touch == "lower" and -divergence > MARKOV_DIVERGENCE:
        return Signal(
            case_id="C5_MARKOV_FAIRVALUE", direction="fade",
            title="LOWER TOUCH BELOW MARKOV FAIR VALUE",
            reason=(f"Market {s.current_price:.3f} vs Markov "
                    f"{s.markov_fair_prob:.3f} ({divergence:.3f})."),
            trade_action="FADE. Buy the underpriced side.",
            confidence="HIGH",
            details={"divergence": divergence},
        )
    return None


def case_6_favorite_longshot(s: MarketSnapshot) -> Optional[Signal]:
    if s.touch is None or s.role == "unknown":
        return None
    if s.role == "favorite" and s.touch == "lower":
        return Signal(
            case_id="C6_FAVORITE_LONGSHOT", direction="fade",
            title="FAVORITE HIT LOWER BAND",
            reason="Market over-fearing underdog momentum.",
            trade_action="FADE. Buy the FAVORITE — likely underpriced.",
            confidence="MEDIUM",
            details={"role": s.role},
        )
    if s.role == "underdog" and s.touch == "upper":
        return Signal(
            case_id="C6_FAVORITE_LONGSHOT", direction="fade",
            title="UNDERDOG HIT UPPER BAND",
            reason="Market over-valuing a short underdog run.",
            trade_action="FADE. Buy the FAVORITE — underdog overpriced.",
            confidence="MEDIUM",
            details={"role": s.role},
        )
    return None


CASE_DETECTORS = {
    "C1_BREAKPOINT_CONTEXT": case_1_breakpoint,
    "C2_SCORE_DRIVEN": case_2_score_driven,
    "C3_TIMEBREAK_NOISE": case_3_timebreak,
    "C4_VOLATILITY_ADJUSTED": case_4_volatility,
    "C5_MARKOV_FAIRVALUE": case_5_markov,
    "C6_FAVORITE_LONGSHOT": case_6_favorite_longshot,
}


def run_all_cases(s: MarketSnapshot) -> List[Signal]:
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
# DISCOVERY — LIVE MATCHES ONLY
# ============================================================================

def discover_tennis_markets_all() -> List:
    out = []
    with GammaClient() as gamma:
        all_markets = discover_tennis_markets(
            gamma, market_types={"moneyline"}, matches_only=True,
        )
        for m in all_markets:
            text = f"{m.question} {m.slug}".lower()
            if any(t in text for t in TOUR_FILTERS):
                out.append(m)
    return out


def match_markets_to_live(markets, live_client):
    """
    Match Polymarket markets to LIVE matches only. Fixtures (upcoming
    matches) are excluded so pre-match markets don't pollute signals.
    """
    live = live_client.live_matches()
    print(f"  [match] {len(live)} live matches from Live Tennis API")

    paired = []
    for m in markets:
        try:
            d = match_market(m, live)
            if d and d.match:
                paired.append({"market": m, "match": d.match,
                               "confidence": d.confidence})
                print(f"  [match] + {m.slug}")
            else:
                print(f"  [match] - {m.slug} (not live)")
        except Exception as e:
            print(f"  ! match error for {m.slug}: {e}")
    return paired


# ============================================================================
# NTFY FORMATTER
# ============================================================================

DIRECTION_ICON = {"fade": "🟡 FADE", "follow": "🟢 FOLLOW", "info": "ℹ️ INFO"}
DIRECTION_TAG = {"fade": "warning", "follow": "white_check_mark", "info": "information_source"}
DIRECTION_PRIORITY = {"fade": "high", "follow": "high", "info": "default"}


def format_ntfy_body(sig: Signal, s: MarketSnapshot) -> str:
    lines = [
        f"Tour:    {s.tour}",
        f"Market:  {s.question}",
        f"Score:   {s.score_line}",
        f"Server:  {s.server or 'N/A'}   Role: {s.role}",
        "",
        f"Price:   {s.current_price:.4f}",
        f"Touch:   {s.touch}",
    ]
    if s.bands:
        lines += [
            f"Bands:   up={s.bands['upper']:.4f}  "
            f"mid={s.bands['middle']:.4f}  lo={s.bands['lower']:.4f}",
        ]
    lines += [
        "",
        f"Reason:  {sig.reason}",
        f"Action:  {sig.trade_action}",
        f"Confidence: {sig.confidence}",
    ]
    if sig.details:
        lines += ["", f"Details: {sig.details}"]
    lines += ["", f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"]
    return "\n".join(lines)


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

    def initialize(self):
        key = (LIVETENNIS_API_KEY or "").strip()
        if not key:
            raise ValueError("LIVETENNIS_API_KEY not set.")
        self.live_client = LiveTennisClient(api_key=key)
        print(f"[{datetime.now()}] Signal Engine initialized (LIVE-only). "
              f"Cases: {sorted(ENABLED_CASES)}")
        print(f"[{datetime.now()}] ntfy topic: {NTFY_TOPIC} @ {NTFY_BASE_URL}")
        cd_summary = ", ".join(
            f"{k.split('_')[0]}={v}s" for k, v in CASE_SPECIFIC_COOLDOWN.items()
        )
        print(f"[{datetime.now()}] per-case cooldowns: {cd_summary}")

    def cooldown_key(self, slug: str, case_id: str) -> str:
        return f"{slug}::{case_id}"

    def cooldown_seconds_for(self, case_id: str) -> int:
        return CASE_SPECIFIC_COOLDOWN.get(case_id, DEFAULT_ALERT_COOLDOWN_SECONDS)

    def cooldown_ok(self, key: str, case_id: str) -> bool:
        st = self.alert_state.get(key)
        if st is None or st.last_alert_time is None:
            return True
        elapsed = (datetime.now() - st.last_alert_time).total_seconds()
        return elapsed >= self.cooldown_seconds_for(case_id)

    def record_alert(self, key: str, kind: str):
        self.alert_state[key] = MarketAlert(
            market_slug=key, last_alert_time=datetime.now(),
            last_alert_type=kind,
        )

    def fire(self, slug: str, sig: Signal, s: MarketSnapshot):
        key = self.cooldown_key(slug, sig.case_id)
        if not self.cooldown_ok(key, sig.case_id):
            return

        bar = "=" * 68
        icon = DIRECTION_ICON[sig.direction]
        print(f"\n{bar}")
        print(f"  {sig.case_id} [{sig.confidence}]")
        print(f"  {icon} — {sig.title}")
        print(bar)
        print(f"  Market:    {s.question}")
        print(f"  Tour:      {s.tour}")
        print(f"  Score:     {s.score_line}")
        print(f"  Server:    {s.server or 'N/A'}   Role: {s.role}")
        print(f"  Price:     {s.current_price:.4f}   Touch: {s.touch}")
        if s.bands:
            print(f"  Bands:     up={s.bands['upper']:.4f}  "
                  f"mid={s.bands['middle']:.4f}  lo={s.bands['lower']:.4f}")
        print(f"  Reason:    {sig.reason}")
        print(f"  ACTION:    {sig.trade_action}")
        if sig.details:
            print(f"  Details:   {sig.details}")
        print(f"  Cooldown:  {self.cooldown_seconds_for(sig.case_id)}s")
        print(f"{bar}\n")

        title = f"{icon} {sig.case_id} — {s.tour}"
        body = format_ntfy_body(sig, s)
        tags = DIRECTION_TAG.get(sig.direction, "")
        priority = DIRECTION_PRIORITY.get(sig.direction, "default")
        ok = NTFY.send(title=title, body=body, priority=priority, tags=tags)
        
        
        if not ok:
            print(f"  ! ntfy delivery failed for {sig.case_id}")

        self.record_alert(key, sig.case_id)

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

        touch = detect_band_touch(float(closes.iloc[-1]), bands)

        if bands is not None and not pd.isna(bands["upper"]) and not pd.isna(bands["lower"]):
            px = float(closes.iloc[-1])
            up, lo, mid = bands["upper"], bands["lower"], bands["middle"]
            dist_up_pct = (up - px) / px * 100.0
            dist_lo_pct = (px - lo) / px * 100.0
            dist_mid_pct = (px - mid) / mid * 100.0
            band_width_pct = (up - lo) / mid * 100.0 if mid else 0.0
            touch_tag = touch if touch else "-"
            print(
                f"  ~ {slug[:45]:<45}  "
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
        try:
            view = build_view(market, match)
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
                else:
                    server = None

                if server and len(outcomes) == 2:
                    receiver = outcomes[1] if outcomes[0] == server else outcomes[0]
        except Exception as e:
            print(f"  ! build_view error for {slug}: {e}")

        prev = self.last_score.get(slug)
        if prev != score_line:
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
            tour="ATP" if "atp" in market.question.lower()
            else ("WTA" if "wta" in market.question.lower() else "UNKNOWN"),
            outcomes=list(getattr(market, "outcomes", []) or []),
            token_id=token_id,
            current_price=float(closes.iloc[-1]),
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

    def scan_once(self):
        self.scan_count += 1
        print(f"\n[{datetime.now()}] scan start (cycle {self.scan_count})")
        markets = discover_tennis_markets_all()
        print(f"  discovered: {len(markets)}")
        if not markets:
            return
        paired = match_markets_to_live(markets, self.live_client)
        print(f"  matched live: {len(paired)}")

        for item in paired:
            s = self.build_snapshot(item)
            if s is None:
                continue

            # Skip alert firing on cycle 1 — state memory isn't warmed up yet
            if self.scan_count < 2:
                continue

            signals = run_all_cases(s)
            for sig in signals:
                self.fire(s.market_slug, sig, s)

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
    scanner = TennisBollingerScanner()
    _scanner_thread = threading.Thread(target=scanner.run, daemon=True)
    _scanner_thread.start()
    print(f"[{datetime.now()}] Scanner thread started")


@app.route("/")
def root():
    _start_scanner_once()
    return "Tennis scanner is running", 200


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
