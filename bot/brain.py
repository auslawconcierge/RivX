# RIVX_VERSION: v2.1-render-fixed-2026-04-26
"""
RivX brain.py — Claude wrapper for trading decisions.

═══════════════════════════════════════════════════════════════════════════
NON-NEGOTIABLE RULE: Claude is never the final authority. If Claude:
  - fails (API exception, network error)
  - times out
  - returns invalid JSON
  - returns empty decisions
  - returns confidence below MIN_CONFIDENCE
  - returns "buy" for symbols already held
  - returns "buy" for buckets that are full
  - returns "buy" that would breach the ops floor

→ result is NO TRADE. There is no "mechanical fallback" path. Empty is fine.
═══════════════════════════════════════════════════════════════════════════

Job: given a list of candidates from scanner.py and the current portfolio
state, ask Claude which (if any) to buy. Return structured decisions.

This module is intentionally narrow:

  - It does NOT fetch market data (that's prices.py + scanner.py).
  - It does NOT execute trades (that's bot.py).
  - It does NOT decide rules (that's strategy.py).
  - It DOES translate "scanner says these qualify" + "we have these positions"
    into "Claude says: buy these, skip those, here's why."

Why Claude in the loop at all (when strategy.py already qualifies things)?
Because qualification is binary — it says "this matches a pullback pattern."
Claude adds judgment — "of these 8 qualified pullbacks, which 2 actually look
clean, and which look like falling knives or news-driven panic?" The
strategy rules are the floor; Claude is the editor.

Yesterday's lessons baked in:

  - We never tell Claude "fill all slots" or use a "mechanical fallback."
    If Claude says no to all candidates, we buy nothing. Empty is fine.

  - The prompt explicitly forbids buying coins that have already pumped.
    Even if a candidate slipped through (it shouldn't, scanner blocks this),
    Claude is told: pumps and breakouts beyond their initial move are skips.

  - Token budget is hard-capped per call. No 6000-token responses.

  - Output is structured JSON only — no free-form "I'd suggest..." text
    that yesterday's parser tried (and failed) to regex-extract trades from.
"""

import os
import json
import logging
from dataclasses import dataclass, asdict, field
from typing import Optional

from . import strategy

log = logging.getLogger(__name__)


# ── Model + budget ────────────────────────────────────────────────────────

MODEL_DECIDE = "claude-opus-4-7"   # the model that picks trades
MAX_TOKENS_DECIDE = 1500           # generous enough for 10 candidates × short reasoning
DAILY_USD_CAP = 2.0                # hard ceiling — same as before
SYSTEM_PROMPT_VERSION = 4          # bump this when prompt changes for tracking
MIN_CONFIDENCE = 0.6               # below this, treat as "skip" even if Claude said buy
MAX_CANDIDATES_TO_CLAUDE = 8       # token-budget cap; rank scanner output, send top 8
MAX_BUYS_PER_CLUSTER = 2           # don't buy 3 highly-correlated assets in one round


# ── Correlation clusters ─────────────────────────────────────────────────
# Coarse but useful grouping: assets in the same cluster tend to move together,
# so buying multiple from one cluster isn't diversification — it's concentration.
# Anything not listed defaults to its own single-asset cluster.
ASSET_CLUSTERS = {
    # Layer-1 platforms — usually move together on "L1 narrative" days
    "ETH": "l1", "SOL": "l1", "AVAX": "l1", "NEAR": "l1", "APT": "l1",
    "SUI": "l1", "SEI": "l1", "ICP": "l1", "ATOM": "l1", "DOT": "l1",
    "TON": "l1", "TRX": "l1", "ALGO": "l1", "HBAR": "l1",

    # L2/scaling
    "ARB": "l2", "OP": "l2", "MATIC": "l2", "IMX": "l2", "STX": "l2",

    # Memecoins — extreme correlation in retail-driven moves
    "DOGE": "meme", "SHIB": "meme", "PEPE": "meme", "WIF": "meme",
    "BONK": "meme", "FLOKI": "meme",

    # AI tokens
    "FET": "ai", "TAO": "ai", "WLD": "ai", "RNDR": "ai", "AGIX": "ai",

    # DeFi blue chips
    "UNI": "defi", "AAVE": "defi", "CRV": "defi", "COMP": "defi",
    "MKR": "defi", "SUSHI": "defi", "LDO": "defi",

    # Tech/semi stocks (when buying multiple at once = sector concentration)
    "NVDA": "semi", "AMD": "semi", "AVGO": "semi", "TSM": "semi",
    "AAPL": "megacap", "MSFT": "megacap", "GOOGL": "megacap",
    "AMZN": "megacap", "META": "megacap", "TSLA": "megacap", "NFLX": "megacap",

    # ETFs — own cluster each (hard to "diversify" by buying multiple broad ETFs)
    "SPY": "etf", "QQQ": "etf", "IWM": "etf",
    # BTC: own cluster — uncorrelated enough with everything else to ignore here
}


def _cluster_for(symbol: str) -> str:
    """Returns the cluster name for a symbol, or 'solo:SYM' if not mapped."""
    sym = symbol.upper()
    return ASSET_CLUSTERS.get(sym, f"solo:{sym}")


# ── Data classes ──────────────────────────────────────────────────────────

@dataclass
class TradeDecision:
    """One decision from Claude on one candidate."""
    symbol: str
    bucket: str
    action: str              # "buy" | "skip"
    confidence: float        # 0..1, Claude's self-rated confidence
    reason: str              # Claude's short reasoning


@dataclass
class BrainResult:
    """Full output of decide_buys()."""
    decisions: list = field(default_factory=list)
    summary: str = ""
    used_input_tokens: int = 0
    used_output_tokens: int = 0
    estimated_cost_usd: float = 0.0
    error: str = ""


# ── Prompt construction ─────────────────────────────────────────────────

def _format_candidate(c: dict) -> str:
    """Render one candidate as a short bullet for the prompt."""
    sig = c.get("signal", {})
    bits = []
    if "rank" in sig:
        bits.append(f"rank {sig['rank']}")
    if "pullback_pct" in sig:
        bits.append(f"pullback {sig['pullback_pct']*100:+.1f}%")
    if "above_50d_ma" in sig:
        bits.append("above 50dMA" if sig["above_50d_ma"] else "BELOW 50dMA")
    if "broke_7d_high_today" in sig and sig["broke_7d_high_today"]:
        bits.append("broke 7d high today")
    if "volume_ratio" in sig:
        bits.append(f"vol {sig['volume_ratio']:.1f}x avg")
    if "close" in sig:
        bits.append(f"close ${sig['close']:.4f}")

    return f"- {c['symbol']} ({c['bucket']}): {', '.join(bits)} | qualifies because: {c.get('reasoning','')}"


def _prescore_candidate(c: dict) -> float:
    """
    Deterministic ranking score. Used to cap candidates sent to Claude when
    there are more than MAX_CANDIDATES_TO_CLAUDE qualified setups. Higher = better.
    """
    sig = c.get("signal", {})
    bucket = c.get("bucket", "")
    if bucket == strategy.Bucket.SWING_CRYPTO:
        return strategy.prescore_swing_crypto(
            market_cap_rank=sig.get("rank", 9999),
            pullback_pct=sig.get("pullback_pct", 0.0),
            above_50d_ma=sig.get("above_50d_ma", False),
        )
    if bucket == strategy.Bucket.MOMENTUM_CRYPTO:
        return strategy.prescore_momentum_crypto(
            market_cap_rank=sig.get("rank", 9999),
            broke_7d_high_today=sig.get("broke_7d_high_today", False),
            volume_ratio=sig.get("volume_ratio", 0.0),
        )
    if bucket == strategy.Bucket.SWING_STOCK:
        return strategy.prescore_swing_stock(
            pullback_pct=sig.get("pullback_pct", 0.0),
            above_50d_ma=sig.get("above_50d_ma", False),
        )
    return 0.0


def _format_portfolio(positions: dict, slot_state: dict, cash_aud: float) -> str:
    """Render current portfolio state for Claude's context."""
    lines = []
    lines.append(f"Cash available: ${cash_aud:.2f} AUD (ops floor: ${strategy.OPS_FLOOR_AUD:.0f})")
    lines.append(f"Slots used:")
    lines.append(f"  swing_crypto    {slot_state.get('swing_crypto', 0)}/{strategy.SWING_CRYPTO_SLOTS}    "
                 f"(${strategy.SWING_CRYPTO_SIZE:.0f} per buy)")
    lines.append(f"  momentum_crypto {slot_state.get('momentum_crypto', 0)}/{strategy.MOMENTUM_CRYPTO_SLOTS}    "
                 f"(${strategy.MOMENTUM_CRYPTO_SIZE:.0f} per buy)")
    lines.append(f"  swing_stock     {slot_state.get('swing_stock', 0)}/{strategy.SWING_STOCKS_SLOTS}    "
                 f"(${strategy.SWING_STOCKS_SIZE:.0f} per buy)")

    if positions:
        lines.append(f"\nCurrently holding ({len(positions)}):")
        for sym, p in positions.items():
            pnl = float(p.get("pnl_pct") or 0) * 100
            mkt = (p.get("market") or "?")
            lines.append(f"  {sym} ({mkt}) {pnl:+.2f}%")
    else:
        lines.append("\nNo current positions.")
    return "\n".join(lines)


SYSTEM_PROMPT = """You are RivX's trade decider. You see candidates that ALREADY passed the strategy's mechanical rules (correct rank, correct pullback or breakout, correct volume). Your job is judgment: of these qualified candidates, which actually look clean enough to buy now?

CORE PRINCIPLES (non-negotiable):

1. SKIP IS THE DEFAULT. If you have any meaningful doubt, skip. Empty slots are fine. Cash is a position.

2. NEVER buy something that has already pumped 15%+ in the last 24h. The scanner shouldn't show you these, but if one slips through (e.g. a 24h surge after qualifying earlier), skip it. You're catching pullbacks and fresh breakouts, not chasing tops.

3. NEVER buy something the user already holds. Re-entries get explicit approval, not bot autonomy.

4. RESPECT BUCKET CAPS. If swing_crypto has 5/5 slots full, don't buy more for that bucket no matter how good the setup looks.

5. AT MOST 3 BUYS PER CALL. Even if 8 candidates look great, pick the 3 cleanest. Diversification isn't piling in.

OUTPUT FORMAT (strict JSON, no preamble, no markdown):

{
  "decisions": [
    {"symbol": "BTC", "bucket": "swing_crypto", "action": "buy", "confidence": 0.7, "reason": "clean -8% pullback in established uptrend, top-1 cap, no red flags"},
    {"symbol": "DOGE", "bucket": "momentum_crypto", "action": "skip", "confidence": 0.8, "reason": "breakout but price already +12% intraday, late entry"}
  ],
  "summary": "1 buy, 5 skips. Most candidates are post-pump, not entries."
}

Every candidate I show you must appear in `decisions` with action either "buy" or "skip". Nothing else. confidence is your self-rated 0-1 of the call."""


def _build_user_message(
    candidates: list,
    positions: dict,
    slot_state: dict,
    cash_aud: float,
) -> str:
    """The user-facing message Claude reads to make decisions."""
    if not candidates:
        return "No candidates today. Output {\"decisions\":[], \"summary\":\"no candidates\"}."

    portfolio_block = _format_portfolio(positions, slot_state, cash_aud)
    candidate_lines = "\n".join(_format_candidate(c) for c in candidates)

    return f"""PORTFOLIO STATE
{portfolio_block}

CANDIDATES ({len(candidates)} qualified)
{candidate_lines}

Decide: which to buy, which to skip, and why. Remember: skip is default; never buy something the user already holds; respect bucket caps; at most 3 buys."""


# ── Response parsing ────────────────────────────────────────────────────

def _parse_response(text: str) -> tuple[list, str, str]:
    """
    Parse Claude's JSON output.
    Returns (decisions_list, summary, error_str). error_str is "" on success.
    """
    # Strip common prefixes/suffixes Claude sometimes adds despite system prompt
    text = text.strip()
    if text.startswith("```"):
        # Remove ``` and ```json wrappers
        lines = text.split("\n")
        if lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        text = "\n".join(lines)

    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        return [], "", f"JSON parse error: {e}. Raw: {text[:200]}"

    if not isinstance(data, dict):
        return [], "", f"Response not a dict: {type(data).__name__}"

    decisions = data.get("decisions", [])
    if not isinstance(decisions, list):
        return [], "", "decisions field is not a list"

    parsed = []
    for d in decisions:
        if not isinstance(d, dict):
            continue
        sym = d.get("symbol", "").upper().strip()
        bucket = d.get("bucket", "").strip()
        action = d.get("action", "").lower().strip()
        if action not in ("buy", "skip"):
            continue
        if not sym or not bucket:
            continue
        try:
            confidence = float(d.get("confidence", 0))
            confidence = max(0.0, min(1.0, confidence))
        except (TypeError, ValueError):
            confidence = 0.0
        parsed.append(TradeDecision(
            symbol=sym,
            bucket=bucket,
            action=action,
            confidence=confidence,
            reason=str(d.get("reason", ""))[:300],
        ))

    summary = str(data.get("summary", ""))[:500]
    return parsed, summary, ""


# ── Cost estimate (Claude Opus 4.7 pricing) ──────────────────────────────

def _estimate_cost_usd(input_tokens: int, output_tokens: int) -> float:
    """
    Opus 4.7 pricing per Anthropic's docs at the time of writing:
      $15 per 1M input tokens
      $75 per 1M output tokens
    Conservative; if pricing changes the number is just a soft estimate.
    """
    return (input_tokens * 15.0 + output_tokens * 75.0) / 1_000_000


# ── Main entry point ────────────────────────────────────────────────────

def decide_buys(
    *,
    candidates: list,
    positions: dict,
    slot_state: dict,
    cash_aud: float,
    anthropic_client=None,
    daily_spent_usd: float = 0.0,
) -> BrainResult:
    """
    Ask Claude which candidates to actually buy.

    Args:
        candidates: list of dicts from scanner.scan_*()
        positions: {symbol: {pnl_pct, market, ...}} — currently held
        slot_state: {bucket: count_used} — for cap awareness
        cash_aud: available cash for buying
        anthropic_client: an Anthropic client object with .messages.create().
                          Pass None to skip the API call (used by tests)
        daily_spent_usd: how much already spent today; aborts if over cap

    Returns: BrainResult with decisions list + metadata
    """
    if daily_spent_usd >= DAILY_USD_CAP:
        log.warning(f"brain: daily cap ${DAILY_USD_CAP} reached, skipping decision")
        return BrainResult(
            decisions=[],
            summary="daily Claude budget exhausted",
            error=f"daily_spent ${daily_spent_usd:.2f} >= cap ${DAILY_USD_CAP:.2f}",
        )

    # Filter out candidates for symbols we already hold — never call Claude on
    # those. Saves tokens and removes a class of mistakes.
    held = {s.upper() for s in (positions or {}).keys()}
    fresh_candidates = [c for c in candidates if c.get("symbol", "").upper() not in held]
    if len(fresh_candidates) < len(candidates):
        skipped = [c["symbol"] for c in candidates if c["symbol"].upper() in held]
        log.info(f"brain: skipped {len(skipped)} already-held: {skipped}")

    if not fresh_candidates:
        return BrainResult(decisions=[], summary="no fresh candidates after held-position filter")

    # Pre-filter against slot caps. If swing_crypto is full, drop swing_crypto
    # candidates from what we even ask Claude. Saves the model from having to
    # juggle that.
    available = []
    for c in fresh_candidates:
        bucket = c.get("bucket", "")
        slots_left = strategy.slots_available(bucket, slot_state.get(bucket, 0))
        if slots_left > 0:
            available.append(c)
    if len(available) < len(fresh_candidates):
        log.info(f"brain: dropped {len(fresh_candidates) - len(available)} candidates whose buckets are full")

    if not available:
        return BrainResult(decisions=[], summary="all relevant buckets full")

    # Cap candidates by deterministic pre-score (token control + dilution control).
    # Send Claude only the top N strongest setups, not 50 borderline ones.
    if len(available) > MAX_CANDIDATES_TO_CLAUDE:
        scored = [(_prescore_candidate(c), c) for c in available]
        scored.sort(key=lambda x: x[0], reverse=True)
        available = [c for _, c in scored[:MAX_CANDIDATES_TO_CLAUDE]]
        log.info(f"brain: capped to top {MAX_CANDIDATES_TO_CLAUDE} candidates by pre-score")

    if anthropic_client is None:
        log.warning("brain: no anthropic_client passed, returning empty (test mode)")
        return BrainResult(decisions=[], summary="test mode — no client")

    user_msg = _build_user_message(available, positions, slot_state, cash_aud)

    try:
        resp = anthropic_client.messages.create(
            model=MODEL_DECIDE,
            max_tokens=MAX_TOKENS_DECIDE,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_msg}],
        )
    except Exception as e:
        log.error(f"brain: Claude API failed: {e}")
        return BrainResult(decisions=[], summary="", error=f"API error: {e}")

    # Extract text from response
    try:
        text = resp.content[0].text
    except (AttributeError, IndexError) as e:
        return BrainResult(decisions=[], summary="", error=f"unexpected response shape: {e}")

    decisions, summary, parse_err = _parse_response(text)
    if parse_err:
        log.error(f"brain: {parse_err}")
        return BrainResult(decisions=[], summary="", error=parse_err)

    # Get token usage from response (Anthropic SDK uses .usage attribute)
    in_tok = getattr(getattr(resp, "usage", None), "input_tokens", 0) or 0
    out_tok = getattr(getattr(resp, "usage", None), "output_tokens", 0) or 0
    cost = _estimate_cost_usd(in_tok, out_tok)

    return BrainResult(
        decisions=decisions,
        summary=summary,
        used_input_tokens=in_tok,
        used_output_tokens=out_tok,
        estimated_cost_usd=cost,
    )


# ── Validation against strategy rules ──────────────────────────────────

def filter_decisions_by_safety(
    decisions: list,
    cash_aud: float,
    slot_state: dict,
) -> tuple[list, list]:
    """
    Final gate: even if Claude said "buy", refuse if:
      - bucket cap reached (defensive — we already filtered, but Claude might
        have said buy on multiple from same bucket)
      - ops floor would be breached
      - duplicate symbols (Claude returned same symbol twice)

    Returns (allowed_decisions, rejected_with_reason).
    """
    allowed = []
    rejected = []
    seen_symbols = set()
    cluster_used_this_round = {}    # cluster_name -> count bought this round
    bucket_used_this_round = dict(slot_state)
    cash_remaining = cash_aud

    for d in decisions:
        if d.action != "buy":
            continue

        # Confidence floor: Claude often hedges at 0.5 ("could go either way").
        # We treat anything below MIN_CONFIDENCE as a skip, even if Claude said buy.
        if d.confidence < MIN_CONFIDENCE:
            rejected.append((d, f"confidence {d.confidence:.2f} below floor {MIN_CONFIDENCE}"))
            continue

        if d.symbol in seen_symbols:
            rejected.append((d, "duplicate symbol in same decision"))
            continue
        seen_symbols.add(d.symbol)

        # Correlation cluster cap: don't buy 3 things that move together
        cluster = _cluster_for(d.symbol)
        if cluster_used_this_round.get(cluster, 0) >= MAX_BUYS_PER_CLUSTER:
            rejected.append((d, f"cluster '{cluster}' already has {MAX_BUYS_PER_CLUSTER} buys this round"))
            continue

        slots_left = strategy.slots_available(d.bucket, bucket_used_this_round.get(d.bucket, 0))
        if slots_left <= 0:
            rejected.append((d, f"bucket {d.bucket} full"))
            continue

        size = strategy.position_size_for(d.bucket)
        ok, reason = strategy.buy_respects_ops_floor(
            current_cash_aud=cash_remaining,
            intended_buy_aud=size,
        )
        if not ok:
            rejected.append((d, reason))
            continue

        allowed.append(d)
        bucket_used_this_round[d.bucket] = bucket_used_this_round.get(d.bucket, 0) + 1
        cluster_used_this_round[cluster] = cluster_used_this_round.get(cluster, 0) + 1
        cash_remaining -= size

    return allowed, rejected


# ── Stock bar fetcher (used by scanner.scan_stocks) ──────────────────────

def _fetch_bars(symbol: str, days: int = 60):
    """
    Fetch daily OHLC bars for a US stock from Alpaca's data API.
    Returns a pandas DataFrame with columns: open, high, low, close, volume.
    Returns None on failure (caller treats this as "skip this symbol").

    This is here in brain.py rather than scanner.py because brain already
    has the Alpaca credentials in scope via config, and scanner is meant to
    be exchange-agnostic.
    """
    try:
        import requests
        import pandas as pd
        from datetime import datetime, timedelta, timezone

        # Lazy-load credentials so import errors are handled gracefully
        try:
            from .config import ALPACA_API_KEY, ALPACA_SECRET_KEY
        except ImportError:
            from bot.config import ALPACA_API_KEY, ALPACA_SECRET_KEY

        if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
            log.warning(f"_fetch_bars {symbol}: no Alpaca credentials")
            return None

        end = datetime.now(timezone.utc) - timedelta(minutes=20)  # avoid SIP delay
        start = end - timedelta(days=days * 2)  # account for weekends/holidays

        url = f"https://data.alpaca.markets/v2/stocks/{symbol}/bars"
        headers = {
            "APCA-API-KEY-ID": ALPACA_API_KEY,
            "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
        }
        params = {
            "timeframe": "1Day",
            "start": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "limit": 1000,
            "adjustment": "raw",
            "feed": "iex",  # free tier; "sip" requires paid plan
        }

        r = requests.get(url, headers=headers, params=params, timeout=10)
        if r.status_code != 200:
            log.debug(f"_fetch_bars {symbol}: HTTP {r.status_code}")
            return None

        bars = r.json().get("bars") or []
        if not bars:
            return None

        df = pd.DataFrame(bars)
        # Alpaca bar fields: t (time), o, h, l, c, v
        df = df.rename(columns={"o": "open", "h": "high", "l": "low",
                                "c": "close", "v": "volume", "t": "timestamp"})
        return df.tail(days)  # cap to requested range
    except Exception as e:
        log.debug(f"_fetch_bars {symbol}: {e}")
        return None
