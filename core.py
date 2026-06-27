"""
core.py — Polymarket Price-Trigger Trading Engine
===================================================
Extracted trading logic: state machine, market resolution, order execution.
Designed to be imported by the Telegram bot or any other frontend.

tick_tracker() returns structured TradeEvent objects so the caller
can decide how to present them (Telegram messages, CLI logs, etc.).
"""

import json
import logging
import os
import sys
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional

import requests
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger("polymarket_core")

# ── Constants ─────────────────────────────────────────────────────────────────
HOST      = "https://clob.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"
CHAIN_ID  = 137   # Polygon mainnet

# ── py-clob-client imports (graceful fallback) ────────────────────────────────
try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import MarketOrderArgs, OrderType
    from py_clob_client.order_builder.constants import BUY, SELL

    CLOB_AVAILABLE = True
except ImportError:
    CLOB_AVAILABLE = False
    ClobClient = None  # type: ignore
    log.warning(
        "py-clob-client is not installed. Live trading will be unavailable.\n"
        "Install with:  pip install py-clob-client"
    )


# ══════════════════════════════════════════════════════════════════════════════
#  Data types
# ══════════════════════════════════════════════════════════════════════════════

class TradeState(Enum):
    WATCHING = "WATCHING"   # waiting for buy trigger
    BOUGHT   = "BOUGHT"     # position open, watching stop-loss
    SOLD     = "SOLD"       # stop-loss fired — done


class EventType(Enum):
    """Categories of events emitted by tick_tracker."""
    PRICE_UPDATE         = "PRICE_UPDATE"
    NO_DATA              = "NO_DATA"
    BUY_TRIGGERED        = "BUY_TRIGGERED"
    BUY_EXECUTED         = "BUY_EXECUTED"
    BUY_FAILED           = "BUY_FAILED"
    STOP_LOSS_TRIGGERED  = "STOP_LOSS_TRIGGERED"
    SELL_EXECUTED         = "SELL_EXECUTED"
    SELL_FAILED           = "SELL_FAILED"
    CYCLE_COMPLETE       = "CYCLE_COMPLETE"


@dataclass
class TradeEvent:
    """Structured event returned by tick_tracker for the UI layer."""
    type:          EventType
    tracker_label: str
    message:       str
    mid_price:     Optional[float] = None
    details:       Optional[dict]  = None


@dataclass
class TokenTracker:
    """Tracks the full lifecycle of one outcome token across both phases."""
    slug:            str
    market_question: str
    outcome:         str
    token_id:        str
    target_price:    float
    stop_loss_price: float
    usdc_amount:     float
    auto_trade:      bool

    state:        TradeState = field(default=TradeState.WATCHING, init=False)
    shares_held:  float      = field(default=0.0,                 init=False)
    buy_price:    float      = field(default=0.0,                 init=False)

    @property
    def label(self) -> str:
        return f"[{self.slug[:25]}] {self.outcome}"

    @property
    def is_done(self) -> bool:
        return self.state == TradeState.SOLD


# ══════════════════════════════════════════════════════════════════════════════
#  Authentication
# ══════════════════════════════════════════════════════════════════════════════

def build_client() -> "ClobClient":
    """Create and authenticate a ClobClient from environment variables."""
    if not CLOB_AVAILABLE:
        raise ImportError(
            "py-clob-client is not installed. "
            "Run:  pip install py-clob-client"
        )

    private_key    = os.getenv("PRIVATE_KEY", "").strip()
    funder_address = os.getenv("FUNDER_ADDRESS", "").strip()
    sig_type       = int(os.getenv("SIGNATURE_TYPE", "1"))

    if not private_key:
        raise EnvironmentError("PRIVATE_KEY is not set. Add it to your .env file.")

    kwargs: dict = dict(
        host=HOST,
        key=private_key,
        chain_id=CHAIN_ID,
        signature_type=sig_type,
    )
    if funder_address:
        kwargs["funder"] = funder_address

    log.info("Connecting to Polymarket CLOB …")
    client = ClobClient(**kwargs)
    log.info("Deriving API credentials …")
    client.set_api_creds(client.create_or_derive_api_creds())
    log.info("Authentication successful ✓")
    return client


# ══════════════════════════════════════════════════════════════════════════════
#  Market helpers
# ══════════════════════════════════════════════════════════════════════════════

def fetch_markets_by_slug(slug: str) -> List[Dict]:
    """
    Resolve a Gamma event slug into a list of market dicts.

    Each dict has:
        {"question": str, "tokens": [{"outcome": str, "token_id": str}, ...]}
    """
    r = requests.get(f"{GAMMA_API}/events", params={"slug": slug}, timeout=10)
    r.raise_for_status()
    events = r.json()
    if not events:
        raise ValueError(f"No event found for slug: {slug!r}")

    markets: List[Dict] = []
    for m in events[0].get("markets", []):
        tids = m.get("clobTokenIds", [])
        outs = m.get("outcomes", [])
        if isinstance(tids, str):
            tids = json.loads(tids)
        if isinstance(outs, str):
            outs = json.loads(outs)
        markets.append(
            {
                "question": m["question"],
                "tokens": [
                    {"outcome": o, "token_id": t} for o, t in zip(outs, tids)
                ],
            }
        )
    return markets


def get_midpoint(token_id: str) -> Optional[float]:
    """Fetch the current mid-price for a CLOB token."""
    try:
        r = requests.get(
            f"{HOST}/midpoint", params={"token_id": token_id}, timeout=5
        )
        if r.ok:
            return float(r.json()["mid"])
    except Exception as exc:
        log.warning("Midpoint fetch failed for %s: %s", token_id[:12], exc)
    return None


# ══════════════════════════════════════════════════════════════════════════════
#  Order execution
# ══════════════════════════════════════════════════════════════════════════════

def place_market_buy(client, token_id: str, usdc_amount: float) -> dict:
    """Place a Fill-Or-Kill market BUY order."""
    log.info(
        "Placing MARKET BUY  token=%s…  amount=$%.2f USDC",
        token_id[:16], usdc_amount,
    )
    order_args = MarketOrderArgs(
        token_id=token_id, amount=usdc_amount, side=BUY,
    )
    signed = client.create_market_order(order_args)
    resp   = client.post_order(signed, OrderType.FOK)
    log.info("BUY response: %s", resp)
    return resp


def place_market_sell(client, token_id: str, share_amount: float) -> dict:
    """Place a Fill-Or-Kill market SELL order for a given number of shares."""
    log.info(
        "Placing MARKET SELL  token=%s…  shares=%.4f",
        token_id[:16], share_amount,
    )
    order_args = MarketOrderArgs(
        token_id=token_id, amount=share_amount, side=SELL,
    )
    signed = client.create_market_order(order_args)
    resp   = client.post_order(signed, OrderType.FOK)
    log.info("SELL response: %s", resp)
    return resp


# ══════════════════════════════════════════════════════════════════════════════
#  Per-token state-machine tick
# ══════════════════════════════════════════════════════════════════════════════

def tick_tracker(tracker: TokenTracker, client=None) -> List[TradeEvent]:
    """
    Advance the state machine for one token by one polling tick.
    Returns a list of TradeEvent objects describing what happened.

    WATCHING → mid <= target   → BUY  → BOUGHT
    BOUGHT   → mid <= stop-loss → SELL → SOLD
    """
    events: List[TradeEvent] = []

    if tracker.is_done:
        return events

    mid = get_midpoint(tracker.token_id)
    if mid is None:
        return events   # silently skip — no need to spam

    # ── WATCHING state ────────────────────────────────────────────────────
    if tracker.state == TradeState.WATCHING:
        log.info(
            "  %-35s | WATCHING  mid=%.4f  target=%.4f",
            tracker.label, mid, tracker.target_price,
        )

        if mid >= tracker.target_price:
            tracker.buy_price   = mid
            tracker.shares_held = tracker.usdc_amount / mid if mid > 0 else 0.0

            events.append(TradeEvent(
                type=EventType.BUY_TRIGGERED,
                tracker_label=tracker.label,
                mid_price=mid,
                message=(
                    f"🎯 <b>BUY TRIGGER</b>\n"
                    f"<code>{tracker.label}</code>\n"
                    f"mid = {mid:.4f}  ≥  target = {tracker.target_price:.4f}"
                ),
            ))

            if tracker.auto_trade and client is not None:
                try:
                    resp = place_market_buy(
                        client, tracker.token_id, tracker.usdc_amount
                    )
                    # Try to read actual filled size
                    if isinstance(resp, dict):
                        filled = (
                            resp.get("size_matched")
                            or resp.get("sizeFilled")
                        )
                        if filled:
                            tracker.shares_held = float(filled)

                    events.append(TradeEvent(
                        type=EventType.BUY_EXECUTED,
                        tracker_label=tracker.label,
                        mid_price=mid,
                        message=(
                            f"✅ <b>BUY EXECUTED</b>\n"
                            f"<code>{tracker.label}</code>\n"
                            f"${tracker.usdc_amount:.2f} USDC → "
                            f"~{tracker.shares_held:.4f} shares @ {mid:.4f}"
                        ),
                        details=resp if isinstance(resp, dict) else None,
                    ))
                except Exception as exc:
                    events.append(TradeEvent(
                        type=EventType.BUY_FAILED,
                        tracker_label=tracker.label,
                        mid_price=mid,
                        message=(
                            f"❌ <b>BUY FAILED</b>\n"
                            f"<code>{tracker.label}</code>\n"
                            f"{exc}"
                        ),
                    ))
                    return events   # stay in WATCHING
            else:
                events.append(TradeEvent(
                    type=EventType.BUY_EXECUTED,
                    tracker_label=tracker.label,
                    mid_price=mid,
                    message=(
                        f"🏷 <b>DRY-RUN BUY</b>\n"
                        f"<code>{tracker.label}</code>\n"
                        f"Would buy ${tracker.usdc_amount:.2f} "
                        f"@ {mid:.4f} (~{tracker.shares_held:.4f} shares)"
                    ),
                ))

            tracker.state = TradeState.BOUGHT
            log.info(
                "  → %s  now BOUGHT  shares=%.4f  stop=%.4f",
                tracker.label, tracker.shares_held, tracker.stop_loss_price,
            )

    # ── BOUGHT state ──────────────────────────────────────────────────────
    elif tracker.state == TradeState.BOUGHT:
        log.info(
            "  %-35s | BOUGHT   mid=%.4f  stop=%.4f",
            tracker.label, mid, tracker.stop_loss_price,
        )

        if mid <= tracker.stop_loss_price:
            events.append(TradeEvent(
                type=EventType.STOP_LOSS_TRIGGERED,
                tracker_label=tracker.label,
                mid_price=mid,
                message=(
                    f"🛑 <b>STOP-LOSS TRIGGERED</b>\n"
                    f"<code>{tracker.label}</code>\n"
                    f"mid = {mid:.4f}  ≤  stop = {tracker.stop_loss_price:.4f}"
                ),
            ))

            if tracker.auto_trade and client is not None:
                try:
                    resp = place_market_sell(
                        client, tracker.token_id, tracker.shares_held
                    )
                    events.append(TradeEvent(
                        type=EventType.SELL_EXECUTED,
                        tracker_label=tracker.label,
                        mid_price=mid,
                        message=(
                            f"✅ <b>SELL EXECUTED</b>\n"
                            f"<code>{tracker.label}</code>\n"
                            f"Sold {tracker.shares_held:.4f} shares @ {mid:.4f}"
                        ),
                        details=resp if isinstance(resp, dict) else None,
                    ))
                except Exception as exc:
                    events.append(TradeEvent(
                        type=EventType.SELL_FAILED,
                        tracker_label=tracker.label,
                        mid_price=mid,
                        message=(
                            f"❌ <b>SELL FAILED</b>\n"
                            f"<code>{tracker.label}</code>\n"
                            f"{exc}"
                        ),
                    ))
                    return events   # stay in BOUGHT
            else:
                events.append(TradeEvent(
                    type=EventType.SELL_EXECUTED,
                    tracker_label=tracker.label,
                    mid_price=mid,
                    message=(
                        f"🏷 <b>DRY-RUN SELL</b>\n"
                        f"<code>{tracker.label}</code>\n"
                        f"Would sell {tracker.shares_held:.4f} shares @ {mid:.4f}"
                    ),
                ))

            tracker.state = TradeState.SOLD
            events.append(TradeEvent(
                type=EventType.CYCLE_COMPLETE,
                tracker_label=tracker.label,
                mid_price=mid,
                message=(
                    f"🏁 <b>CYCLE COMPLETE</b>\n"
                    f"<code>{tracker.label}</code>\n"
                    f"Buy @ {tracker.buy_price:.4f} → Sell @ {mid:.4f}"
                ),
            ))
            log.info("  → %s  SOLD. Full cycle complete.", tracker.label)

    return events
