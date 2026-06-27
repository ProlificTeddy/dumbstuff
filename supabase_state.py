"""
supabase_state.py — Supabase State Persistence Layer
=====================================================
Mirrors bot.py's in-memory state.trackers to a Supabase database
so that open positions survive GitHub Actions restarts.

All Supabase reads/writes live here — neither core.py nor the
Telegram handler logic needs to know anything about the database.
"""

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

from supabase import create_client, Client

from core import TokenTracker, TradeState

log = logging.getLogger("supabase_state")


# ══════════════════════════════════════════════════════════════════════════════
#  Client initialisation
# ══════════════════════════════════════════════════════════════════════════════

def init_supabase() -> Client:
    """Create a Supabase client from environment variables."""
    url = os.getenv("SUPABASE_URL", "").strip()
    key = os.getenv("SUPABASE_KEY", "").strip()

    if not url or not key:
        raise EnvironmentError(
            "SUPABASE_URL and SUPABASE_KEY must be set in .env or environment."
        )

    client = create_client(url, key)
    log.info("Supabase client initialized ✓")
    return client


# ══════════════════════════════════════════════════════════════════════════════
#  Run lock  (bot_runs table)
# ══════════════════════════════════════════════════════════════════════════════

def acquire_run_lock(
    client: Client, github_run_id: str = "local",
) -> Tuple[bool, Optional[str]]:
    """
    Try to acquire the run lock.

    1. Mark any stale 'running' rows (heartbeat > 10 min old) as 'failed'.
    2. Check if a 'running' row with a fresh heartbeat still exists.
    3. If clear → insert a new row → return (True, run_id).
    4. If blocked → return (False, None).
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()

    # Mark stale runs as failed
    client.table("bot_runs").update(
        {"status": "failed"}
    ).eq("status", "running").lt("last_heartbeat", cutoff).execute()

    # Check for a live runner
    result = (
        client.table("bot_runs")
        .select("id")
        .eq("status", "running")
        .gte("last_heartbeat", cutoff)
        .execute()
    )
    if result.data:
        log.warning(
            "Another instance is running (run_id=%s)", result.data[0]["id"]
        )
        return False, None

    # Insert new run
    result = client.table("bot_runs").insert({
        "github_run_id": github_run_id,
        "status": "running",
    }).execute()

    run_id = result.data[0]["id"]
    log.info("Run lock acquired (run_id=%s)", run_id)
    return True, run_id


def update_heartbeat(client: Client, run_id: str) -> None:
    """Update the heartbeat timestamp for the current run."""
    client.table("bot_runs").update(
        {"last_heartbeat": datetime.now(timezone.utc).isoformat()}
    ).eq("id", run_id).execute()


def release_run_lock(
    client: Client, run_id: str, status: str = "completed",
) -> None:
    """Mark the current run as completed or failed."""
    client.table("bot_runs").update({
        "status": status,
        "last_heartbeat": datetime.now(timezone.utc).isoformat(),
    }).eq("id", run_id).execute()
    log.info("Run lock released (run_id=%s, status=%s)", run_id, status)


# ══════════════════════════════════════════════════════════════════════════════
#  Trade state  (active_trades table)
# ══════════════════════════════════════════════════════════════════════════════

def save_tracker(client: Client, tracker: TokenTracker) -> None:
    """Upsert a TokenTracker into the active_trades table."""
    row = {
        "token_id":        tracker.token_id,
        "slug":            tracker.slug,
        "market_question": tracker.market_question,
        "outcome":         tracker.outcome,
        "state":           tracker.state.value,
        "target_price":    tracker.target_price,
        "stop_loss_price": tracker.stop_loss_price,
        "usdc_amount":     tracker.usdc_amount,
        "auto_trade":      tracker.auto_trade,
        "buy_price":       tracker.buy_price,
        "shares_held":     tracker.shares_held,
        "updated_at":      datetime.now(timezone.utc).isoformat(),
    }
    client.table("active_trades").upsert(
        row, on_conflict="token_id",
    ).execute()
    log.info(
        "Saved tracker to Supabase: %s [%s]",
        tracker.label, tracker.state.value,
    )


def load_active_trackers(client: Client) -> List[TokenTracker]:
    """
    Load all WATCHING and BOUGHT trackers from Supabase.

    Returns a list of TokenTracker objects with their state fully restored
    (including buy_price, shares_held, auto_trade).
    """
    result = (
        client.table("active_trades")
        .select("*")
        .in_("state", ["WATCHING", "BOUGHT"])
        .execute()
    )

    trackers: List[TokenTracker] = []
    for row in result.data:
        tracker = TokenTracker(
            slug=row["slug"],
            market_question=row["market_question"],
            outcome=row["outcome"],
            token_id=row["token_id"],
            target_price=row["target_price"],
            stop_loss_price=row["stop_loss_price"],
            usdc_amount=row["usdc_amount"],
            auto_trade=row["auto_trade"],
        )
        # Restore non-init fields that dataclass sets via field(init=False)
        tracker.state       = TradeState(row["state"])
        tracker.buy_price   = row["buy_price"]
        tracker.shares_held = row["shares_held"]
        trackers.append(tracker)

    log.info("Loaded %d active tracker(s) from Supabase", len(trackers))
    return trackers


def update_tracker_state(
    client: Client,
    token_id: str,
    new_state: str,
    **kwargs,
) -> None:
    """
    Update the state of a tracker in Supabase.

    Accepts optional keyword arguments: buy_price, shares_held, auto_trade.
    """
    update: Dict = {
        "state":      new_state,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    for key in ("buy_price", "shares_held", "auto_trade"):
        if key in kwargs:
            update[key] = kwargs[key]

    client.table("active_trades").update(update).eq(
        "token_id", token_id,
    ).execute()
    log.info("Updated tracker %s… → %s", token_id[:16], new_state)


def is_token_bought(client: Client, token_id: str) -> Optional[Dict]:
    """
    Check if a token is already in BOUGHT state in Supabase.

    Returns the row dict if BOUGHT, None otherwise.
    Used as a safety net to prevent re-buying after a restart.
    """
    result = (
        client.table("active_trades")
        .select("buy_price, shares_held")
        .eq("token_id", token_id)
        .eq("state", "BOUGHT")
        .execute()
    )
    if result.data:
        return result.data[0]
    return None


def delete_tracker(client: Client, token_id: str) -> None:
    """Delete a single tracker from Supabase by token_id."""
    client.table("active_trades").delete().eq(
        "token_id", token_id,
    ).execute()
    log.info("Deleted tracker %s… from Supabase", token_id[:16])


def delete_trackers_by_slug(client: Client, slug: str) -> None:
    """Delete all trackers for a given slug from Supabase."""
    client.table("active_trades").delete().eq(
        "slug", slug,
    ).execute()
    log.info("Deleted all trackers for slug '%s' from Supabase", slug)


def delete_all_trackers(client: Client) -> None:
    """Delete all tracker rows from Supabase."""
    client.table("active_trades").delete().gte(
        "created_at", "1970-01-01T00:00:00Z",
    ).execute()
    log.info("Deleted all trackers from Supabase")
