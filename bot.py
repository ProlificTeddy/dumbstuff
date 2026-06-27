"""
bot.py — Telegram Bot for Polymarket Price-Trigger Buyer
=========================================================
Controls the trading engine entirely from Telegram chat:
  /add     — enter slugs, pick specific markets via inline buttons
  /status  — live view of all tracked tokens
  /remove  — drop a slug or all trackers
  /config  — view settings
  /set     — change target, stop-loss, amount, poll interval
  /live    — toggle real vs dry-run trading
  /kill    — shut down the bot remotely

The monitoring loop runs as an asyncio background task and sends
Telegram notifications when buy/sell triggers fire.
"""

import asyncio
import functools
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Set

from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)

from core import (
    CLOB_AVAILABLE,
    EventType,
    TokenTracker,
    TradeEvent,
    TradeState,
    build_client,
    fetch_markets_by_slug,
    get_midpoint,
    tick_tracker,
)

try:
    from supabase_state import (
        init_supabase,
        acquire_run_lock,
        update_heartbeat,
        release_run_lock,
        save_tracker,
        load_active_trackers,
        update_tracker_state,
        is_token_bought,
        delete_tracker,
        delete_trackers_by_slug,
        delete_all_trackers,
    )
    SUPABASE_AVAILABLE = True
except ImportError:
    SUPABASE_AVAILABLE = False

# ── Environment ───────────────────────────────────────────────────────────────
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("telegram_bot")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
AUTHORIZED_USER_ID = int(os.getenv("AUTHORIZED_USER_ID", "0"))

if not TELEGRAM_BOT_TOKEN:
    log.error("TELEGRAM_BOT_TOKEN is not set in .env — exiting.")
    sys.exit(1)

if not AUTHORIZED_USER_ID:
    log.warning(
        "AUTHORIZED_USER_ID is not set. Bot will accept commands from ANY user. "
        "Set it in .env for security."
    )


# ══════════════════════════════════════════════════════════════════════════════
#  Shared state
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class BotState:
    """Mutable global state shared between handlers and the monitoring task."""
    # ── Active trackers ───────────────────────────────────────────────────
    trackers: List[TokenTracker] = field(default_factory=list)
    client: Optional[object]    = None          # ClobClient once authenticated
    monitoring_task: Optional[asyncio.Task] = None

    # ── Defaults for new trackers ─────────────────────────────────────────
    target_price:    float = 0.85
    stop_loss_price: float = 0.70
    usdc_amount:     float = 1.0
    poll_interval:   int   = 20
    live_trading:    bool  = False

    # ── Pending market selection (per /add flow) ──────────────────────────
    pending_slug_markets: Dict[str, List[Dict]] = field(default_factory=dict)
    pending_entries:      List[tuple]           = field(default_factory=list)
    pending_selected:     Set[int]              = field(default_factory=set)

    # ── Supabase persistence ──────────────────────────────────────────────
    run_id:     Optional[str]    = None
    start_time: float            = field(default_factory=time.time)
    sb_client:  Optional[object] = None         # Supabase client

    # ── Async lock ────────────────────────────────────────────────────────
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


state = BotState()


# ══════════════════════════════════════════════════════════════════════════════
#  Authorization decorator
# ══════════════════════════════════════════════════════════════════════════════

def authorized(func):
    """Silently ignores updates from non-authorized users."""
    @functools.wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        if AUTHORIZED_USER_ID and user and user.id != AUTHORIZED_USER_ID:
            log.warning("Unauthorized access attempt from user %s", user.id)
            return
        return await func(update, context)
    return wrapper


# ══════════════════════════════════════════════════════════════════════════════
#  Telegram message helpers
# ══════════════════════════════════════════════════════════════════════════════

MAX_MSG_LEN = 4096


async def send_long_message(bot, chat_id: int, text: str, **kwargs):
    """Split messages that exceed Telegram's 4096-char limit."""
    while text:
        chunk = text[:MAX_MSG_LEN]
        text  = text[MAX_MSG_LEN:]
        await bot.send_message(chat_id=chat_id, text=chunk, **kwargs)


# ══════════════════════════════════════════════════════════════════════════════
#  Background monitoring loop
# ══════════════════════════════════════════════════════════════════════════════

# Events that get forwarded to Telegram (skip noisy price-update ticks)
_NOTIFY_EVENTS = {
    EventType.BUY_TRIGGERED,
    EventType.BUY_EXECUTED,
    EventType.BUY_FAILED,
    EventType.STOP_LOSS_TRIGGERED,
    EventType.SELL_EXECUTED,
    EventType.SELL_FAILED,
    EventType.CYCLE_COMPLETE,
}


async def monitoring_loop(app: Application) -> None:
    """
    Continuously polls all active trackers and sends Telegram alerts
    when significant events occur (buy triggers, stop-losses, failures).

    Supabase integration:
      - Auto-shutdown at 350 minutes (GitHub Actions 6-hour limit).
      - Heartbeat update every cycle so the run lock stays fresh.
      - Trade events (BUY_EXECUTED, SELL_EXECUTED) are persisted to DB.
      - Duplicate prevention: WATCHING tokens that are already BOUGHT
        in Supabase are synced before tick_tracker runs.
    """
    chat_id = AUTHORIZED_USER_ID
    log.info("Monitoring loop started  (poll every %ds)", state.poll_interval)

    while True:
        try:
            # ── Auto-shutdown at 350 minutes ──────────────────────────
            elapsed = time.time() - state.start_time
            if elapsed >= 350 * 60:
                log.info("350-minute limit reached. Shutting down.")
                if chat_id:
                    await app.bot.send_message(
                        chat_id=chat_id,
                        text=(
                            "⏰ Approaching GitHub Actions timeout (350 min).\n"
                            "Shutting down gracefully.\n"
                            "Re-trigger the workflow to continue — "
                            "all state is saved in Supabase."
                        ),
                    )
                break

            # ── Heartbeat ─────────────────────────────────────────────
            if state.sb_client and state.run_id:
                try:
                    await asyncio.to_thread(
                        update_heartbeat, state.sb_client, state.run_id,
                    )
                except Exception as exc:
                    log.warning("Heartbeat update failed: %s", exc)

            async with state.lock:
                active = [t for t in state.trackers if not t.is_done]

            if active:
                log.info(
                    "─── poll @ %s  [%d active / %d total] ───",
                    datetime.now().strftime("%H:%M:%S"),
                    len(active),
                    len(state.trackers),
                )

                for tracker in active:
                    # ── Duplicate prevention ───────────────────────
                    if (
                        tracker.state == TradeState.WATCHING
                        and state.sb_client
                    ):
                        try:
                            db_row = await asyncio.to_thread(
                                is_token_bought,
                                state.sb_client,
                                tracker.token_id,
                            )
                            if db_row:
                                tracker.state       = TradeState.BOUGHT
                                tracker.buy_price   = db_row["buy_price"]
                                tracker.shares_held = db_row["shares_held"]
                                log.info(
                                    "  %s already BOUGHT in Supabase — synced.",
                                    tracker.label,
                                )
                                continue
                        except Exception as exc:
                            log.warning("Supabase dup-check failed: %s", exc)

                    events = await asyncio.to_thread(
                        tick_tracker, tracker, state.client
                    )
                    for event in events:
                        if event.type in _NOTIFY_EVENTS and chat_id:
                            await send_long_message(
                                app.bot, chat_id, event.message,
                                parse_mode="HTML",
                            )

                        # ── Persist state changes to Supabase ─────
                        if state.sb_client:
                            try:
                                if event.type == EventType.BUY_EXECUTED:
                                    await asyncio.to_thread(
                                        update_tracker_state,
                                        state.sb_client,
                                        tracker.token_id,
                                        "BOUGHT",
                                        buy_price=tracker.buy_price,
                                        shares_held=tracker.shares_held,
                                    )
                                elif event.type == EventType.SELL_EXECUTED:
                                    await asyncio.to_thread(
                                        update_tracker_state,
                                        state.sb_client,
                                        tracker.token_id,
                                        "SOLD",
                                    )
                            except Exception as exc:
                                log.warning(
                                    "Supabase state update failed: %s", exc,
                                )

                # Check if every tracker finished
                async with state.lock:
                    if state.trackers and all(
                        t.is_done for t in state.trackers
                    ):
                        if chat_id:
                            await app.bot.send_message(
                                chat_id=chat_id,
                                text="✅ All tracked tokens have completed their full cycle.",
                            )

            await asyncio.sleep(state.poll_interval)

        except asyncio.CancelledError:
            log.info("Monitoring loop cancelled.")
            break
        except Exception as exc:
            log.error("Monitoring loop error: %s", exc, exc_info=True)
            await asyncio.sleep(5)

    # ── Clean exit: release run lock ──────────────────────────────────
    if state.sb_client and state.run_id:
        try:
            await asyncio.to_thread(
                release_run_lock, state.sb_client, state.run_id, "completed",
            )
        except Exception as exc:
            log.warning("Failed to release run lock: %s", exc)


def ensure_monitoring(app: Application) -> None:
    """Start the monitoring loop if it is not already running."""
    if state.monitoring_task is None or state.monitoring_task.done():
        state.monitoring_task = asyncio.create_task(monitoring_loop(app))
        log.info("Monitoring task (re)started.")


# ══════════════════════════════════════════════════════════════════════════════
#  Inline-keyboard builder for market selection
# ══════════════════════════════════════════════════════════════════════════════

def _build_selection_keyboard(
    entries: List[tuple], selected: Set[int],
) -> InlineKeyboardMarkup:
    buttons: List[List[InlineKeyboardButton]] = []
    for i, (slug, market) in enumerate(entries):
        check = "✅" if i in selected else "⬜"
        label = f"{check} {i}: {market['question'][:45]}"
        buttons.append(
            [InlineKeyboardButton(label, callback_data=f"mkt_toggle:{i}")]
        )
    buttons.append([
        InlineKeyboardButton("Select All", callback_data="mkt_select_all"),
        InlineKeyboardButton("✓ Confirm",  callback_data="mkt_confirm"),
    ])
    return InlineKeyboardMarkup(buttons)


# ══════════════════════════════════════════════════════════════════════════════
#  /start
# ══════════════════════════════════════════════════════════════════════════════

@authorized
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "╔═══════════════════════════════════════════╗\n"
        "║   🤖 <b>Polymarket Price-Trigger Bot</b>       ║\n"
        "╚═══════════════════════════════════════════╝\n\n"
        "<b>Commands:</b>\n"
        "/add <code>slug1, slug2</code>  — Add slugs &amp; pick markets\n"
        "/status  — Show tracked tokens &amp; prices\n"
        "/remove <code>slug | all</code>  — Drop trackers\n"
        "/config  — View current settings\n"
        "/set <code>target|stoploss|amount|poll  value</code>\n"
        "/live <code>on | off</code>  — Toggle real trading\n"
        "/kill  — Shut down the bot remotely\n"
    )
    await update.message.reply_text(text, parse_mode="HTML")


# ══════════════════════════════════════════════════════════════════════════════
#  /add <slug1>, <slug2>, …
# ══════════════════════════════════════════════════════════════════════════════

@authorized
async def cmd_add(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text(
            "Usage:  /add <code>slug1, slug2, …</code>", parse_mode="HTML",
        )
        return

    raw   = " ".join(context.args)
    slugs = [s.strip() for s in raw.split(",") if s.strip()]

    msg = await update.message.reply_text(
        f"🔍 Resolving {len(slugs)} slug(s)…"
    )

    slug_markets: Dict[str, List[Dict]] = {}
    errors: List[str] = []

    for slug in slugs:
        try:
            markets = await asyncio.to_thread(fetch_markets_by_slug, slug)
            slug_markets[slug] = markets
        except Exception as exc:
            errors.append(f"• <code>{slug}</code>: {exc}")

    if errors:
        await update.message.reply_text(
            "⚠️ Some slugs failed:\n" + "\n".join(errors),
            parse_mode="HTML",
        )

    if not slug_markets:
        await msg.edit_text("❌ No valid slugs resolved.")
        return

    # Build flat entry list:  [(slug, market_dict), …]
    entries: List[tuple] = []
    for slug, markets in slug_markets.items():
        for market in markets:
            entries.append((slug, market))

    # Store pending selection state
    async with state.lock:
        state.pending_slug_markets = slug_markets
        state.pending_entries      = entries
        state.pending_selected     = set()

    # Build description text
    lines = ["📋 <b>Select markets to monitor:</b>\n"]
    for i, (slug, market) in enumerate(entries):
        outcomes = ", ".join(tok["outcome"] for tok in market["tokens"])
        lines.append(
            f"<b>{i}.</b>  {market['question']}\n"
            f"    <i>{slug}</i>  │  {outcomes}\n"
        )

    keyboard = _build_selection_keyboard(entries, set())
    await msg.edit_text("\n".join(lines), parse_mode="HTML", reply_markup=keyboard)


# ══════════════════════════════════════════════════════════════════════════════
#  Callback query handler (inline-keyboard interactions)
# ══════════════════════════════════════════════════════════════════════════════

@authorized
async def callback_handler(
    update: Update, context: ContextTypes.DEFAULT_TYPE,
) -> None:
    query = update.callback_query
    await query.answer()
    data = query.data

    # ── Toggle a market checkbox ──────────────────────────────────────────
    if data.startswith("mkt_toggle:"):
        idx = int(data.split(":")[1])
        async with state.lock:
            state.pending_selected.symmetric_difference_update({idx})
            keyboard = _build_selection_keyboard(
                state.pending_entries, state.pending_selected,
            )
        await query.edit_message_reply_markup(reply_markup=keyboard)

    # ── Select / deselect all ─────────────────────────────────────────────
    elif data == "mkt_select_all":
        async with state.lock:
            if len(state.pending_selected) == len(state.pending_entries):
                state.pending_selected = set()
            else:
                state.pending_selected = set(range(len(state.pending_entries)))
            keyboard = _build_selection_keyboard(
                state.pending_entries, state.pending_selected,
            )
        await query.edit_message_reply_markup(reply_markup=keyboard)

    # ── Confirm selection → create trackers ───────────────────────────────
    elif data == "mkt_confirm":
        async with state.lock:
            if not state.pending_selected:
                await query.edit_message_text(
                    "❌ No markets selected. Use /add to try again."
                )
                return

            chosen = [
                state.pending_entries[i]
                for i in sorted(state.pending_selected)
            ]

            new_trackers: List[TokenTracker] = []
            for slug, market in chosen:
                for token in market["tokens"]:
                    new_trackers.append(
                        TokenTracker(
                            slug            = slug,
                            market_question = market["question"],
                            outcome         = token["outcome"],
                            token_id        = token["token_id"],
                            target_price    = state.target_price,
                            stop_loss_price = state.stop_loss_price,
                            usdc_amount     = state.usdc_amount,
                            auto_trade      = state.live_trading,
                        )
                    )

            state.trackers.extend(new_trackers)

            # Persist new trackers to Supabase
            if state.sb_client:
                for t in new_trackers:
                    save_tracker(state.sb_client, t)

            # Clear pending state
            state.pending_slug_markets = {}
            state.pending_entries      = []
            state.pending_selected     = set()

        # ── Confirmation message ──────────────────────────────────────────
        mode  = "🔴 LIVE" if state.live_trading else "⚪ DRY-RUN"
        lines = [f"✅ <b>Now tracking {len(new_trackers)} token(s):</b>\n"]
        for t in new_trackers:
            lines.append(
                f"  • <code>{t.label}</code>  "
                f"target≤{t.target_price:.2f}  stop≤{t.stop_loss_price:.2f}"
            )
        lines.append(f"\nMode: {mode}  │  Poll: {state.poll_interval}s")

        await query.edit_message_text("\n".join(lines), parse_mode="HTML")

        # Ensure monitoring loop is running
        ensure_monitoring(context.application)


# ══════════════════════════════════════════════════════════════════════════════
#  /status
# ══════════════════════════════════════════════════════════════════════════════

@authorized
async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    async with state.lock:
        trackers = list(state.trackers)

    if not trackers:
        await update.message.reply_text(
            "📭 No tokens being tracked. Use /add to start."
        )
        return

    await update.message.reply_text("⏳ Fetching prices…")

    # Fetch all midpoints concurrently
    midpoints = await asyncio.gather(
        *(asyncio.to_thread(get_midpoint, t.token_id) for t in trackers)
    )

    active_count = sum(1 for t in trackers if not t.is_done)
    lines = [
        f"📊 <b>Tracked Tokens</b>  "
        f"({active_count} active / {len(trackers)} total)\n"
    ]

    for t, mid in zip(trackers, midpoints):
        mid_str = f"{mid:.4f}" if mid is not None else "N/A"

        if t.state == TradeState.WATCHING:
            icon   = "🟡"
            detail = f"mid: {mid_str}  │  target: ≤{t.target_price:.4f}"

        elif t.state == TradeState.BOUGHT:
            icon = "🟢"
            pnl  = ""
            if mid is not None and t.buy_price > 0:
                pnl_pct = ((mid - t.buy_price) / t.buy_price) * 100
                pnl = f"  │  P&L: {pnl_pct:+.1f}%"
            detail = (
                f"mid: {mid_str}  │  stop: ≤{t.stop_loss_price:.4f}\n"
                f"     shares: {t.shares_held:.4f}  │  buy@: {t.buy_price:.4f}{pnl}"
            )

        else:   # SOLD
            icon   = "✅"
            detail = f"shares: {t.shares_held:.4f}  │  buy@: {t.buy_price:.4f}"

        lines.append(
            f"{icon} <b>{t.state.value:8s}</b> │ <code>{t.label}</code>\n"
            f"     {detail}\n"
        )

    mode = "🔴 LIVE" if state.live_trading else "⚪ DRY-RUN"
    lines.append(f"\nMode: {mode}")

    await send_long_message(
        update.get_bot(),
        update.effective_chat.id,
        "\n".join(lines),
        parse_mode="HTML",
    )


# ══════════════════════════════════════════════════════════════════════════════
#  /remove <slug | all>
# ══════════════════════════════════════════════════════════════════════════════

@authorized
async def cmd_remove(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text(
            "Usage:  /remove <code>slug</code>  or  /remove <code>all</code>",
            parse_mode="HTML",
        )
        return

    target = " ".join(context.args).strip().lower()

    async with state.lock:
        # Warn about open positions
        if target == "all":
            bought = [t for t in state.trackers if t.state == TradeState.BOUGHT]
            removed = len(state.trackers)
            state.trackers.clear()
            if state.sb_client:
                delete_all_trackers(state.sb_client)
        else:
            removed_trackers = [
                t for t in state.trackers if t.slug.lower() == target
            ]
            bought = [t for t in removed_trackers if t.state == TradeState.BOUGHT]
            state.trackers = [
                t for t in state.trackers if t.slug.lower() != target
            ]
            removed = len(removed_trackers)
            if state.sb_client and removed_trackers:
                delete_trackers_by_slug(state.sb_client, removed_trackers[0].slug)

    if bought:
        names = "\n".join(f"  • {t.label}" for t in bought)
        await update.message.reply_text(
            f"⚠️ <b>Warning:</b> {len(bought)} token(s) were in BOUGHT state:\n{names}",
            parse_mode="HTML",
        )

    if removed:
        await update.message.reply_text(f"🗑 Removed {removed} tracker(s).")
    else:
        await update.message.reply_text(
            f"❓ No trackers found for <code>{target}</code>.",
            parse_mode="HTML",
        )


# ══════════════════════════════════════════════════════════════════════════════
#  /config
# ══════════════════════════════════════════════════════════════════════════════

@authorized
async def cmd_config(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    mode = "🔴 LIVE" if state.live_trading else "⚪ DRY-RUN"
    clob = "✅ installed" if CLOB_AVAILABLE else "❌ not installed"
    auth = "✅ authenticated" if state.client else "⚪ not connected"

    async with state.lock:
        n_total  = len(state.trackers)
        n_active = sum(1 for t in state.trackers if not t.is_done)

    text = (
        "⚙️ <b>Configuration</b>\n\n"
        f"  Buy trigger   :  ≤ {state.target_price:.4f}  ({state.target_price*100:.0f}¢)\n"
        f"  Stop-loss     :  ≤ {state.stop_loss_price:.4f}  ({state.stop_loss_price*100:.0f}¢)\n"
        f"  USDC / trade  :  ${state.usdc_amount:.2f}\n"
        f"  Poll interval :  {state.poll_interval}s\n"
        f"  Trading mode  :  {mode}\n"
        f"  CLOB client   :  {clob}\n"
        f"  Auth status   :  {auth}\n"
        f"  Trackers      :  {n_active} active / {n_total} total\n"
    )
    await update.message.reply_text(text, parse_mode="HTML")


# ══════════════════════════════════════════════════════════════════════════════
#  /set <key> <value>
# ══════════════════════════════════════════════════════════════════════════════

@authorized
async def cmd_set(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if len(context.args) < 2:
        await update.message.reply_text(
            "Usage:  /set <code>target | stoploss | amount | poll</code>  <code>value</code>",
            parse_mode="HTML",
        )
        return

    key = context.args[0].lower()
    try:
        value = float(context.args[1])
    except ValueError:
        await update.message.reply_text("❌ Value must be a number.")
        return

    if key in ("target", "tp"):
        state.target_price = value
        await update.message.reply_text(
            f"✅ Buy trigger set to ≤ <b>{value:.4f}</b>", parse_mode="HTML",
        )
    elif key in ("stoploss", "sl", "stop"):
        state.stop_loss_price = value
        await update.message.reply_text(
            f"✅ Stop-loss set to ≤ <b>{value:.4f}</b>", parse_mode="HTML",
        )
    elif key in ("amount", "amt", "usdc"):
        state.usdc_amount = value
        await update.message.reply_text(
            f"✅ USDC per trade set to <b>${value:.2f}</b>", parse_mode="HTML",
        )
    elif key in ("poll", "interval"):
        state.poll_interval = max(5, int(value))
        await update.message.reply_text(
            f"✅ Poll interval set to <b>{state.poll_interval}s</b>",
            parse_mode="HTML",
        )
    else:
        await update.message.reply_text(
            f"❌ Unknown setting: <code>{key}</code>\n"
            "Valid keys: target, stoploss, amount, poll",
            parse_mode="HTML",
        )


# ══════════════════════════════════════════════════════════════════════════════
#  /live on|off
# ══════════════════════════════════════════════════════════════════════════════

@authorized
async def cmd_live(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        mode = "🔴 ON (LIVE)" if state.live_trading else "⚪ OFF (DRY-RUN)"
        await update.message.reply_text(
            f"Live trading is: {mode}\n\n"
            "Usage:  /live <code>on</code>  or  /live <code>off</code>",
            parse_mode="HTML",
        )
        return

    arg = context.args[0].lower()

    if arg == "on":
        if not CLOB_AVAILABLE:
            await update.message.reply_text(
                "❌ <code>py-clob-client</code> is not installed. "
                "Cannot enable live trading.",
                parse_mode="HTML",
            )
            return

        # Authenticate if needed
        if state.client is None:
            await update.message.reply_text("🔐 Authenticating with Polymarket CLOB…")
            try:
                state.client = await asyncio.to_thread(build_client)
                await update.message.reply_text("✅ Authenticated successfully.")
            except Exception as exc:
                await update.message.reply_text(
                    f"❌ Authentication failed:\n<code>{exc}</code>",
                    parse_mode="HTML",
                )
                return

        state.live_trading = True
        # Update existing trackers
        async with state.lock:
            for t in state.trackers:
                if t.state == TradeState.WATCHING:
                    t.auto_trade = True
                    if state.sb_client:
                        update_tracker_state(
                            state.sb_client, t.token_id,
                            "WATCHING", auto_trade=True,
                        )

        await update.message.reply_text(
            "🔴 <b>LIVE TRADING ENABLED</b>\n"
            "Real orders will be placed on triggers.",
            parse_mode="HTML",
        )

    elif arg == "off":
        state.live_trading = False
        async with state.lock:
            for t in state.trackers:
                if t.state == TradeState.WATCHING:
                    t.auto_trade = False
                    if state.sb_client:
                        update_tracker_state(
                            state.sb_client, t.token_id,
                            "WATCHING", auto_trade=False,
                        )

        await update.message.reply_text(
            "⚪ Live trading <b>disabled</b>. Running in dry-run mode.",
            parse_mode="HTML",
        )
    else:
        await update.message.reply_text(
            "Usage:  /live <code>on</code>  or  /live <code>off</code>",
            parse_mode="HTML",
        )


# ══════════════════════════════════════════════════════════════════════════════
#  /kill  — remote shutdown
# ══════════════════════════════════════════════════════════════════════════════

@authorized
async def cmd_kill(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Show summary of open positions before dying
    async with state.lock:
        bought = [t for t in state.trackers if t.state == TradeState.BOUGHT]

    if bought:
        names = "\n".join(
            f"  • <code>{t.label}</code>  ({t.shares_held:.4f} shares)"
            for t in bought
        )
        await update.message.reply_text(
            f"⚠️ <b>{len(bought)} open position(s) will NOT be closed:</b>\n"
            f"{names}\n\n"
            "Shutting down anyway…",
            parse_mode="HTML",
        )

    await update.message.reply_text("🛑 Bot shutting down. Goodbye!")
    log.info("Kill command received — shutting down.")

    # Cancel the monitoring loop
    if state.monitoring_task and not state.monitoring_task.done():
        state.monitoring_task.cancel()
        try:
            await state.monitoring_task
        except asyncio.CancelledError:
            pass

    # Release Supabase run lock before exiting
    if state.sb_client and state.run_id:
        try:
            release_run_lock(state.sb_client, state.run_id, "completed")
        except Exception as exc:
            log.warning("Failed to release run lock: %s", exc)

    # Hard exit after a short grace period for the reply to send
    await asyncio.sleep(1)
    os._exit(0)


# ══════════════════════════════════════════════════════════════════════════════
#  Application setup
# ══════════════════════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════════════════════
#  Supabase startup hook
# ══════════════════════════════════════════════════════════════════════════════

async def post_init(app: Application) -> None:
    """
    Called after the Telegram Application is initialised but before polling.
    Connects to Supabase, acquires the run lock, and reloads any active
    trackers from the previous session.
    """
    # ── 1. Init Supabase (optional — bot works without it) ────────────────
    if not SUPABASE_AVAILABLE:
        log.warning(
            "supabase_state module not available. "
            "Running without state persistence."
        )
        return

    try:
        state.sb_client = init_supabase()
    except Exception as exc:
        log.warning("Supabase init failed: %s — running without persistence.", exc)
        return

    # ── 2. Acquire run lock ───────────────────────────────────────────────
    github_run_id = os.getenv("GITHUB_RUN_ID", "local")
    try:
        ok, run_id = acquire_run_lock(state.sb_client, github_run_id)
    except Exception as exc:
        log.warning("Run lock check failed: %s — continuing anyway.", exc)
        ok, run_id = True, None

    if not ok:
        log.error("Another bot instance is already running. Exiting.")
        if AUTHORIZED_USER_ID:
            try:
                await app.bot.send_message(
                    chat_id=AUTHORIZED_USER_ID,
                    text="⚠️ Another bot instance is already running. This one will exit.",
                )
            except Exception:
                pass
        await asyncio.sleep(2)
        os._exit(1)

    state.run_id = run_id

    # ── 3. Reload trackers from Supabase ──────────────────────────────────
    try:
        restored = load_active_trackers(state.sb_client)
    except Exception as exc:
        log.warning("Failed to load trackers from Supabase: %s", exc)
        restored = []

    if restored:
        state.trackers = restored

        # Re-authenticate CLOB if any tracker needs live trading
        if any(t.auto_trade for t in restored):
            try:
                state.client = await asyncio.to_thread(build_client)
                state.live_trading = True
                log.info("CLOB client re-authenticated for live trading.")
            except Exception as exc:
                log.warning(
                    "Could not re-auth CLOB: %s — trackers will run dry-run.",
                    exc,
                )
                for t in restored:
                    t.auto_trade = False

        ensure_monitoring(app)

        if AUTHORIZED_USER_ID:
            bought   = sum(1 for t in restored if t.state == TradeState.BOUGHT)
            watching = sum(1 for t in restored if t.state == TradeState.WATCHING)
            try:
                await app.bot.send_message(
                    chat_id=AUTHORIZED_USER_ID,
                    text=(
                        f"♻️ <b>Bot restarted.</b>\n"
                        f"Restored {len(restored)} tracker(s) from previous session "
                        f"({bought} BOUGHT, {watching} WATCHING).\n"
                        f"Monitoring has resumed automatically."
                    ),
                    parse_mode="HTML",
                )
            except Exception:
                pass

    state.start_time = time.time()
    log.info("Post-init complete. Start time recorded for 350-min shutdown.")


def main() -> None:
    print()
    print("=====================================================")
    print("|   Polymarket Price-Trigger Bot  (Telegram)        |")
    print("|   Buy on momentum  *  Stop-loss auto-sell         |")
    print("|   All controlled from Telegram chat               |")
    print("|   State persisted via Supabase                    |")
    print("=====================================================")
    print()

    app = (
        Application.builder()
        .token(TELEGRAM_BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    # ── Register handlers ─────────────────────────────────────────────────
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help",  cmd_start))
    app.add_handler(CommandHandler("add",   cmd_add))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("remove", cmd_remove))
    app.add_handler(CommandHandler("config", cmd_config))
    app.add_handler(CommandHandler("set",   cmd_set))
    app.add_handler(CommandHandler("live",  cmd_live))
    app.add_handler(CommandHandler("kill",  cmd_kill))
    app.add_handler(CallbackQueryHandler(callback_handler))

    log.info("Bot starting — polling for updates…")
    log.info("Authorized user ID: %s", AUTHORIZED_USER_ID or "ANY")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
