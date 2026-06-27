-- ═══════════════════════════════════════════════════════════════════════════════
--  Polymarket Bot — Supabase Schema
--  Run this in the Supabase SQL Editor (https://app.supabase.com → SQL Editor)
-- ═══════════════════════════════════════════════════════════════════════════════


-- ── bot_runs ─────────────────────────────────────────────────────────────────
-- Prevents two GitHub Actions runs from operating simultaneously.
-- Each run inserts a row on startup and updates last_heartbeat every poll cycle.
-- A heartbeat older than 10 minutes is considered a crashed run.

CREATE TABLE IF NOT EXISTS bot_runs (
    id              UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    github_run_id   TEXT,
    started_at      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    last_heartbeat  TIMESTAMPTZ     NOT NULL DEFAULT now(),
    status          TEXT            NOT NULL DEFAULT 'running'
                    CHECK (status IN ('running', 'completed', 'failed'))
);

-- Index for the lock query (find active runs quickly)
CREATE INDEX IF NOT EXISTS idx_bot_runs_status
    ON bot_runs (status, last_heartbeat);


-- ── active_trades ────────────────────────────────────────────────────────────
-- Mirrors the in-memory TokenTracker list (bot.py → state.trackers).
-- Every column maps 1:1 to a field on core.py's TokenTracker dataclass.
-- token_id is UNIQUE so upserts on the same token replace rather than duplicate.

CREATE TABLE IF NOT EXISTS active_trades (
    id              UUID                PRIMARY KEY DEFAULT gen_random_uuid(),
    token_id        TEXT                NOT NULL UNIQUE,
    slug            TEXT                NOT NULL,
    market_question TEXT                NOT NULL,
    outcome         TEXT                NOT NULL,
    state           TEXT                NOT NULL DEFAULT 'WATCHING'
                    CHECK (state IN ('WATCHING', 'BOUGHT', 'SOLD')),
    target_price    DOUBLE PRECISION    NOT NULL,
    stop_loss_price DOUBLE PRECISION    NOT NULL,
    usdc_amount     DOUBLE PRECISION    NOT NULL,
    auto_trade      BOOLEAN             NOT NULL DEFAULT false,
    buy_price       DOUBLE PRECISION    NOT NULL DEFAULT 0,
    shares_held     DOUBLE PRECISION    NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ         NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ         NOT NULL DEFAULT now()
);

-- Index for the startup reload query (find active trackers)
CREATE INDEX IF NOT EXISTS idx_active_trades_state
    ON active_trades (state);
