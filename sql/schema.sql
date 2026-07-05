-- PSX Candle Pipeline — Database Schema
-- Supports both PostgreSQL (TIMESTAMPTZ) and SQLite (TEXT ISO-8601)

CREATE TABLE IF NOT EXISTS candles_1m (
    symbol      TEXT          NOT NULL,
    ts          TIMESTAMPTZ   NOT NULL,          -- minute bucket start, stored UTC
    open        NUMERIC(18,6) NOT NULL,
    high        NUMERIC(18,6) NOT NULL,
    low         NUMERIC(18,6) NOT NULL,
    close       NUMERIC(18,6) NOT NULL,
    volume      BIGINT        NOT NULL DEFAULT 0,
    had_trade   BOOLEAN       NOT NULL DEFAULT TRUE,
    PRIMARY KEY (symbol, ts)
);
CREATE INDEX IF NOT EXISTS idx_candles_symbol_ts ON candles_1m (symbol, ts);

CREATE TABLE IF NOT EXISTS day_status (
    symbol        TEXT NOT NULL,                 -- '*' allowed for market-wide
    trade_date    DATE NOT NULL,
    status        TEXT NOT NULL,                 -- 'COMPLETE' | 'MISSING' | 'CONFLICT'
    reason        TEXT,                          -- 'absent' | 'corrupt' | 'conflict' | NULL
    minutes_count INTEGER,
    first_ts      TIMESTAMPTZ,
    last_ts       TIMESTAMPTZ,
    built_at      TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, trade_date)
);
