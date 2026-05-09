CREATE TABLE IF NOT EXISTS leaderboard_processed_events (
    event_id VARCHAR(120) NOT NULL PRIMARY KEY,
    leaderboard_id VARCHAR(80) NOT NULL,
    item_id VARCHAR(120) NOT NULL,
    display_name VARCHAR(240),
    final_display_name VARCHAR(240),
    delta DOUBLE PRECISION,
    absolute_score DOUBLE PRECISION,
    new_score DOUBLE PRECISION,
    occurred_at TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    duplicate_count BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT chk_leaderboard_processed_events_one_mutation
        CHECK ((delta IS NULL) <> (absolute_score IS NULL))
);

CREATE INDEX IF NOT EXISTS idx_leaderboard_processed_events_leaderboard_time
    ON leaderboard_processed_events (leaderboard_id, processed_at DESC);

CREATE INDEX IF NOT EXISTS idx_leaderboard_processed_events_item
    ON leaderboard_processed_events (leaderboard_id, item_id, processed_at DESC);
