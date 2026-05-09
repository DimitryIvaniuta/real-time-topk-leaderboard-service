CREATE TABLE IF NOT EXISTS leaderboard_scores (
    leaderboard_id VARCHAR(80) NOT NULL,
    item_id VARCHAR(120) NOT NULL,
    display_name VARCHAR(240),
    score DOUBLE PRECISION NOT NULL DEFAULT 0,
    version BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (leaderboard_id, item_id)
);

CREATE INDEX IF NOT EXISTS idx_leaderboard_scores_top
    ON leaderboard_scores (leaderboard_id, score DESC, item_id DESC);
