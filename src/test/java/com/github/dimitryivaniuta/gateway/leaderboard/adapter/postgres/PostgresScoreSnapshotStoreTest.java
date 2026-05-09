package com.github.dimitryivaniuta.gateway.leaderboard.adapter.postgres;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/**
 * Lightweight schema tests that verify Flyway migrations contain the required durable tables.
 */
class PostgresScoreSnapshotStoreTest {

    /**
     * Ensures the snapshot migration contains primary key and Top-K index definitions.
     *
     * @throws Exception when the migration file cannot be read
     */
    @Test
    void migrationShouldDefineSnapshotTableAndTopIndex() throws Exception {
        String migration = Files.readString(Path.of("src/main/resources/db/migration/V1__create_leaderboard_scores.sql"));

        assertTrue(migration.contains("CREATE TABLE IF NOT EXISTS leaderboard_scores"));
        assertTrue(migration.contains("PRIMARY KEY (leaderboard_id, item_id)"));
        assertTrue(migration.contains("idx_leaderboard_scores_top"));
    }

    /**
     * Ensures the idempotency migration contains event id uniqueness and mutation constraints.
     *
     * @throws Exception when the migration file cannot be read
     */
    @Test
    void migrationShouldDefineProcessedEventsTable() throws Exception {
        String migration = Files.readString(Path.of(
                "src/main/resources/db/migration/V2__create_processed_score_events.sql"
        ));

        assertTrue(migration.contains("CREATE TABLE IF NOT EXISTS leaderboard_processed_events"));
        assertTrue(migration.contains("event_id VARCHAR(120) NOT NULL PRIMARY KEY"));
        assertTrue(migration.contains("chk_leaderboard_processed_events_one_mutation"));
        assertTrue(migration.contains("idx_leaderboard_processed_events_leaderboard_time"));
    }
}
