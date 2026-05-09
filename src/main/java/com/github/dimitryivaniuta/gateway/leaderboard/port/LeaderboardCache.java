package com.github.dimitryivaniuta.gateway.leaderboard.port;

import com.github.dimitryivaniuta.gateway.leaderboard.domain.LeaderboardEntry;
import com.github.dimitryivaniuta.gateway.leaderboard.domain.ProcessedScoreEvent;
import java.util.List;
import reactor.core.publisher.Mono;

/**
 * Hot leaderboard cache backed by Redis sorted sets.
 */
public interface LeaderboardCache {

    /**
     * Writes the final durable event result into the Redis read model.
     *
     * <p>This method must be idempotent. Re-delivered Kafka events use it to repair Redis after a
     * previous attempt committed PostgreSQL but failed before the cache write.</p>
     *
     * @param processed processed durable event
     * @return completion signal
     */
    Mono<Void> putProcessedEvent(ProcessedScoreEvent processed);

    /**
     * Reads the Top-K entries from the cache.
     *
     * @param leaderboardId leaderboard id
     * @param limit maximum number of entries
     * @return ranked entries
     */
    Mono<List<LeaderboardEntry>> topK(String leaderboardId, int limit);

    /**
     * Writes a snapshot row into the cache during warmup or PostgreSQL fallback repair.
     *
     * @param leaderboardId leaderboard id
     * @param itemId item id
     * @param displayName optional display name
     * @param score score to write
     * @return completion signal
     */
    Mono<Void> putSnapshot(String leaderboardId, String itemId, String displayName, double score);
}
