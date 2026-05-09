package com.github.dimitryivaniuta.gateway.leaderboard.port;

import com.github.dimitryivaniuta.gateway.leaderboard.domain.LeaderboardEntry;
import com.github.dimitryivaniuta.gateway.leaderboard.domain.ProcessedScoreEvent;
import com.github.dimitryivaniuta.gateway.leaderboard.domain.ScoreEvent;
import java.util.List;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Durable score snapshot storage.
 */
public interface ScoreSnapshotStore {

    /**
     * Applies a normalized score event exactly once in PostgreSQL and returns the resulting score.
     *
     * <p>The operation owns durable idempotency. Redis is treated as a rebuildable read model, so
     * the database must decide whether an event is new before the cache is updated.</p>
     *
     * @param event normalized score event
     * @return processed event with final score and duplicate marker
     */
    Mono<ProcessedScoreEvent> apply(ScoreEvent event);

    /**
     * Reads the highest score snapshots for cache warmup or fallback diagnostics.
     *
     * @param leaderboardId leaderboard id
     * @param limit maximum number of rows
     * @return snapshot entries ordered by score descending
     */
    Mono<List<LeaderboardEntry>> topSnapshots(String leaderboardId, int limit);

    /**
     * Streams known leaderboard identifiers that have durable score snapshots.
     *
     * @param limit maximum number of leaderboard ids to return
     * @return leaderboard ids ordered by recent updates
     */
    Flux<String> leaderboardIds(int limit);
}
