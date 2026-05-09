package com.github.dimitryivaniuta.gateway.leaderboard.service;

import com.github.dimitryivaniuta.gateway.leaderboard.domain.LeaderboardResponse;
import com.github.dimitryivaniuta.gateway.leaderboard.domain.ProcessedScoreEvent;
import com.github.dimitryivaniuta.gateway.leaderboard.domain.ScoreEvent;
import reactor.core.publisher.Mono;

/**
 * Application service API for leaderboard operations.
 */
public interface LeaderboardService {

    /**
     * Processes a score event through Redis and PostgreSQL.
     *
     * @param event incoming score event
     * @return processed event, or empty result when the event is duplicate
     */
    Mono<ProcessedScoreEvent> process(ScoreEvent event);

    /**
     * Reads a Top-K leaderboard response.
     *
     * @param leaderboardId requested leaderboard id
     * @param limit requested result limit
     * @return leaderboard response
     */
    Mono<LeaderboardResponse> topK(String leaderboardId, Integer limit);
}
