package com.github.dimitryivaniuta.gateway.leaderboard.domain;

import java.time.Instant;
import java.util.List;

/**
 * Response returned by the Top-K leaderboard endpoint.
 *
 * @param leaderboardId leaderboard id
 * @param limit requested limit after validation and capping
 * @param generatedAt response creation timestamp
 * @param items ranked leaderboard entries
 */
public record LeaderboardResponse(String leaderboardId, int limit, Instant generatedAt, List<LeaderboardEntry> items) {
}
