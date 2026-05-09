package com.github.dimitryivaniuta.gateway.leaderboard.domain;

/**
 * Single ranked leaderboard entry returned by the public API.
 *
 * @param rank one-based rank in the requested Top-K view
 * @param itemId player or item id
 * @param displayName optional display name
 * @param score current score
 */
public record LeaderboardEntry(long rank, String itemId, String displayName, double score) {
}
