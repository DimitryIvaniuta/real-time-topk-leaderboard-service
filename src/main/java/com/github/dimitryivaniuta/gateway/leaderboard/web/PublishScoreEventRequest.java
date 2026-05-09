package com.github.dimitryivaniuta.gateway.leaderboard.web;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

/**
 * HTTP request used by local smoke tests to publish score events into Kafka.
 *
 * @param eventId optional idempotency key; generated when absent
 * @param leaderboardId optional leaderboard id; default is used when absent
 * @param itemId player or item id
 * @param displayName optional display name
 * @param delta score delta
 * @param absoluteScore optional absolute score replacement
 */
public record PublishScoreEventRequest(
        @Size(max = 120) String eventId,
        @Size(max = 80) String leaderboardId,
        @NotBlank @Size(max = 120) String itemId,
        @Size(max = 240) String displayName,
        Double delta,
        Double absoluteScore
) {
}
