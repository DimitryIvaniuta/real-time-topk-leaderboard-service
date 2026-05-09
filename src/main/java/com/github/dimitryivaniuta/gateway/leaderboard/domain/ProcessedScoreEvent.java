package com.github.dimitryivaniuta.gateway.leaderboard.domain;

import java.time.Instant;

/**
 * Result of durably applying a score event and preparing the hot Redis read model update.
 *
 * <p>The {@code duplicate} flag is important for at-least-once Kafka delivery. A duplicate event
 * must not change the PostgreSQL score again, but it can still repair Redis with the already
 * persisted final score when a previous delivery failed after the database commit.</p>
 *
 * @param event normalized source event, or the originally stored event for duplicate deliveries
 * @param newScore score after durable mutation
 * @param displayName final display name to keep in Redis and API responses
 * @param duplicate whether the event id had already been durably processed
 * @param processedAt server-side processing timestamp
 */
public record ProcessedScoreEvent(
        ScoreEvent event,
        double newScore,
        String displayName,
        boolean duplicate,
        Instant processedAt
) {
}
