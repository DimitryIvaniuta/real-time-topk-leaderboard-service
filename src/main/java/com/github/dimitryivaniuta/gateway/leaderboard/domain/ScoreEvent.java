package com.github.dimitryivaniuta.gateway.leaderboard.domain;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;
import java.util.UUID;

/**
 * Immutable Kafka event that mutates the score of one leaderboard item.
 *
 * @param eventId unique event id used for idempotency
 * @param leaderboardId target leaderboard id
 * @param itemId target player or item id
 * @param displayName optional human-readable display name
 * @param delta score delta; ignored when {@code absoluteScore} is provided
 * @param absoluteScore optional absolute score replacement
 * @param occurredAt event creation timestamp
 */
public record ScoreEvent(
        @NotBlank String eventId,
        @NotBlank String leaderboardId,
        @NotBlank String itemId,
        String displayName,
        Double delta,
        Double absoluteScore,
        @NotNull Instant occurredAt
) {

    /**
     * Creates a delta score event with generated id and current timestamp.
     *
     * @param leaderboardId target leaderboard id
     * @param itemId target item id
     * @param displayName optional display name
     * @param delta score delta
     * @return new score event
     */
    public static ScoreEvent delta(String leaderboardId, String itemId, String displayName, double delta) {
        return new ScoreEvent(UUID.randomUUID().toString(), leaderboardId, itemId, displayName, delta, null, Instant.now());
    }

    /**
     * Creates an absolute-score replacement event with generated id and current timestamp.
     *
     * @param leaderboardId target leaderboard id
     * @param itemId target item id
     * @param displayName optional display name
     * @param score absolute score
     * @return new score event
     */
    public static ScoreEvent absolute(String leaderboardId, String itemId, String displayName, double score) {
        return new ScoreEvent(UUID.randomUUID().toString(), leaderboardId, itemId, displayName, null, score, Instant.now());
    }

    /**
     * Returns the effective delta used when no absolute score is specified.
     *
     * @return delta or zero when absent
     */
    public double effectiveDelta() {
        return delta == null ? 0.0 : delta;
    }

    /**
     * Returns true when this event is an absolute score replacement.
     *
     * @return whether absolute score is present
     */
    public boolean hasAbsoluteScore() {
        return absoluteScore != null;
    }
}
