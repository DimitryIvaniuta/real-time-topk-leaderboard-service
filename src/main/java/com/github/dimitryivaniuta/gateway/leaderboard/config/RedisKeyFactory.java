package com.github.dimitryivaniuta.gateway.leaderboard.config;

import java.util.regex.Pattern;
import org.springframework.stereotype.Component;

/**
 * Builds and validates Redis keys for leaderboard data.
 */
@Component
public class RedisKeyFactory {

    private static final Pattern SAFE_ID = Pattern.compile("^[A-Za-z0-9._:-]{1,80}$");

    private final LeaderboardProperties properties;

    /**
     * Creates a Redis key factory.
     *
     * @param properties application leaderboard settings
     */
    public RedisKeyFactory(LeaderboardProperties properties) {
        this.properties = properties;
    }

    /**
     * Validates a leaderboard identifier and returns the normalized value.
     *
     * @param leaderboardId incoming leaderboard id
     * @return validated leaderboard id
     */
    public String requireSafeLeaderboardId(String leaderboardId) {
        if (leaderboardId == null || !SAFE_ID.matcher(leaderboardId).matches()) {
            throw new IllegalArgumentException("leaderboardId must match ^[A-Za-z0-9._:-]{1,80}$");
        }
        return leaderboardId;
    }

    /**
     * Validates an item identifier and returns the normalized value.
     *
     * @param itemId incoming item id
     * @return validated item id
     */
    public String requireSafeItemId(String itemId) {
        if (itemId == null || !SAFE_ID.matcher(itemId).matches()) {
            throw new IllegalArgumentException("itemId must match ^[A-Za-z0-9._:-]{1,80}$");
        }
        return itemId;
    }

    /**
     * Returns the Redis sorted-set key for leaderboard scores.
     *
     * @param leaderboardId leaderboard id
     * @return Redis key
     */
    public String scoresKey(String leaderboardId) {
        return prefix(leaderboardId) + ":scores";
    }

    /**
     * Returns the Redis hash key for display names.
     *
     * @param leaderboardId leaderboard id
     * @return Redis key
     */
    public String namesKey(String leaderboardId) {
        return prefix(leaderboardId) + ":names";
    }

    private String prefix(String leaderboardId) {
        return properties.getRedisKeyPrefix() + ":leaderboard:" + requireSafeLeaderboardId(leaderboardId);
    }
}
