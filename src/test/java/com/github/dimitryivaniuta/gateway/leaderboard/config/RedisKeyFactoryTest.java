package com.github.dimitryivaniuta.gateway.leaderboard.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/**
 * Tests Redis key generation and safety validation.
 */
class RedisKeyFactoryTest {

    /**
     * Verifies valid ids produce deterministic Redis keys.
     */
    @Test
    void shouldBuildDeterministicKeysForSafeLeaderboardId() {
        LeaderboardProperties properties = new LeaderboardProperties();
        properties.setRedisKeyPrefix("test");
        RedisKeyFactory factory = new RedisKeyFactory(properties);

        assertEquals("test:leaderboard:global:scores", factory.scoresKey("global"));
        assertEquals("test:leaderboard:global:names", factory.namesKey("global"));
    }

    /**
     * Verifies unsafe ids are rejected before becoming Redis keys.
     */
    @Test
    void shouldRejectUnsafeIds() {
        RedisKeyFactory factory = new RedisKeyFactory(new LeaderboardProperties());

        assertThrows(IllegalArgumentException.class, () -> factory.scoresKey("bad id"));
        assertThrows(IllegalArgumentException.class, () -> factory.requireSafeItemId("../../admin"));
    }
}
