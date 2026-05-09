package com.github.dimitryivaniuta.gateway.leaderboard.domain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Tests score event helpers.
 */
class ScoreEventTest {

    /**
     * Verifies delta events expose effective delta.
     */
    @Test
    void shouldCreateDeltaEvent() {
        ScoreEvent event = ScoreEvent.delta("global", "player-1", "Alice", 12.5);

        assertNotNull(event.eventId());
        assertEquals(12.5, event.effectiveDelta());
        assertFalse(event.hasAbsoluteScore());
    }

    /**
     * Verifies absolute events are identified correctly.
     */
    @Test
    void shouldCreateAbsoluteEvent() {
        ScoreEvent event = ScoreEvent.absolute("global", "player-1", "Alice", 100.0);

        assertTrue(event.hasAbsoluteScore());
        assertEquals(100.0, event.absoluteScore());
        assertEquals(0.0, event.effectiveDelta());
    }
}
