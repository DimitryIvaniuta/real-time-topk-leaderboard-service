package com.github.dimitryivaniuta.gateway.leaderboard.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.github.dimitryivaniuta.gateway.leaderboard.config.LeaderboardProperties;
import com.github.dimitryivaniuta.gateway.leaderboard.config.RedisKeyFactory;
import com.github.dimitryivaniuta.gateway.leaderboard.domain.LeaderboardEntry;
import com.github.dimitryivaniuta.gateway.leaderboard.domain.ProcessedScoreEvent;
import com.github.dimitryivaniuta.gateway.leaderboard.domain.ScoreEvent;
import com.github.dimitryivaniuta.gateway.leaderboard.port.LeaderboardCache;
import com.github.dimitryivaniuta.gateway.leaderboard.port.ScoreSnapshotStore;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * Unit tests for the default leaderboard service.
 */
class DefaultLeaderboardServiceTest {

    /**
     * Verifies limit capping and response creation.
     */
    @Test
    void shouldCapTopKLimit() {
        LeaderboardProperties properties = new LeaderboardProperties();
        properties.setMaxLimit(2);
        FakeCache cache = new FakeCache();
        DefaultLeaderboardService service = service(properties, cache, new FakeSnapshotStore());

        StepVerifier.create(service.topK("global", 100))
                .assertNext(response -> {
                    assertEquals(2, response.limit());
                    assertEquals(2, response.items().size());
                    assertEquals("p1", response.items().getFirst().itemId());
                })
                .verifyComplete();
    }

    /**
     * Verifies a non-duplicate event is first applied in PostgreSQL and then written to Redis.
     */
    @Test
    void shouldProcessEventDurablyBeforeCacheWrite() {
        LeaderboardProperties properties = new LeaderboardProperties();
        FakeSnapshotStore snapshotStore = new FakeSnapshotStore();
        FakeCache cache = new FakeCache();
        DefaultLeaderboardService service = service(properties, cache, snapshotStore);

        StepVerifier.create(service.process(ScoreEvent.delta("global", "p1", "Alice", 3.0)))
                .assertNext(processed -> assertEquals(3.0, processed.newScore()))
                .verifyComplete();

        assertEquals(1, snapshotStore.applyCount.get());
        assertEquals(1, cache.putProcessedCount.get());
    }

    /**
     * Verifies invalid limits are rejected immediately.
     */
    @Test
    void shouldRejectInvalidLimit() {
        DefaultLeaderboardService service = service(new LeaderboardProperties(), new FakeCache(), new FakeSnapshotStore());

        assertThrows(IllegalArgumentException.class, () -> service.topK("global", 0).block());
    }

    /**
     * Verifies ambiguous mutation payloads are rejected.
     */
    @Test
    void shouldRejectEventWithDeltaAndAbsoluteScore() {
        DefaultLeaderboardService service = service(new LeaderboardProperties(), new FakeCache(), new FakeSnapshotStore());
        ScoreEvent invalid = new ScoreEvent("event-1", "global", "p1", "Alice", 1.0, 2.0, Instant.now());

        assertThrows(IllegalArgumentException.class, () -> service.process(invalid).block());
    }

    /**
     * Verifies Redis cache misses are repaired from the durable snapshot table.
     */
    @Test
    void shouldFallbackToPostgresWhenRedisIsEmpty() {
        LeaderboardProperties properties = new LeaderboardProperties();
        FakeCache cache = new FakeCache();
        cache.entries.clear();
        FakeSnapshotStore snapshotStore = new FakeSnapshotStore();
        snapshotStore.snapshotEntries = List.of(new LeaderboardEntry(1, "p9", "Carol", 99));
        DefaultLeaderboardService service = service(properties, cache, snapshotStore);

        StepVerifier.create(service.topK("global", 100))
                .assertNext(response -> {
                    assertEquals(1, response.items().size());
                    assertEquals("p9", response.items().getFirst().itemId());
                })
                .verifyComplete();

        assertEquals(1, snapshotStore.topSnapshotCount.get());
        assertEquals(1, cache.putSnapshotCount.get());
    }

    private DefaultLeaderboardService service(
            LeaderboardProperties properties,
            LeaderboardCache cache,
            ScoreSnapshotStore snapshotStore
    ) {
        return new DefaultLeaderboardService(
                properties,
                new RedisKeyFactory(properties),
                cache,
                snapshotStore,
                new SimpleMeterRegistry()
        );
    }

    private static final class FakeCache implements LeaderboardCache {
        private final AtomicInteger putProcessedCount = new AtomicInteger();
        private final AtomicInteger putSnapshotCount = new AtomicInteger();
        private final List<LeaderboardEntry> entries = new ArrayList<>();

        private FakeCache() {
            entries.add(new LeaderboardEntry(1, "p1", "Alice", 10));
            entries.add(new LeaderboardEntry(2, "p2", "Bob", 9));
        }

        @Override
        public Mono<Void> putProcessedEvent(ProcessedScoreEvent processed) {
            putProcessedCount.incrementAndGet();
            return Mono.empty();
        }

        @Override
        public Mono<List<LeaderboardEntry>> topK(String leaderboardId, int limit) {
            return Mono.just(entries.subList(0, Math.min(limit, entries.size())));
        }

        @Override
        public Mono<Void> putSnapshot(String leaderboardId, String itemId, String displayName, double score) {
            putSnapshotCount.incrementAndGet();
            return Mono.empty();
        }
    }

    private static final class FakeSnapshotStore implements ScoreSnapshotStore {
        private final AtomicInteger applyCount = new AtomicInteger();
        private final AtomicInteger topSnapshotCount = new AtomicInteger();
        private List<LeaderboardEntry> snapshotEntries = List.of();

        @Override
        public Mono<ProcessedScoreEvent> apply(ScoreEvent event) {
            applyCount.incrementAndGet();
            return Mono.just(new ProcessedScoreEvent(
                    event,
                    event.absoluteScore() == null ? event.effectiveDelta() : event.absoluteScore(),
                    event.displayName(),
                    false,
                    Instant.now()
            ));
        }

        @Override
        public Mono<List<LeaderboardEntry>> topSnapshots(String leaderboardId, int limit) {
            topSnapshotCount.incrementAndGet();
            return Mono.just(snapshotEntries.subList(0, Math.min(limit, snapshotEntries.size())));
        }

        @Override
        public Flux<String> leaderboardIds(int limit) {
            return Flux.just("global");
        }
    }
}
