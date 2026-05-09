package com.github.dimitryivaniuta.gateway.leaderboard.service;

import com.github.dimitryivaniuta.gateway.leaderboard.config.LeaderboardProperties;
import com.github.dimitryivaniuta.gateway.leaderboard.config.RedisKeyFactory;
import com.github.dimitryivaniuta.gateway.leaderboard.domain.LeaderboardEntry;
import com.github.dimitryivaniuta.gateway.leaderboard.domain.LeaderboardResponse;
import com.github.dimitryivaniuta.gateway.leaderboard.domain.ProcessedScoreEvent;
import com.github.dimitryivaniuta.gateway.leaderboard.domain.ScoreEvent;
import com.github.dimitryivaniuta.gateway.leaderboard.port.LeaderboardCache;
import com.github.dimitryivaniuta.gateway.leaderboard.port.ScoreSnapshotStore;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Default production implementation of leaderboard use cases.
 */
@Slf4j
@Service
public class DefaultLeaderboardService implements LeaderboardService {

    private static final int MAX_EVENT_ID_LENGTH = 120;
    private static final int MAX_DISPLAY_NAME_LENGTH = 240;

    private final LeaderboardProperties properties;
    private final RedisKeyFactory keyFactory;
    private final LeaderboardCache cache;
    private final ScoreSnapshotStore snapshotStore;
    private final Timer topKTimer;
    private final Counter processedCounter;
    private final Counter duplicateCounter;
    private final Counter fallbackCounter;

    /**
     * Creates the service.
     *
     * @param properties application settings
     * @param keyFactory Redis key validator/factory
     * @param cache hot leaderboard cache
     * @param snapshotStore durable snapshot store
     * @param meterRegistry metrics registry
     */
    public DefaultLeaderboardService(
            LeaderboardProperties properties,
            RedisKeyFactory keyFactory,
            LeaderboardCache cache,
            ScoreSnapshotStore snapshotStore,
            MeterRegistry meterRegistry
    ) {
        this.properties = properties;
        this.keyFactory = keyFactory;
        this.cache = cache;
        this.snapshotStore = snapshotStore;
        this.topKTimer = Timer.builder("leaderboard.query.latency")
                .description("Latency of Top-K leaderboard queries")
                .publishPercentileHistogram()
                .register(meterRegistry);
        this.processedCounter = Counter.builder("leaderboard.events.processed")
                .description("Number of non-duplicate score events processed")
                .register(meterRegistry);
        this.duplicateCounter = Counter.builder("leaderboard.events.duplicates")
                .description("Number of duplicate score events repaired or skipped")
                .register(meterRegistry);
        this.fallbackCounter = Counter.builder("leaderboard.query.postgres.fallbacks")
                .description("Number of Top-K queries served from PostgreSQL fallback")
                .register(meterRegistry);
    }

    /**
     * Processes a score event by applying PostgreSQL durable idempotency first and then updating Redis.
     *
     * <p>Kafka offsets must be acknowledged only after this method completes. If Redis fails after
     * PostgreSQL commits, a retry becomes a duplicate database event and rewrites Redis with the
     * stored final score instead of incrementing the score again.</p>
     *
     * @param event incoming score event
     * @return processed event with final score
     */
    @Override
    public Mono<ProcessedScoreEvent> process(ScoreEvent event) {
        ScoreEvent normalized = normalize(event);
        return snapshotStore.apply(normalized)
                .flatMap(processed -> cache.putProcessedEvent(processed).thenReturn(processed))
                .doOnNext(processed -> {
                    if (processed.duplicate()) {
                        duplicateCounter.increment();
                    } else {
                        processedCounter.increment();
                    }
                })
                .doOnError(error -> log.warn("Failed to process leaderboard event {}", normalized.eventId(), error));
    }

    /**
     * Reads Top-K entries from Redis and falls back to PostgreSQL only on cache miss or cache error.
     *
     * @param leaderboardId requested leaderboard id
     * @param limit requested limit
     * @return leaderboard response
     */
    @Override
    public Mono<LeaderboardResponse> topK(String leaderboardId, Integer limit) {
        String normalizedLeaderboardId = normalizeLeaderboardId(leaderboardId);
        int normalizedLimit = normalizeLimit(limit);
        return Mono.defer(() -> readTopKWithFallback(normalizedLeaderboardId, normalizedLimit)
                        .map(items -> new LeaderboardResponse(
                                normalizedLeaderboardId,
                                normalizedLimit,
                                Instant.now(),
                                items
                        )))
                .transformDeferred(mono -> Mono.fromCallable(topKTimer::start)
                        .flatMap(sample -> mono.doFinally(signalType -> sample.stop(topKTimer))));
    }

    private Mono<List<LeaderboardEntry>> readTopKWithFallback(String leaderboardId, int limit) {
        return cache.topK(leaderboardId, limit)
                .flatMap(items -> items.isEmpty()
                        ? fallbackToPostgresAndRepairCache(leaderboardId, limit)
                        : Mono.just(items))
                .onErrorResume(error -> {
                    log.warn("Redis Top-K read failed for leaderboard {}; using PostgreSQL fallback", leaderboardId, error);
                    return fallbackToPostgresAndRepairCache(leaderboardId, limit);
                });
    }

    private Mono<List<LeaderboardEntry>> fallbackToPostgresAndRepairCache(String leaderboardId, int limit) {
        fallbackCounter.increment();
        return snapshotStore.topSnapshots(leaderboardId, limit)
                .flatMap(entries -> Flux.fromIterable(entries)
                        .flatMap(entry -> cache.putSnapshot(
                                leaderboardId,
                                entry.itemId(),
                                entry.displayName(),
                                entry.score()
                        ).onErrorResume(error -> {
                            log.warn("Redis cache repair failed for item {}", entry.itemId(), error);
                            return Mono.empty();
                        }), 64)
                        .then(Mono.just(entries)));
    }

    private ScoreEvent normalize(ScoreEvent event) {
        Objects.requireNonNull(event, "event must not be null");
        String eventId = normalizeEventId(event.eventId());
        String leaderboardId = normalizeLeaderboardId(event.leaderboardId());
        String itemId = keyFactory.requireSafeItemId(event.itemId());
        String displayName = normalizeDisplayName(event.displayName());
        boolean hasDelta = event.delta() != null;
        boolean hasAbsolute = event.hasAbsoluteScore();
        if (hasDelta == hasAbsolute) {
            throw new IllegalArgumentException("Exactly one of delta or absoluteScore must be provided");
        }
        if (hasAbsolute && !Double.isFinite(event.absoluteScore())) {
            throw new IllegalArgumentException("absoluteScore must be finite");
        }
        if (hasDelta && !Double.isFinite(event.delta())) {
            throw new IllegalArgumentException("delta must be finite");
        }
        return new ScoreEvent(
                eventId,
                leaderboardId,
                itemId,
                displayName,
                event.delta(),
                event.absoluteScore(),
                event.occurredAt() == null ? Instant.now() : event.occurredAt()
        );
    }

    private String normalizeEventId(String eventId) {
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("eventId must not be blank");
        }
        String trimmed = eventId.trim();
        if (trimmed.length() > MAX_EVENT_ID_LENGTH) {
            throw new IllegalArgumentException("eventId must be at most 120 characters");
        }
        return trimmed;
    }

    private String normalizeDisplayName(String displayName) {
        if (displayName == null || displayName.isBlank()) {
            return null;
        }
        String trimmed = displayName.trim();
        if (trimmed.length() > MAX_DISPLAY_NAME_LENGTH) {
            throw new IllegalArgumentException("displayName must be at most 240 characters");
        }
        return trimmed;
    }

    private String normalizeLeaderboardId(String leaderboardId) {
        String candidate = leaderboardId == null || leaderboardId.isBlank()
                ? properties.getDefaultLeaderboardId()
                : leaderboardId;
        return keyFactory.requireSafeLeaderboardId(candidate);
    }

    private int normalizeLimit(Integer limit) {
        int requested = limit == null ? 100 : limit;
        if (requested < 1) {
            throw new IllegalArgumentException("limit must be greater than zero");
        }
        return Math.min(requested, properties.getMaxLimit());
    }
}
