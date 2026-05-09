package com.github.dimitryivaniuta.gateway.leaderboard.adapter.postgres;

import com.github.dimitryivaniuta.gateway.leaderboard.domain.LeaderboardEntry;
import com.github.dimitryivaniuta.gateway.leaderboard.domain.ProcessedScoreEvent;
import com.github.dimitryivaniuta.gateway.leaderboard.domain.ScoreEvent;
import com.github.dimitryivaniuta.gateway.leaderboard.port.ScoreSnapshotStore;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.r2dbc.core.DatabaseClient.GenericExecuteSpec;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * PostgreSQL implementation of durable leaderboard score snapshots and event idempotency.
 *
 * <p>This adapter is the source of truth. Every Kafka event is first recorded in
 * {@code leaderboard_processed_events}. Only a newly inserted event mutates
 * {@code leaderboard_scores}; duplicate deliveries read the already stored final score and allow the
 * caller to repair Redis without applying the score twice.</p>
 */
@Repository
public class PostgresScoreSnapshotStore implements ScoreSnapshotStore {

    private static final String INSERT_EVENT_SQL = """
            INSERT INTO leaderboard_processed_events (
                event_id, leaderboard_id, item_id, display_name, delta, absolute_score,
                occurred_at, processed_at
            ) VALUES (
                :eventId, :leaderboardId, :itemId, :displayName, :delta, :absoluteScore,
                :occurredAt, :processedAt
            )
            ON CONFLICT (event_id) DO NOTHING
            """;

    private static final String UPSERT_DELTA_SCORE_SQL = """
            INSERT INTO leaderboard_scores (leaderboard_id, item_id, display_name, score, version, updated_at)
            VALUES (:leaderboardId, :itemId, :displayName, :delta, 1, :updatedAt)
            ON CONFLICT (leaderboard_id, item_id)
            DO UPDATE SET
                display_name = COALESCE(EXCLUDED.display_name, leaderboard_scores.display_name),
                score = leaderboard_scores.score + EXCLUDED.score,
                version = leaderboard_scores.version + 1,
                updated_at = EXCLUDED.updated_at
            RETURNING display_name, score
            """;

    private static final String UPSERT_ABSOLUTE_SCORE_SQL = """
            INSERT INTO leaderboard_scores (leaderboard_id, item_id, display_name, score, version, updated_at)
            VALUES (:leaderboardId, :itemId, :displayName, :score, 1, :updatedAt)
            ON CONFLICT (leaderboard_id, item_id)
            DO UPDATE SET
                display_name = COALESCE(EXCLUDED.display_name, leaderboard_scores.display_name),
                score = EXCLUDED.score,
                version = leaderboard_scores.version + 1,
                updated_at = EXCLUDED.updated_at
            RETURNING display_name, score
            """;

    private static final String MARK_EVENT_APPLIED_SQL = """
            UPDATE leaderboard_processed_events
            SET final_display_name = :displayName,
                new_score = :newScore
            WHERE event_id = :eventId
            """;

    private static final String READ_PROCESSED_EVENT_SQL = """
            SELECT event_id, leaderboard_id, item_id, display_name, final_display_name,
                   delta, absolute_score, new_score, occurred_at, processed_at
            FROM leaderboard_processed_events
            WHERE event_id = :eventId
            """;

    private static final String INCREMENT_DUPLICATE_COUNT_SQL = """
            UPDATE leaderboard_processed_events
            SET duplicate_count = duplicate_count + 1
            WHERE event_id = :eventId
            """;

    private final DatabaseClient databaseClient;
    private final TransactionalOperator transactionalOperator;

    /**
     * Creates PostgreSQL snapshot store.
     *
     * @param databaseClient reactive database client
     * @param transactionalOperator transactional operator
     */
    public PostgresScoreSnapshotStore(
            DatabaseClient databaseClient,
            TransactionalOperator transactionalOperator
    ) {
        this.databaseClient = databaseClient;
        this.transactionalOperator = transactionalOperator;
    }

    /**
     * Applies the score event exactly once and returns the final score.
     *
     * @param event normalized score event
     * @return processed event with durable score state
     */
    @Override
    public Mono<ProcessedScoreEvent> apply(ScoreEvent event) {
        Instant processedAt = Instant.now();
        OffsetDateTime processedAtOffset = processedAt.atOffset(ZoneOffset.UTC);
        return insertEvent(event, processedAtOffset)
                .flatMap(inserted -> inserted
                        ? applyNewEvent(event, processedAt, processedAtOffset)
                        : readDuplicateEvent(event.eventId()))
                .as(transactionalOperator::transactional);
    }

    /**
     * Reads top snapshot rows, ordered consistently with Redis reverse sorted-set reads.
     *
     * @param leaderboardId leaderboard id
     * @param limit maximum number of rows
     * @return snapshot entries
     */
    @Override
    public Mono<List<LeaderboardEntry>> topSnapshots(String leaderboardId, int limit) {
        return databaseClient.sql("""
                        SELECT item_id, display_name, score
                        FROM leaderboard_scores
                        WHERE leaderboard_id = :leaderboardId
                        ORDER BY score DESC, item_id DESC
                        LIMIT :limit
                        """)
                .bind("leaderboardId", leaderboardId)
                .bind("limit", limit)
                .map((row, metadata) -> new SnapshotRow(
                        row.get("item_id", String.class),
                        row.get("display_name", String.class),
                        row.get("score", Double.class)
                ))
                .all()
                .index()
                .map(indexed -> new LeaderboardEntry(
                        indexed.getT1() + 1L,
                        indexed.getT2().itemId(),
                        indexed.getT2().displayName(),
                        indexed.getT2().score() == null ? 0.0 : indexed.getT2().score()
                ))
                .collectList();
    }

    /**
     * Streams known leaderboard ids ordered by recent updates.
     *
     * @param limit maximum number of leaderboard ids to return
     * @return known leaderboard ids
     */
    @Override
    public Flux<String> leaderboardIds(int limit) {
        return databaseClient.sql("""
                        SELECT leaderboard_id
                        FROM leaderboard_scores
                        GROUP BY leaderboard_id
                        ORDER BY MAX(updated_at) DESC
                        LIMIT :limit
                        """)
                .bind("limit", limit)
                .map((row, metadata) -> row.get("leaderboard_id", String.class))
                .all();
    }

    private Mono<Boolean> insertEvent(ScoreEvent event, OffsetDateTime processedAt) {
        GenericExecuteSpec spec = databaseClient.sql(INSERT_EVENT_SQL)
                .bind("eventId", event.eventId())
                .bind("leaderboardId", event.leaderboardId())
                .bind("itemId", event.itemId())
                .bind("occurredAt", event.occurredAt().atOffset(ZoneOffset.UTC))
                .bind("processedAt", processedAt);
        spec = bindNullable(spec, "displayName", event.displayName(), String.class);
        spec = bindNullable(spec, "delta", event.delta(), Double.class);
        spec = bindNullable(spec, "absoluteScore", event.absoluteScore(), Double.class);
        return spec.fetch().rowsUpdated().map(rows -> rows > 0);
    }

    private Mono<ProcessedScoreEvent> applyNewEvent(
            ScoreEvent event,
            Instant processedAt,
            OffsetDateTime processedAtOffset
    ) {
        Mono<ScoreState> mutation = event.hasAbsoluteScore()
                ? upsertAbsoluteScore(event, processedAtOffset)
                : upsertDeltaScore(event, processedAtOffset);
        return mutation.flatMap(state -> markEventApplied(event.eventId(), state)
                .thenReturn(new ProcessedScoreEvent(
                        event,
                        state.score(),
                        state.displayName(),
                        false,
                        processedAt
                )));
    }

    private Mono<ScoreState> upsertDeltaScore(ScoreEvent event, OffsetDateTime updatedAt) {
        GenericExecuteSpec spec = databaseClient.sql(UPSERT_DELTA_SCORE_SQL)
                .bind("leaderboardId", event.leaderboardId())
                .bind("itemId", event.itemId())
                .bind("delta", event.effectiveDelta())
                .bind("updatedAt", updatedAt);
        spec = bindNullable(spec, "displayName", event.displayName(), String.class);
        return mapScoreState(spec);
    }

    private Mono<ScoreState> upsertAbsoluteScore(ScoreEvent event, OffsetDateTime updatedAt) {
        GenericExecuteSpec spec = databaseClient.sql(UPSERT_ABSOLUTE_SCORE_SQL)
                .bind("leaderboardId", event.leaderboardId())
                .bind("itemId", event.itemId())
                .bind("score", event.absoluteScore())
                .bind("updatedAt", updatedAt);
        spec = bindNullable(spec, "displayName", event.displayName(), String.class);
        return mapScoreState(spec);
    }

    private Mono<ScoreState> mapScoreState(GenericExecuteSpec spec) {
        return spec.map((row, metadata) -> new ScoreState(
                        row.get("display_name", String.class),
                        row.get("score", Double.class) == null ? 0.0 : row.get("score", Double.class)
                ))
                .one();
    }

    private Mono<Void> markEventApplied(String eventId, ScoreState state) {
        GenericExecuteSpec spec = databaseClient.sql(MARK_EVENT_APPLIED_SQL)
                .bind("eventId", eventId)
                .bind("newScore", state.score());
        spec = bindNullable(spec, "displayName", state.displayName(), String.class);
        return spec.fetch().rowsUpdated().then();
    }

    private Mono<ProcessedScoreEvent> readDuplicateEvent(String eventId) {
        return databaseClient.sql(INCREMENT_DUPLICATE_COUNT_SQL)
                .bind("eventId", eventId)
                .fetch()
                .rowsUpdated()
                .then(databaseClient.sql(READ_PROCESSED_EVENT_SQL)
                        .bind("eventId", eventId)
                        .map((row, metadata) -> {
                            String finalDisplayName = row.get("final_display_name", String.class);
                            String originalDisplayName = row.get("display_name", String.class);
                            OffsetDateTime occurredAt = row.get("occurred_at", OffsetDateTime.class);
                            OffsetDateTime processedAt = row.get("processed_at", OffsetDateTime.class);
                            ScoreEvent storedEvent = new ScoreEvent(
                                    row.get("event_id", String.class),
                                    row.get("leaderboard_id", String.class),
                                    row.get("item_id", String.class),
                                    originalDisplayName,
                                    row.get("delta", Double.class),
                                    row.get("absolute_score", Double.class),
                                    occurredAt == null ? Instant.now() : occurredAt.toInstant()
                            );
                            Double newScore = row.get("new_score", Double.class);
                            if (newScore == null) {
                                throw new IllegalStateException("Processed event has no final score: " + eventId);
                            }
                            return new ProcessedScoreEvent(
                                    storedEvent,
                                    newScore,
                                    finalDisplayName == null ? originalDisplayName : finalDisplayName,
                                    true,
                                    processedAt == null ? Instant.now() : processedAt.toInstant()
                            );
                        })
                        .one()
                        .switchIfEmpty(Mono.error(new IllegalStateException("Processed event not found: " + eventId))));
    }

    private <T> GenericExecuteSpec bindNullable(
            GenericExecuteSpec spec,
            String name,
            T value,
            Class<T> type
    ) {
        return value == null ? spec.bindNull(name, type) : spec.bind(name, value);
    }

    private record ScoreState(String displayName, double score) {
    }

    private record SnapshotRow(String itemId, String displayName, Double score) {
    }
}
