package com.github.dimitryivaniuta.gateway.leaderboard.adapter.redis;

import com.github.dimitryivaniuta.gateway.leaderboard.config.RedisKeyFactory;
import com.github.dimitryivaniuta.gateway.leaderboard.domain.LeaderboardEntry;
import com.github.dimitryivaniuta.gateway.leaderboard.domain.ProcessedScoreEvent;
import com.github.dimitryivaniuta.gateway.leaderboard.port.LeaderboardCache;
import java.util.ArrayList;
import java.util.List;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.core.ReactiveHashOperations;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.ReactiveZSetOperations;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/**
 * Redis implementation of the hot leaderboard read model.
 *
 * <p>Redis is intentionally used as a derived, rebuildable cache. Durable idempotency and score
 * mutation are handled by PostgreSQL first, then this adapter writes the final score into a sorted
 * set. The design prevents data loss when Kafka retries an event after the database commit but
 * before the cache write.</p>
 */
@Component
public class RedisLeaderboardCache implements LeaderboardCache {

    private final RedisKeyFactory keyFactory;
    private final ReactiveStringRedisTemplate redisTemplate;

    /**
     * Creates Redis leaderboard cache adapter.
     *
     * @param keyFactory Redis key factory
     * @param redisTemplate reactive Redis template
     */
    public RedisLeaderboardCache(
            RedisKeyFactory keyFactory,
            ReactiveStringRedisTemplate redisTemplate
    ) {
        this.keyFactory = keyFactory;
        this.redisTemplate = redisTemplate;
    }

    /**
     * Writes the final processed score into Redis as an absolute value.
     *
     * @param processed durable event result
     * @return completion signal
     */
    @Override
    public Mono<Void> putProcessedEvent(ProcessedScoreEvent processed) {
        return putSnapshot(
                processed.event().leaderboardId(),
                processed.event().itemId(),
                processed.displayName(),
                processed.newScore()
        );
    }

    /**
     * Reads Top-K entries from Redis sorted set and resolves display names from a Redis hash.
     *
     * @param leaderboardId leaderboard id
     * @param limit maximum number of entries
     * @return ranked entries
     */
    @Override
    public Mono<List<LeaderboardEntry>> topK(String leaderboardId, int limit) {
        String normalizedLeaderboardId = keyFactory.requireSafeLeaderboardId(leaderboardId);
        String scoresKey = keyFactory.scoresKey(normalizedLeaderboardId);
        String namesKey = keyFactory.namesKey(normalizedLeaderboardId);
        ReactiveZSetOperations<String, String> zset = redisTemplate.opsForZSet();
        ReactiveHashOperations<String, Object, Object> hash = redisTemplate.opsForHash();

        return zset.reverseRangeWithScores(scoresKey, Range.closed(0L, (long) limit - 1L))
                .collectList()
                .flatMap(tuples -> {
                    List<String> itemIds = tuples.stream()
                            .map(ZSetOperations.TypedTuple::getValue)
                            .toList();
                    if (itemIds.isEmpty()) {
                        return Mono.just(List.of());
                    }
                    List<Object> hashKeys = new ArrayList<>(itemIds);
                    return hash.multiGet(namesKey, hashKeys)
                            .map(names -> toEntries(tuples, names));
                });
    }

    /**
     * Restores a snapshot row into Redis during warmup or fallback repair.
     *
     * @param leaderboardId leaderboard id
     * @param itemId item id
     * @param displayName optional display name
     * @param score score
     * @return completion signal
     */
    @Override
    public Mono<Void> putSnapshot(String leaderboardId, String itemId, String displayName, double score) {
        String normalizedLeaderboardId = keyFactory.requireSafeLeaderboardId(leaderboardId);
        String normalizedItemId = keyFactory.requireSafeItemId(itemId);
        Mono<Boolean> scoreWrite = redisTemplate.opsForZSet()
                .add(keyFactory.scoresKey(normalizedLeaderboardId), normalizedItemId, score);
        Mono<Boolean> nameWrite = displayName == null || displayName.isBlank()
                ? Mono.just(Boolean.TRUE)
                : redisTemplate.opsForHash().put(keyFactory.namesKey(normalizedLeaderboardId), normalizedItemId, displayName);
        return scoreWrite.then(nameWrite).then();
    }

    private List<LeaderboardEntry> toEntries(
            List<ZSetOperations.TypedTuple<String>> tuples,
            List<Object> displayNames
    ) {
        List<LeaderboardEntry> entries = new ArrayList<>(tuples.size());
        for (int i = 0; i < tuples.size(); i++) {
            ZSetOperations.TypedTuple<String> tuple = tuples.get(i);
            String itemId = tuple.getValue();
            Object rawDisplayName = i < displayNames.size() ? displayNames.get(i) : null;
            String displayName = rawDisplayName == null ? null : rawDisplayName.toString();
            double score = tuple.getScore() == null ? 0.0 : tuple.getScore();
            entries.add(new LeaderboardEntry(i + 1L, itemId, displayName, score));
        }
        return entries;
    }
}
