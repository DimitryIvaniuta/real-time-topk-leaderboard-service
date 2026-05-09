package com.github.dimitryivaniuta.gateway.leaderboard.startup;

import com.github.dimitryivaniuta.gateway.leaderboard.config.LeaderboardProperties;
import com.github.dimitryivaniuta.gateway.leaderboard.port.LeaderboardCache;
import com.github.dimitryivaniuta.gateway.leaderboard.port.ScoreSnapshotStore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

/**
 * Warms Redis from PostgreSQL snapshots after application startup.
 */
@Slf4j
@Component
public class RedisWarmupRunner implements ApplicationRunner {

    private final LeaderboardProperties properties;
    private final ScoreSnapshotStore snapshotStore;
    private final LeaderboardCache cache;

    /**
     * Creates cache warmup runner.
     *
     * @param properties application settings
     * @param snapshotStore durable snapshot store
     * @param cache hot cache
     */
    public RedisWarmupRunner(
            LeaderboardProperties properties,
            ScoreSnapshotStore snapshotStore,
            LeaderboardCache cache
    ) {
        this.properties = properties;
        this.snapshotStore = snapshotStore;
        this.cache = cache;
    }

    /**
     * Loads the highest snapshot rows into Redis. The runner is best-effort and does not block startup.
     *
     * @param args application arguments
     */
    @Override
    public void run(ApplicationArguments args) {
        int perLeaderboardLimit = properties.getCacheWarmupLimit();
        if (perLeaderboardLimit <= 0) {
            log.info("Redis warmup disabled");
            return;
        }
        snapshotStore.leaderboardIds(properties.getCacheWarmupLeaderboardLimit())
                .switchIfEmpty(Flux.just(properties.getDefaultLeaderboardId()))
                .flatMap(leaderboardId -> snapshotStore.topSnapshots(leaderboardId, perLeaderboardLimit)
                        .flatMapMany(Flux::fromIterable)
                        .flatMap(entry -> cache.putSnapshot(
                                leaderboardId,
                                entry.itemId(),
                                entry.displayName(),
                                entry.score()
                        ), 64)
                        .count()
                        .doOnNext(count -> log.info(
                                "Redis leaderboard cache warmed for {} with {} snapshot rows",
                                leaderboardId,
                                count
                        )), 4)
                .reduce(0L, Long::sum)
                .doOnNext(total -> log.info("Redis leaderboard cache warmup completed with {} rows", total))
                .doOnError(error -> log.warn("Redis leaderboard cache warmup failed", error))
                .subscribe();
    }
}
