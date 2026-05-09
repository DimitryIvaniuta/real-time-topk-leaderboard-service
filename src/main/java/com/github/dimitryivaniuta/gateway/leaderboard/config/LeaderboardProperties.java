package com.github.dimitryivaniuta.gateway.leaderboard.config;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Externalized application settings used by the leaderboard service.
 */
@Getter
@Setter
@Validated
@ConfigurationProperties(prefix = "leaderboard")
public class LeaderboardProperties {

    /** Kafka topic that carries score update events. */
    @NotBlank
    private String kafkaTopic = "leaderboard.score-events";

    /** Default leaderboard used when the API request does not pass a leaderboard id. */
    @NotBlank
    private String defaultLeaderboardId = "global";

    /** Prefix used for all Redis keys owned by this service. */
    @NotBlank
    private String redisKeyPrefix = "rtlb";

    /** Maximum accepted value for the public top-k limit parameter. */
    @Min(1)
    @Max(10_000)
    private int maxLimit = 500;

    /** Number of PostgreSQL rows loaded per leaderboard into Redis during startup warmup. */
    @Min(0)
    private int cacheWarmupLimit = 10_000;

    /** Maximum number of leaderboards loaded into Redis during startup warmup. */
    @Min(1)
    @Max(10_000)
    private int cacheWarmupLeaderboardLimit = 100;

    /** Enables the local HTTP endpoint that publishes score events to Kafka for smoke testing. */
    private boolean httpEventPublishingEnabled = true;

    /** Maximum time the synchronous Kafka listener waits for Redis and PostgreSQL processing. */
    @NotNull
    private Duration kafkaProcessingTimeout = Duration.ofSeconds(5);

    /** Suffix appended to the input topic name for poison-message dead letters. */
    @NotBlank
    private String kafkaDeadLetterSuffix = ".DLT";

    /** Maximum retry attempts before a Kafka record is sent to the dead-letter topic. */
    @Min(0)
    @Max(100)
    private int kafkaMaxRetryAttempts = 5;
}
