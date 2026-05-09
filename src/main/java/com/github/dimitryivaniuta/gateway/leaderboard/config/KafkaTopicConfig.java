package com.github.dimitryivaniuta.gateway.leaderboard.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Kafka topic declaration used by local development and infrastructure-as-code aware deployments.
 */
@Configuration
public class KafkaTopicConfig {

    /**
     * Declares the score-events topic with enough partitions for horizontal consumer scaling.
     *
     * @param properties application leaderboard settings
     * @return Kafka topic definition
     */
    @Bean
    public NewTopic scoreEventsTopic(LeaderboardProperties properties) {
        return TopicBuilder.name(properties.getKafkaTopic())
                .partitions(12)
                .replicas(1)
                .config("cleanup.policy", "delete")
                .config("retention.ms", String.valueOf(7L * 24 * 60 * 60 * 1000))
                .build();
    }

    /**
     * Declares the dead-letter topic for poison records that cannot be processed safely.
     *
     * @param properties application leaderboard settings
     * @return Kafka dead-letter topic definition
     */
    @Bean
    public NewTopic scoreEventsDeadLetterTopic(LeaderboardProperties properties) {
        return TopicBuilder.name(properties.getKafkaTopic() + properties.getKafkaDeadLetterSuffix())
                .partitions(12)
                .replicas(1)
                .config("cleanup.policy", "delete")
                .config("retention.ms", String.valueOf(14L * 24 * 60 * 60 * 1000))
                .build();
    }
}
