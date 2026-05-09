package com.github.dimitryivaniuta.gateway.leaderboard.config;

import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOffWithMaxRetries;

/**
 * Kafka listener retry and dead-letter handling configuration.
 */
@Configuration
public class KafkaConsumerErrorConfig {

    /**
     * Configures bounded retries and a dead-letter topic for poison messages.
     *
     * @param kafkaTemplate Kafka template used by the dead-letter recoverer
     * @param properties application leaderboard settings
     * @return default error handler for Kafka listener containers
     */
    @Bean
    @SuppressWarnings({"rawtypes", "unchecked"})
    public DefaultErrorHandler kafkaErrorHandler(
            KafkaTemplate kafkaTemplate,
            LeaderboardProperties properties
    ) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, exception) -> new TopicPartition(
                        record.topic() + properties.getKafkaDeadLetterSuffix(),
                        record.partition()
                )
        );
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(
                properties.getKafkaMaxRetryAttempts()
        );
        backOff.setInitialInterval(250L);
        backOff.setMultiplier(2.0d);
        backOff.setMaxInterval(5_000L);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
        errorHandler.addNotRetryableExceptions(IllegalArgumentException.class);
        return errorHandler;
    }
}
