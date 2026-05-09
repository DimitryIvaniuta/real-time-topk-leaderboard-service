package com.github.dimitryivaniuta.gateway.leaderboard.adapter.kafka;

import com.github.dimitryivaniuta.gateway.leaderboard.config.LeaderboardProperties;
import com.github.dimitryivaniuta.gateway.leaderboard.domain.ScoreEvent;
import com.github.dimitryivaniuta.gateway.leaderboard.service.LeaderboardService;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer that applies score update events to the leaderboard read model.
 */
@Slf4j
@Component
public class ScoreEventConsumer {

    private final LeaderboardService leaderboardService;
    private final LeaderboardProperties properties;

    /**
     * Creates the Kafka consumer.
     *
     * @param leaderboardService leaderboard service
     * @param properties application settings
     */
    public ScoreEventConsumer(LeaderboardService leaderboardService, LeaderboardProperties properties) {
        this.leaderboardService = leaderboardService;
        this.properties = properties;
    }

    /**
     * Receives one score event, updates Redis and PostgreSQL, then commits Kafka offset manually.
     *
     * @param event incoming Kafka event
     * @param acknowledgment manual Kafka acknowledgment
     */
    @KafkaListener(topics = "${leaderboard.kafka-topic}")
    public void onScoreEvent(ScoreEvent event, Acknowledgment acknowledgment) {
        Duration timeout = properties.getKafkaProcessingTimeout();
        leaderboardService.process(event).block(timeout);
        acknowledgment.acknowledge();
        log.debug("Acknowledged leaderboard event {}", event.eventId());
    }
}
