package com.github.dimitryivaniuta.gateway.leaderboard.adapter.kafka;

import com.github.dimitryivaniuta.gateway.leaderboard.config.LeaderboardProperties;
import com.github.dimitryivaniuta.gateway.leaderboard.domain.ScoreEvent;
import com.github.dimitryivaniuta.gateway.leaderboard.web.PublishScoreEventResponse;
import java.util.concurrent.CompletableFuture;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/**
 * Kafka producer used by the local HTTP smoke-test ingestion endpoint.
 */
@Component
public class ScoreEventPublisher {

    private final KafkaTemplate<String, ScoreEvent> kafkaTemplate;
    private final LeaderboardProperties properties;

    /**
     * Creates score event publisher.
     *
     * @param kafkaTemplate Kafka template
     * @param properties application settings
     */
    public ScoreEventPublisher(KafkaTemplate<String, ScoreEvent> kafkaTemplate, LeaderboardProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
    }

    /**
     * Publishes a score event to Kafka using item id as the partition key.
     *
     * @param event score event to publish
     * @return publication metadata
     */
    public Mono<PublishScoreEventResponse> publish(ScoreEvent event) {
        CompletableFuture<SendResult<String, ScoreEvent>> future = kafkaTemplate.send(
                properties.getKafkaTopic(),
                event.leaderboardId() + ":" + event.itemId(),
                event
        );
        return Mono.fromFuture(future)
                .map(result -> new PublishScoreEventResponse(
                        event.eventId(),
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset()
                ));
    }
}
