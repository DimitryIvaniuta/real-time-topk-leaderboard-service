package com.github.dimitryivaniuta.gateway.leaderboard.web;

/**
 * HTTP response returned after a score event is accepted for Kafka publication.
 *
 * @param eventId accepted event id
 * @param topic Kafka topic where the event was sent
 * @param partition Kafka partition used by the producer
 * @param offset Kafka offset assigned by the broker
 */
public record PublishScoreEventResponse(String eventId, String topic, int partition, long offset) {
}
