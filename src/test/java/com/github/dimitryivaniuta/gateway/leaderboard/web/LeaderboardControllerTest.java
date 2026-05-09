package com.github.dimitryivaniuta.gateway.leaderboard.web;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.dimitryivaniuta.gateway.leaderboard.adapter.kafka.ScoreEventPublisher;
import com.github.dimitryivaniuta.gateway.leaderboard.config.LeaderboardProperties;
import com.github.dimitryivaniuta.gateway.leaderboard.domain.LeaderboardResponse;
import com.github.dimitryivaniuta.gateway.leaderboard.service.LeaderboardService;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * Unit tests for the REST controller without starting Spring.
 */
@ExtendWith(MockitoExtension.class)
class LeaderboardControllerTest {

    @Mock
    private LeaderboardService leaderboardService;

    @Mock
    private ScoreEventPublisher eventPublisher;

    /**
     * Verifies GET /leaderboard delegates to the application service.
     */
    @Test
    void shouldDelegateTopKRead() {
        LeaderboardProperties properties = new LeaderboardProperties();
        LeaderboardController controller = new LeaderboardController(leaderboardService, eventPublisher, properties);
        LeaderboardResponse response = new LeaderboardResponse("global", 100, Instant.now(), List.of());
        when(leaderboardService.topK("global", 100)).thenReturn(Mono.just(response));

        StepVerifier.create(controller.leaderboard("global", 100))
                .assertNext(actual -> assertEquals(response, actual))
                .verifyComplete();
    }

    /**
     * Verifies POST /leaderboard/events creates and publishes a Kafka event.
     */
    @Test
    void shouldPublishScoreEvent() {
        LeaderboardProperties properties = new LeaderboardProperties();
        LeaderboardController controller = new LeaderboardController(leaderboardService, eventPublisher, properties);
        PublishScoreEventResponse response = new PublishScoreEventResponse("event-1", "topic", 0, 42);
        when(eventPublisher.publish(org.mockito.ArgumentMatchers.any())).thenReturn(Mono.just(response));

        PublishScoreEventRequest request = new PublishScoreEventRequest("event-1", "global", "p1", "Alice", 5.0, null);

        StepVerifier.create(controller.publishScoreEvent(request))
                .assertNext(actual -> assertEquals(response, actual))
                .verifyComplete();

        ArgumentCaptor<com.github.dimitryivaniuta.gateway.leaderboard.domain.ScoreEvent> eventCaptor =
                ArgumentCaptor.forClass(com.github.dimitryivaniuta.gateway.leaderboard.domain.ScoreEvent.class);
        verify(eventPublisher).publish(eventCaptor.capture());
        assertEquals("event-1", eventCaptor.getValue().eventId());
        assertEquals("p1", eventCaptor.getValue().itemId());
    }
}
