package com.github.dimitryivaniuta.gateway.leaderboard.web;

import com.github.dimitryivaniuta.gateway.leaderboard.adapter.kafka.ScoreEventPublisher;
import com.github.dimitryivaniuta.gateway.leaderboard.config.LeaderboardProperties;
import com.github.dimitryivaniuta.gateway.leaderboard.domain.LeaderboardResponse;
import com.github.dimitryivaniuta.gateway.leaderboard.domain.ScoreEvent;
import com.github.dimitryivaniuta.gateway.leaderboard.service.LeaderboardService;
import jakarta.validation.Valid;
import java.time.Instant;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

/**
 * Reactive REST API for leaderboard reads and local event publishing.
 */
@RestController
public class LeaderboardController {

    private final LeaderboardService leaderboardService;
    private final ScoreEventPublisher eventPublisher;
    private final LeaderboardProperties properties;

    /**
     * Creates the leaderboard controller.
     *
     * @param leaderboardService leaderboard service
     * @param eventPublisher Kafka event publisher
     * @param properties application settings
     */
    public LeaderboardController(
            LeaderboardService leaderboardService,
            ScoreEventPublisher eventPublisher,
            LeaderboardProperties properties
    ) {
        this.leaderboardService = leaderboardService;
        this.eventPublisher = eventPublisher;
        this.properties = properties;
    }

    /**
     * Returns Top-K leaderboard entries.
     *
     * @param leaderboardId optional leaderboard id
     * @param limit optional limit, defaults to 100 and is capped by configuration
     * @return leaderboard response
     */
    @GetMapping("/leaderboard")
    public Mono<LeaderboardResponse> leaderboard(
            @RequestParam(required = false) String leaderboardId,
            @RequestParam(required = false) Integer limit
    ) {
        return leaderboardService.topK(leaderboardId, limit);
    }

    /**
     * Publishes a score event into Kafka for local smoke testing.
     *
     * @param request score event request
     * @return Kafka publication metadata
     */
    @PostMapping("/leaderboard/events")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Mono<PublishScoreEventResponse> publishScoreEvent(@Valid @RequestBody PublishScoreEventRequest request) {
        if (!properties.isHttpEventPublishingEnabled()) {
            return Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND, "HTTP score publishing is disabled"));
        }
        if ((request.delta() == null) == (request.absoluteScore() == null)) {
            return Mono.error(new IllegalArgumentException("Exactly one of delta or absoluteScore must be provided"));
        }
        ScoreEvent event = new ScoreEvent(
                request.eventId() == null || request.eventId().isBlank()
                        ? UUID.randomUUID().toString()
                        : request.eventId(),
                request.leaderboardId() == null || request.leaderboardId().isBlank()
                        ? properties.getDefaultLeaderboardId()
                        : request.leaderboardId(),
                request.itemId(),
                request.displayName(),
                request.delta(),
                request.absoluteScore(),
                Instant.now()
        );
        return eventPublisher.publish(event);
    }
}
