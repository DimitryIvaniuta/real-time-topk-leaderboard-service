package com.github.dimitryivaniuta.gateway.leaderboard;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Application entry point for the real-time Top-K leaderboard service.
 */
@EnableScheduling
@SpringBootApplication
@ConfigurationPropertiesScan
public class LeaderboardApplication {

    /**
     * Starts the Spring Boot application.
     *
     * @param args command-line arguments supplied by the runtime
     */
    public static void main(String[] args) {
        SpringApplication.run(LeaderboardApplication.class, args);
    }
}
