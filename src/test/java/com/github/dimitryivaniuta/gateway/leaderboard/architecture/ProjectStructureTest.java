package com.github.dimitryivaniuta.gateway.leaderboard.architecture;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/**
 * Guards key production artifacts that are easy to accidentally remove.
 */
class ProjectStructureTest {

    /**
     * Verifies Docker Compose and Postman artifacts are present.
     */
    @Test
    void shouldContainOperationalArtifacts() {
        assertTrue(Files.exists(Path.of("docker-compose.yml")));
        assertTrue(Files.exists(Path.of("postman/Real-Time-TopK-Leaderboard.postman_collection.json")));
        assertTrue(Files.exists(Path.of("README.md")));
        assertTrue(Files.exists(Path.of("Dockerfile")));
        assertTrue(Files.exists(Path.of("src/main/resources/db/migration/V2__create_processed_score_events.sql")));
    }
}
