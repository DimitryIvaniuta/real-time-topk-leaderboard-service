# Real-Time Top-K Leaderboard Service

Production-grade Java 21 service for near real-time Top-K player/item leaderboards.

## Repository

**Repository name:** `real-time-topk-leaderboard-service`

**Description:** Real-time Top-K leaderboard microservice using Spring Boot 4, WebFlux, Kafka KRaft input events, Redis Sorted Sets for sub-50ms reads, PostgreSQL/R2DBC durable snapshots and idempotency, Flyway, Docker Compose, tests, metrics, and Postman collection.

## Architecture

```text
Kafka score events
       |
       v
Spring Kafka consumer, manual offset ack
       |
       v
PostgreSQL transaction via R2DBC
  - insert event id into leaderboard_processed_events
  - apply delta / absolute score exactly once
  - store final score for duplicate replay repair
       |
       v
Redis derived read model
  - ZADD final score into sorted set
  - HSET final display name
       |
       v
GET /leaderboard?limit=100 reads Redis Sorted Set
       |
       v
PostgreSQL fallback only when Redis is empty/unavailable
```

## Production choices

- **Java 21**: current LTS runtime.
- **Spring Boot 4.0.6**: current stable Spring Boot 4 line at project creation time.
- **WebFlux + R2DBC**: non-blocking API and PostgreSQL access.
- **Redis Sorted Set**: `ZREVRANGE WITHSCORES` style Top-K lookup; suitable for p95 < 50ms with Redis near the service.
- **PostgreSQL-first idempotency**: durable event table prevents Redis-first data loss during Kafka retries.
- **Redis as derived read model**: cache writes use absolute final score returned from PostgreSQL.
- **Kafka KRaft**: no ZooKeeper dependency.
- **Kafka DLT**: poison messages are retried with exponential backoff and then routed to `leaderboard.score-events.DLT`.
- **Flyway**: controlled schema migrations for score snapshots and processed events.
- **Micrometer/Prometheus**: latency, update, duplicate, and fallback counters.
- **Deterministic ordering**: score descending, then item id descending for PostgreSQL fallback to match Redis reverse range behavior for equal scores.

## API

### Get Top-K leaderboard

```http
GET /leaderboard?leaderboardId=global&limit=100
```

Response:

```json
{
  "leaderboardId": "global",
  "limit": 100,
  "generatedAt": "2026-05-09T12:00:00Z",
  "items": [
    {
      "rank": 1,
      "itemId": "player-1",
      "displayName": "Alice",
      "score": 125.0
    }
  ]
}
```

### Publish local test score event through Kafka

```http
POST /leaderboard/events
Content-Type: application/json

{
  "leaderboardId": "global",
  "itemId": "player-1",
  "displayName": "Alice",
  "delta": 25.5
}
```

This endpoint is intended for local smoke tests and Postman. It publishes to Kafka; the Kafka consumer updates PostgreSQL first and then Redis.

## Event contract

Kafka topic: `leaderboard.score-events`

```json
{
  "eventId": "4c44f59d-062e-4583-b77f-cac53d90fd05",
  "leaderboardId": "global",
  "itemId": "player-1",
  "displayName": "Alice",
  "delta": 10.0,
  "absoluteScore": null,
  "occurredAt": "2026-05-09T12:00:00Z"
}
```

Rules:

- `eventId` is required for durable idempotency.
- Use `delta` to increment/decrement score.
- Use `absoluteScore` to replace score exactly.
- Exactly one of `delta` or `absoluteScore` must be provided.
- Re-delivered events repair Redis with the stored final PostgreSQL score and do not mutate the score again.
- Equal scores are ordered deterministically by Redis sorted-set reverse range behavior; PostgreSQL fallback uses `ORDER BY score DESC, item_id DESC`.

## Run locally

```bash
docker compose up -d postgres redis kafka kafka-ui
gradle bootRun
```

Or run the full stack including the app container:

```bash
docker compose --profile app up --build
```

The generated archive also contains `gradlew`/`gradlew.bat` helper scripts that use an installed Gradle when the wrapper jar is not present.

Then open:

- API: `http://localhost:8080/leaderboard?limit=100`
- Actuator health: `http://localhost:8080/actuator/health`
- Prometheus metrics: `http://localhost:8080/actuator/prometheus`
- Kafka UI: `http://localhost:8090`

## Test

```bash
gradle test
```

Optional Docker-backed integration tests can be enabled in CI by adding integration test classes under a dedicated source set or by setting `RUN_TESTCONTAINERS=true` for classes that require Docker.

## Performance target

For `GET /leaderboard?limit=100`, the hot path is one Redis sorted-set read and one Redis hash multi-get for names. With Redis in the same region/VPC and connection pooling enabled, the service is designed for **p95 < 50ms** for Top-100 reads.

Recommended production settings:

- Co-locate application and Redis in the same region/AZ when possible.
- Keep `limit` capped, default max is 500.
- Scale API instances horizontally; Redis is the shared read model.
- Partition Kafka events by `leaderboardId:itemId` for per-item order and better partition spread.
- Keep Kafka consumer concurrency aligned with topic partitions.
- Monitor `leaderboard.query.latency`, `leaderboard.events.processed`, `leaderboard.events.duplicates`, `leaderboard.query.postgres.fallbacks`, and Redis CPU/network saturation.

## Postman

Import: `postman/Real-Time-TopK-Leaderboard.postman_collection.json`

## Important files

- `src/main/java/.../LeaderboardController.java` — REST API.
- `src/main/java/.../ScoreEventConsumer.java` — Kafka consumer.
- `src/main/java/.../RedisLeaderboardCache.java` — Redis sorted-set read model.
- `src/main/java/.../PostgresScoreSnapshotStore.java` — durable score mutation and idempotency.
- `src/main/resources/db/migration/V1__create_leaderboard_scores.sql` — score snapshot schema.
- `src/main/resources/db/migration/V2__create_processed_score_events.sql` — durable event-idempotency schema.
- `docker-compose.yml` — PostgreSQL, Redis, Kafka KRaft, Kafka UI, and optional app service.
- `Dockerfile` — production-style container build.

## Smoke test

```bash
./scripts/smoke-test.sh
```

## k6 p95 read benchmark

```bash
k6 run performance/k6-topk-read.js
```
