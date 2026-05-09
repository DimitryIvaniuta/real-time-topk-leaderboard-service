# Production notes

## Redis ordering

Redis Sorted Set reads return items ordered by score. When scores are equal, reverse range reads return members in deterministic reverse member order. PostgreSQL fallback mirrors this with `ORDER BY score DESC, item_id DESC`.

## Kafka partitioning

For strict per-player ordering and good partition spread, publish events with key `leaderboardId:itemId`. For total per-leaderboard ordering, publish with key `leaderboardId`, but this can create a hot partition for very busy leaderboards.

## Durability and retry model

Kafka provides at-least-once delivery. PostgreSQL is the source of truth and Redis is a rebuildable read model.

The listener acknowledges a Kafka offset only after:

1. PostgreSQL inserts the event id into `leaderboard_processed_events`.
2. PostgreSQL mutates `leaderboard_scores` exactly once.
3. Redis receives the final absolute score and display name.

If Redis fails after PostgreSQL commits, Kafka retry finds the existing event id, reads the stored final score, and repairs Redis without applying the delta again. Invalid poison messages are retried with exponential backoff and routed to the dead-letter topic.

## PostgreSQL fallback

The API reads Redis first. If Redis is empty or unavailable, the service reads `leaderboard_scores`, returns the Top-K result, and best-effort repairs Redis. This is an availability fallback, not the target hot path for p95.

## p95 < 50ms target

The read endpoint executes a bounded Top-K Redis sorted-set read plus one hash multi-get for display names. Keep `leaderboard.max-limit` capped, deploy Redis close to the service, and monitor Redis latency and network saturation.

## Operational metrics

Important metrics:

- `leaderboard.query.latency`
- `leaderboard.events.processed`
- `leaderboard.events.duplicates`
- `leaderboard.query.postgres.fallbacks`
- Kafka consumer lag
- Redis command latency and CPU
- PostgreSQL transaction latency
