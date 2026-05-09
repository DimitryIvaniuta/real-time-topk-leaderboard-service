#!/usr/bin/env bash
set -euo pipefail
BASE_URL="${BASE_URL:-http://localhost:8080}"
LEADERBOARD_ID="${LEADERBOARD_ID:-global}"
DUPLICATE_EVENT_ID="${DUPLICATE_EVENT_ID:-smoke-duplicate-event-1}"

curl -fsS "$BASE_URL/actuator/health" | jq .

curl -fsS -X POST "$BASE_URL/leaderboard/events" \
  -H 'Content-Type: application/json' \
  -d "{\"eventId\":\"$DUPLICATE_EVENT_ID\",\"leaderboardId\":\"$LEADERBOARD_ID\",\"itemId\":\"player-1\",\"displayName\":\"Alice\",\"delta\":25}" | jq .

# Same event id should be idempotent after Kafka redelivery or duplicate publication.
curl -fsS -X POST "$BASE_URL/leaderboard/events" \
  -H 'Content-Type: application/json' \
  -d "{\"eventId\":\"$DUPLICATE_EVENT_ID\",\"leaderboardId\":\"$LEADERBOARD_ID\",\"itemId\":\"player-1\",\"displayName\":\"Alice\",\"delta\":25}" | jq .

curl -fsS -X POST "$BASE_URL/leaderboard/events" \
  -H 'Content-Type: application/json' \
  -d "{\"leaderboardId\":\"$LEADERBOARD_ID\",\"itemId\":\"player-2\",\"displayName\":\"Bob\",\"absoluteScore\":40}" | jq .

sleep 1
curl -fsS "$BASE_URL/leaderboard?leaderboardId=$LEADERBOARD_ID&limit=100" | jq .
