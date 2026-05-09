FROM gradle:9-jdk21-alpine AS builder
WORKDIR /workspace
COPY --chown=gradle:gradle . .
RUN gradle clean bootJar --no-daemon

FROM eclipse-temurin:21-jre-alpine
LABEL org.opencontainers.image.title="real-time-topk-leaderboard-service"
LABEL org.opencontainers.image.description="Real-time Top-K leaderboard service with Kafka, Redis and PostgreSQL"
WORKDIR /app
RUN addgroup -S app && adduser -S app -G app
COPY --from=builder /workspace/build/libs/*.jar /app/app.jar
USER app
EXPOSE 8080
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-XX:+ExitOnOutOfMemoryError", "-jar", "/app/app.jar"]
