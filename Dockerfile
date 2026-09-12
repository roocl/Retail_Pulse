FROM flink:1.20.3-scala_2.12-java17@sha256:6a2b771c073b523e43d72b46e338ce6cef25e969965444f6afe1b0bb15fa9772 AS analytics
COPY analytics-job/target/analytics-job-0.1.0-SNAPSHOT.jar /opt/flink/usrlib/analytics-job.jar

FROM eclipse-temurin:17-jre-jammy@sha256:ec72ba5962b45ae4e7f96bfb5ebf6eeb34a488b967f937c8e14f0aaec688954f AS java-runtime
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
WORKDIR /app
USER 10001:10001
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=70.0 -XX:+ExitOnOutOfMemoryError"
ENTRYPOINT ["java", "-jar", "/app/application.jar"]

FROM java-runtime AS producer
COPY event-producer/target/event-producer-0.1.0-SNAPSHOT.jar /app/application.jar

FROM java-runtime AS consumer
COPY event-consumer/target/event-consumer-0.1.0-SNAPSHOT.jar /app/application.jar

FROM java-runtime AS query
COPY query-api/target/query-api-0.1.0-SNAPSHOT.jar /app/application.jar
HEALTHCHECK --interval=5s --timeout=3s --start-period=30s --retries=20 CMD curl --fail --silent http://127.0.0.1:8080/actuator/health > /dev/null || exit 1
