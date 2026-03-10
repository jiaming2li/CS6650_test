# 注释

FROM openjdk:17-jdk-slim

WORKDIR /app

COPY target/Server-1.0-SNAPSHOT.jar app.jar

ENV KAFKA_BOOTSTRAP_SERVERS=localhost:9092

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]