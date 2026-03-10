FROM eclipse-temurin:17-jdk-jammy

WORKDIR /app

COPY target/Server-1.0-SNAPSHOT.jar app.jar

ENV KAFKA_BOOTSTRAP_SERVERS=localhost:9092

EXPOSE 8080

ENTRYPOINT ["java", "-Xms512m", "-Xmx2g", "-jar", "app.jar"]