FROM eclipse-temurin:17-jre-alpine

WORKDIR /app

COPY target/campuslab-ms-kafka-admin-0.0.1-SNAPSHOT.jar app.jar

USER 10001

EXPOSE 8086

ENTRYPOINT ["java", "-Xms64m", "-Xmx192m", "-jar", "app.jar"]
