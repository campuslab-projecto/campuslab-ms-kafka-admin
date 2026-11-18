
# ETAPA 1: Compilar el proyecto con Maven y Java 17
FROM maven:3.9-eclipse-temurin-17 AS build

WORKDIR /app

COPY pom.xml .
COPY src ./src

RUN mvn -B clean package -DskipTests

# ETAPA 2: Ejecutar Kafka Admin
FROM eclipse-temurin:17-jre-alpine

WORKDIR /app

COPY --from=build /app/target/campuslab-ms-kafka-admin-0.0.1-SNAPSHOT.jar app.jar

USER 10001

EXPOSE 8086

ENTRYPOINT ["java", "-Xms64m", "-Xmx192m", "-jar", "app.jar"]
