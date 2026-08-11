FROM maven:3.9.9-eclipse-temurin-21 AS build
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*

WORKDIR /rally-common
RUN git clone --depth 1 --branch main https://github.com/RallyDeals/rally-common.git .
RUN mvn -B -q install -DskipTests

WORKDIR /rally-security
RUN git clone --depth 1 --branch main https://github.com/RallyDeals/rally-security.git .
RUN mvn -B -q install -DskipTests

WORKDIR /rally-gateway
COPY pom.xml .
RUN mvn -B dependency:go-offline
COPY src src
RUN mvn -B -DskipTests package

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /rally-gateway/target/rally-gateway.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
