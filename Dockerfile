# syntax=docker/dockerfile:1
# --- Build stage ---
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app

ARG GITHUB_ACTOR

# Write settings.xml to a path NOT shadowed by the /root/.m2 cache mount
RUN --mount=type=secret,id=github_token \
    mkdir -p /root/.m2-config && \
    GITHUB_TOKEN=$(cat /run/secrets/github_token) && \
    cat > /root/.m2-config/settings.xml <<EOF
<settings>
  <servers>
    <server>
      <id>github-common</id>
      <username>${GITHUB_ACTOR}</username>
      <password>${GITHUB_TOKEN}</password>
    </server>
    <server>
      <id>github-security</id>
      <username>${GITHUB_ACTOR}</username>
      <password>${GITHUB_TOKEN}</password>
    </server>
  </servers>
</settings>
EOF

COPY pom.xml .
RUN --mount=type=cache,target=/root/.m2 mvn -s /root/.m2-config/settings.xml -B dependency:go-offline
COPY src ./src
RUN --mount=type=cache,target=/root/.m2 mvn -s /root/.m2-config/settings.xml -B clean package -DskipTests

# --- Run stage ---
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /app/target/*.jar app.jar
EXPOSE 8088
ENTRYPOINT ["java", "-jar", "app.jar"]