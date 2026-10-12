# =============================================================================
# Build multi-stage: compila com Maven + JDK 11 e roda num JRE 11 enxuto.
# O Mac não precisa ter Java 11 instalado.
# =============================================================================

# ---------- 1) Build ----------
FROM maven:3.9.9-eclipse-temurin-11 AS build
WORKDIR /src
# Copia só o pom primeiro: as dependências ficam em cache entre builds.
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B -DskipTests package

# ---------- 2) Runtime ----------
FROM eclipse-temurin:11-jre
# curl só para o HEALTHCHECK (equivalente ao health check da task definition).
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /src/target/app.jar /app/app.jar

# ENTRYPOINT_MODE escolhe como o Java é iniciado:
#   exec  -> "exec java ..." : Java vira PID 1 e RECEBE o SIGTERM (correto)
#   shell -> "java ..."      : sh fica como PID 1 e NÃO repassa SIGTERM;
#                              o Java só morre no SIGKILL após o stopTimeout.
ARG ENTRYPOINT_MODE=exec
COPY docker/entrypoint-${ENTRYPOINT_MODE}.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Heap proporcional ao limite de memória do container (como numa task ECS).
ENV JAVA_OPTS="-XX:MaxRAMPercentage=60 -XX:+ExitOnOutOfMemoryError"

HEALTHCHECK --interval=5s --timeout=3s --start-period=20s --retries=3 \
  CMD curl -fs http://localhost:8080/actuator/health || exit 1

ENTRYPOINT ["/app/entrypoint.sh"]
