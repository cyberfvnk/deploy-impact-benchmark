#!/bin/sh
# CORRETO: "exec" substitui o shell pelo Java.
# O Java vira PID 1 e recebe o SIGTERM enviado pelo ECS / docker stop.
exec java $JAVA_OPTS -jar /app/app.jar
