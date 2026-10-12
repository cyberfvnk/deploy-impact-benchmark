#!/bin/sh
# ANTI-PADRÃO (proposital): sem "exec".
# O sh fica como PID 1 e o Java roda como processo filho. O sh NÃO repassa
# o SIGTERM, então o Java ignora o pedido de parada e só morre no SIGKILL,
# depois do stopTimeout. Resultado: nenhum graceful shutdown roda.
java $JAVA_OPTS -jar /app/app.jar
