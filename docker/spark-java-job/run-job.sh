#!/usr/bin/env bash
set -euo pipefail

APP_JAR="${APP_JAR:-/opt/spark-app/app.jar}"
APP_MAIN_CLASS="${APP_MAIN_CLASS:-com.example.Main}"
SPARK_MASTER_URL="${SPARK_MASTER_URL:-spark://spark-master:7077}"

if [[ ! -f "${APP_JAR}" ]]; then
  exit 1
fi

EXTRA_ARGS=()
if [[ -n "${APP_ARGS:-}" ]]; then
  read -r -a EXTRA_ARGS <<< "${APP_ARGS}"
fi

EXTRA_JARS=()
[[ -f "/opt/spark/jars/postgresql.jar" ]] && EXTRA_JARS+=("/opt/spark/jars/postgresql.jar")
[[ -f "/opt/spark/jars/clickhouse.jar" ]] && EXTRA_JARS+=("/opt/spark/jars/clickhouse.jar")

SUBMIT_ARGS=()
if [[ ${#EXTRA_JARS[@]} -gt 0 ]]; then
  JARS_CSV="$(IFS=,; echo "${EXTRA_JARS[*]}")"
  JARS_CP="$(IFS=:; echo "${EXTRA_JARS[*]}")"
  SUBMIT_ARGS+=(--jars "${JARS_CSV}")
  SUBMIT_ARGS+=(--conf "spark.driver.extraClassPath=${JARS_CP}")
  SUBMIT_ARGS+=(--conf "spark.executor.extraClassPath=${JARS_CP}")
fi

exec /opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER_URL}" \
  --class "${APP_MAIN_CLASS}" \
  "${SUBMIT_ARGS[@]}" \
  "${APP_JAR}" "${EXTRA_ARGS[@]}"
