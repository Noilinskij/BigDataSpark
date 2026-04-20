#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${REPO_ROOT}"

CSV_PATH_IN_CONTAINER="${CSV_PATH_IN_CONTAINER:-/opt/spark-data/MOCK_DATA_ALL.csv}"
JDBC_USER="${JDBC_USER:-1}"
JDBC_DB="${JDBC_DB:-spark}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-user}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-1}"


wait_for_postgres() {
  local retries=30
  local i
  for ((i=1; i<=retries; i++)); do
    if docker compose exec -T postgres pg_isready -U "${JDBC_USER}" -d "${JDBC_DB}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  return 1
}

wait_for_clickhouse() {
  local retries=30
  local i
  for ((i=1; i<=retries; i++)); do
    if docker compose exec -T clickhouse clickhouse-client --user "${CLICKHOUSE_USER}" --password "${CLICKHOUSE_PASSWORD}" --query "SELECT 1" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  return 1
}

mvn -DskipTests package

APP_JAR="$(ls -1t target/*.jar | grep -Ev '(sources|javadoc|original)' | head -n 1 || true)"
[[ -n "${APP_JAR}" ]] || { echo "No built jar found in target/" >&2; exit 1; }

mkdir -p jars
cp "${APP_JAR}" jars/app.jar

PG_JAR="$(ls -1 target/libs/postgresql-*.jar | head -n 1 || true)"
[[ -n "${PG_JAR}" ]]
cp "${PG_JAR}" jars/postgresql.jar

CH_JAR="$(ls -1 target/libs/clickhouse-jdbc-*-all.jar | head -n 1 || true)"
[[ -n "${CH_JAR}" ]]
cp "${CH_JAR}" jars/clickhouse.jar

docker compose up -d postgres spark-master spark-worker
docker compose up -d clickhouse

wait_for_postgres
wait_for_clickhouse

docker compose --profile job run --rm \
  -e CSV_PATH="${CSV_PATH_IN_CONTAINER}" \
  spark-java-job

docker compose --profile job run --rm \
  -e APP_MAIN_CLASS=org.example.ClickHouseReportsJob \
  spark-java-job

echo "Success"