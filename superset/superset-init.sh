#!/bin/bash
set -e

echo "Waiting for ClickHouse..."

until curl -s "http://clickhouse:8123/ping" > /dev/null; do
  echo "Still waiting for ClickHouse..."
  sleep 2
done

echo "ClickHouse is up - starting Superset"

superset db upgrade
superset init

superset fab create-admin --username "$ADMIN_USERNAME" --firstname Superset --lastname Admin --email "$ADMIN_EMAIL" --password "$ADMIN_PASSWORD" || echo "Admin user may already exist"

exec /usr/bin/run-server.sh
