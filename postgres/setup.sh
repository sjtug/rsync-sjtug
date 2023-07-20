#!/usr/bin/env bash
set -e

export POSTGRES_DB=${POSTGRES_DB:-$POSTGRES_USER}
export DATABASE_URL="postgres:///?host=/var/run/postgresql&user=$POSTGRES_USER&password=$POSTGRES_PASSWORD&dbname=$POSTGRES_DB"

echo "Running migrations"
oneshot-migration
echo "Migrations complete"