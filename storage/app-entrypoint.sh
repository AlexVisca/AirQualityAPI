#!/bin/bash
set -e
# wait for the database to be up
if [ -n "$DB_HOST" ]; then
    /usr/bin/wait-for-it "$DB_HOST:${DB_PORT:-3306}"
fi
# wait for the server to be up
if [ -n "$SERVER_HOST" ]; then
    /usr/bin/wait-for-it "$SERVER_HOST:${SERVER_PORT:-9092}"
fi
# run the main container command
exec "$@"