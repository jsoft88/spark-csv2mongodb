#!/bin/bash

set -e

MONGO_OUTPUT=$(docker exec -d mongodb sh -c "mongo < /tmp/query/mongo_query.js 2>&1 | tr -s '\n' '#'" | awk -F '#' '{print $6}')
COUNT=10

while [ "$MONGO_OUTPUT" = "0" -o -z "$MONGO_OUTPUT" -a $COUNT -gt 0 ];
do
  ((COUNT--))
  echo "waiting for mongo to have data... sleeping 10s. Remaining: $COUNT attempts"
  sleep 10
  MONGO_OUTPUT=$(docker exec -d mongodb sh -c "mongo < /tmp/query/mongo_query.js 2>&1 | tr -s '\n' '#'" | awk -F '#' '{print $6}')
done

[ "$MONGO_OUTPUT" = "0" ] && exit 1
[ "$MONGO_OUTPUT" != "0" ] && exit 0