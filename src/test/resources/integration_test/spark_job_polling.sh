#!/bin/bash

set -e

[ -z "$1" ] && exit 1

BATCH_ID=$1
JOB_STATUS=$(curl -X GET http://localhost:8998/batches/"$BATCH_ID"/state | tr -d '{' | tr -d '}' | awk -F ',' '{print $NF}' | awk -F ':' '{print $NF}')
while [ "$JOB_STATUS" = "\"running\"" ];
do
  sleep 10
  JOB_STATUS=$(curl -X GET localhost:8998/batches/"$BATCH_ID"/state | tr -d '{' | tr -d '}' | awk -F ',' '{print $NF}' | awk -F ':' '{print $NF}')
done

echo "$JOB_STATUS"