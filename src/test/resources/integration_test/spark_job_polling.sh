#!/bin/bash

set -e

[ -z "$1" ] && exit 1

BATCH_ID=$1
JOB_STATUS=$(curl -X GET http://127.0.0.1:8998/batches/"$BATCH_ID"/state | tr -d '{' | tr -d '}' | tr ',' '\n' | grep -w 'state' | awk -F ':' '{print $NF}' | tr -d '"')
while [ "$JOB_STATUS" = "running" ];
do
  sleep 10
  JOB_STATUS=$(curl -X GET http://127.0.0.1:8998/batches/"$BATCH_ID"/state | tr -d '{' | tr -d '}' | tr ',' '\n' | grep -w 'state' | awk -F ':' '{print $NF}' | tr -d '"')
done

echo "$JOB_STATUS"