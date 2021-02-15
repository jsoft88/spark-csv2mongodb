#!/bin/bash

set -e

RESPONSE=$(curl -X POST http://localhost:8998/batches \
  -H "Content-Type: application/json" \
  -d @- << EOF
{
    "driverMemory": "1g",
    "executorMemory": "1g",
    "className": "com.org.batch.Main",
    "numExecutors": 1,
    "file": "/opt/jars/batch-job-lib-assembly-0.1.jar",
    "args": [
      "--reader-type", "csv",
      "--writer-type", "mongodb",
      "--transform-type", "no-op",
      "--input-source", "file:///home/bitnami/data/movies_metadata.csv",
      "--mongo-output-uri", "mongodb://db/movies.movies_metadata"
    ]
}
EOF
)

echo "$RESPONSE" | tr -d '{'| tr -d '}' | tr ',' '\n' | grep -i id | awk -F '=' '{print $NF}' | tr -d '"'