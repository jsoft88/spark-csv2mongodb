#!/bin/bash

set -e

docker exec -it spark-master sh -c "spark-submit --master spark://spark-master:7077 --conf spark.executors.memory=2g --conf spark.driver.memory=2g --class com.org.batch.Main /home/bitnami/jars/batch-job-lib-assembly-0.1.jar --reader-type csv --writer-type mongodb --transformation-type no-op --input-source file:///home/bitnami/data/movies_metadata.csv --mongo-output-uri mongodb://db:27017 --app-name csv2mongo --reader-config-key movies_metadata --mongo-output-database movies --mongo-output-collection movies_metadata" > /dev/null

echo "success"