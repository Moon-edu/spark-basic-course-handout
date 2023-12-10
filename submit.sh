#!/usr/bin/env bash

RUNNER_CONTAINER=worker-1

echo "Packaging Python code..."
mkdir -p dist
rm -rf dist/*

cp src/spark-main.py dist/spark-main.py

python -m pip install -r requirements.txt -t src/libs
cd src && zip -r ../dist/libs.zip libs/ && cd ../

cd src && zip -r ../dist/jobs.zip jobs/ && cd ../

echo "Copying Python code to Spark cluster..."
docker cp dist/ $RUNNER_CONTAINER:/

NOW=$(date "+%Y%m%d%H%M%S")
LOG_DIR=logs/driver/driver-$NOW/1
mkdir -p $LOG_DIR
STDOUT_LOG_FILE="$LOG_DIR/stdout"
STDERR_LOG_FILE="$LOG_DIR/stderr"

echo "Submitting job to Spark cluster... Logs: $LOG_FILE"
docker exec -it $RUNNER_CONTAINER spark-submit --master spark://spark-master:7077 --py-files /dist/libs.zip,/dist/jobs.zip --conf "spark.driver.extraJavaOptions=-Duser.timezone=Asia/Seoul" --conf "spark.executor.extraJavaOptions=-Duser.timezone=Asia/Seoul" /dist/spark-main.py > "$STDOUT_LOG_FILE" 2> "$STDERR_LOG_FILE"