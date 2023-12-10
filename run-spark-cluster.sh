#!/usr/bin/env bash

if [ ! -f "$PWD/docker-cluster/hadoop-aws-3.3.4.jar" ]; then
  curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -o $PWD/docker-cluster/hadoop-aws-3.3.4.jar
fi

if [ ! -f "$PWD/docker-cluster/aws-java-sdk-bundle-1.12.262.jar" ]; then
  curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -o $PWD/docker-cluster/aws-java-sdk-bundle-1.12.262.jar
fi

echo "Removing old logs..."
rm -rf logs

NOW=$(date "+%y/%m/%d %H:%M:%S")
NODES=(driver worker1 worker2 worker3)
for NODE in ${NODES[@]}; do
  DIR=logs/$NODE/dummy/1
  mkdir -p $DIR
  echo "$NOW DEBUG First stdout spark logs for $NODE" > $DIR/stdout
  echo "$NOW DEBUG First stderr spark logs for $NODE" > $DIR/stderr
done

docker system prune -f
docker volume prune -f
docker compose -p spark -f docker-cluster/spark-cluster.yml up -d
