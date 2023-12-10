#!/usr/bin/env bash
sleep 10s
/opt/spark/sbin/start-history-server.sh --properties-file /opt/spark/conf/spark-history.properties

tail -F /dev/null