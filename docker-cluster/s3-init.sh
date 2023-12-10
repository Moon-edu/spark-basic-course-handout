#!/usr/bin/env bash
awslocal s3api create-bucket --bucket datawarehouse
awslocal s3api create-bucket --bucket jobhistory
awslocal s3api create-bucket --bucket dataset

echo "" > /tmp/empty.txt
awslocal s3 cp /tmp/empty.txt s3://jobhistory/spark/empty.txt

awslocal s3 cp /dataset/covid.csv s3://dataset/covid/covid.csv
awslocal s3 cp /dataset/who-region.csv s3://dataset/covid/who-region.csv
