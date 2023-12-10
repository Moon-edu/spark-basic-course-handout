#!/usr/bin/env bash

docker run --rm -it -v $PWD/sql/sql-practice-init.sql:/docker-entrypoint-initdb.d/init.sql -p5432:5432 -e POSTGRES_PASSWORD=password postgres:16.0-alpine
