#!/bin/bash
echo 'óooooooooo'

spark-submit --packages org.mongodb.spark:mongo-spark-connector:10.0.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 /app/twitter/raw/streaming/load/twitter_load.py

