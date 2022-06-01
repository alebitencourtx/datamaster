#!/bin/bash

pip install --no-cache-dir -q -r /app/requirements.txt 

spark-submit \
    --name "twitter-batch-mongo-to-silver" \
    --packages org.mongodb.spark:mongo-spark-connector:10.0.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 \
    /app/twitter/bronze/streaming/load/streaming_bronze_tt_tweets_load.py; exit_code=$?

if [ $exit_code -eq 0 ]
  then
    echo ">>>>> EXECUTADO COM SUCESSO <<<<<"
  else
    echo ">>>>> OCORREU ERRO NA EXECUCAO <<<<<"
fi


exit "$exit_code"