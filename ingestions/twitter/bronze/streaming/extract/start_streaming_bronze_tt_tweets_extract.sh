#!/bin/bash

pip install --no-cache-dir -q -r /app/requirements.txt 

python /app/twitter/bronze/streaming/extract/streaming_bronze_tt_tweets_extract.py; exit_code=$?

if [ $exit_code -eq 0 ]
  then
    echo ">>>>> EXECUTADO COM SUCESSO <<<<<"
  else
    echo ">>>>> OCORREU ERRO NA EXECUCAO <<<<<"
fi


exit "$exit_code"