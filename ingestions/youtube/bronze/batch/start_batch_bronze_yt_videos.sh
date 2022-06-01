#!/bin/bash

pip install --no-cache-dir -q -r /app/requirements.txt 

python /app/youtube/bronze/batch/batch_bronze_yt_videos.py; exit_code=$?

if [ $exit_code -eq 0 ]
  then
    echo ">>>>> EXECUTADO COM SUCESSO <<<<<"
  else
    echo ">>>>> OCORREU ERRO NA EXECUCAO <<<<<"
fi


exit "$exit_code"