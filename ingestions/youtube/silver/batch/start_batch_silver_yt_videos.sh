#!/bin/bash


spark-submit --conf spark.dynamicAllocation.enabled=false \
            --driver-cores 2 \
            --driver-memory 1g \
            --conf spark.executor.memoryOverhead=1g  \
            --conf spark.memory.offHeap.size=500m \
            --conf spark.driver.extraJavaOptions=-Dio.netty.tryReflectionSetAccessible=true  \
            --conf spark.executor.extraJavaOptions=-Dio.netty.tryReflectionSetAccessible=true \
            --packages org.mongodb.spark:mongo-spark-connector:10.0.2,io.delta:delta-core_2.12:1.2.1 \
            /app/youtube/silver/batch/batch_silver_yt_videos.py; exit_code=$?
            
if [[  $exit_code == 0  ||  $exit_code == 42 ]]
  then
    echo ">>>>> EXECUTADO COM SUCESSO <<<<<"
    exit_code=0
  else
    echo ">>>>> OCORREU ERRO NA EXECUCAO <<<<<"
fi


exit "$exit_code"

