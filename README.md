# datamaster

docker-compose -f docker-compose.yaml up

docker exec -ti jupyter-pyspark sh /app/twitter/bronze/batch/start_batch_bronze_tt_tweets.sh 150

docker exec -ti jupyter-pyspark sh /app/twitter/bronze/streaming/extract/start_streaming_bronze_tt_tweets_extract.sh

docker exec -ti jupyter-pyspark sh /app/twitter/bronze/streaming/load/start_streaming_bronze_tt_tweets_load.sh

docker exec -ti jupyter-pyspark /bin/bash /app/twitter/silver/batch/start_batch_silver_tt_tweets.sh

docker exec -ti jupyter-pyspark /bin/bash /app/youtube/bronze/batch/start_batch_bronze_yt_videos.sh
docker exec -ti jupyter-pyspark /bin/bash /app/youtube/silver/batch/start_batch_silver_yt_videos.sh


spark-submit --packages org.mongodb.spark:mongo-spark-connector:10.0.2,io.delta:delta-core_2.12:1.2.1 /app/youtube/silver/batch/batch_silver_yt_videos.py

docker exec -ti jupyter-pyspark spark-submit  --conf spark.dynamicAllocation.enabled=false --driver-cores 1 --driver-memory 1g  --conf spark.sql.autoBroadcastJoinThreshold=-1 --conf spark.executor.memoryOverhead=700  --conf spark.driver.extraJavaOptions=-XX:+UseG1GC  --packages org.mongodb.spark:mongo-spark-connector:10.0.2,io.delta:delta-core_2.12:1.2.1 /app/youtube/silver/batch/batch_silver_yt_videos.py

docker exec -ti jupyter-pyspark /bin/bash /app/test.sh