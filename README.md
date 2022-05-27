# datamaster

docker exec -ti python python /app/twitter/raw/streaming/extract/twitter_extractor.py
docker exec -ti jupyter-pyspark /app/twitter/raw/streaming/load/start.sh
