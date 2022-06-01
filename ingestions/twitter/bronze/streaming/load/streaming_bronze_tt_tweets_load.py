from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
import sys

#importando módulo utilities de outro path
sys.path.insert(0, '/app/utils')
from utilities import init_logger, current_time_sp
from settings import twitter, mongo, kafka
logger = init_logger('streaming_bronze_tt_tweets_load')

# Leitura das configurações
mongo_db = twitter['mongo_db']
mongo_coll = twitter['mongo_coll']
mongo_host = mongo['host']
mongo_ui = mongo['mongo-express']
broker = kafka['broker']
kafka_ui = kafka['kafka-ui']
twitterTopic = kafka['twitterTopic']

spark = SparkSession \
    .builder \
    .appName("streaming_bronze_tt_tweets_load") \
    .config("spark.mongodb.read.connection.uri", f"{mongo_host}{mongo_db}.{mongo_coll}") \
    .config("spark.mongodb.write.connection.uri", f"{mongo_host}{mongo_db}.{mongo_coll}") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector:10.0.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

logger.info('Realizando leitura do tópico Kafka')
df_kafka_twitter = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", broker) \
  .option("subscribe", twitterTopic) \
  .load()

def parseJSON(df, *cols):
    ''''
    Funcao com objetivo de inferir o schema do json recebido pelo Kafka
    '''
    res = df
    for i in cols:
        schema = spark.read.json(res.rdd.map(lambda x: x[i])).schema
        res = res.withColumn(i, f.from_json(f.col(i), schema))
    
    try:
        res = res.withColumn('json', res['json'].dropFields('_corrupt_record'))
        res = res.withColumn('json', res['json'].dropFields('matching_rules'))
    except:
        pass
    
    return res

def process_row(df, epoch_id):
    """
    Método reponsávem por transformar cada mensagem recebida do Kafka e inserir no Mongo
    """
    try:        
        df_tweet = df.select('value')
        qtd = df_tweet.count()
        if qtd > 0:
            df_tweet = df_tweet.withColumn('json', f.expr("decode(value, 'UTF-8')")).select('json')
            df_tweet = df_tweet.withColumn('json', f.regexp_replace('json', "True", 'true')).withColumn('json', f.regexp_replace('json', "False", 'false'))
            df_tweet = parseJSON(df_tweet, 'json')

            # Definindo o id do twitter como key do documento no mongoDB 
            df_tweet_id = df_tweet.withColumn("_id", df_tweet["json.data.id"])

            # Adicionando coluna de controle contendo a data de carga
            now = current_time_sp()
            df_tweet_id = df_tweet_id.withColumn('dat_ref_carga_fast', f.lit(now))
            df_final = df_tweet_id.withColumn('dat_ref_carga_batch', f.lit(''))
            logger.info(f"Inserindo {qtd} tweet(s) no mongoDB")
            logger.info(f"Voce pode consultar o banco em: {mongo_ui}/db/{mongo_db}/{mongo_coll}")
            # Insere no método upsert
            df_final.write.mode('append').format('mongodb').save()
    except Exception as e:
        logger.exception('Falha em um processamento :')
        pass
    pass
query = df_kafka_twitter.writeStream.foreachBatch(process_row).start() 

spark.streams.awaitAnyTermination()
