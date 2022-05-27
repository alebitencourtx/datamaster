from pyspark.sql import SparkSession
import pyspark.sql.functions as psf
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
import datetime
import pytz

spark = SparkSession \
    .builder \
    .appName("kafka-test") \
    .config("spark.mongodb.read.connection.uri", "mongodb://mongodb:27017/twitter.santander_tts") \
    .config("spark.mongodb.write.connection.uri", "mongodb://mongodb:27017/twitter.santander_tts") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector:10.0.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:9092") \
  .option("subscribe", "twitter") \
  .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

def parseJSON(df, *cols):
    ''''
    Funcao com objetivo de inferir o schema do json recebido pelo Kafka
    '''
    res = df
    for i in cols:
        schema = spark.read.json(res.rdd.map(lambda x: x[i])).schema
        res = res.withColumn(i, psf.from_json(psf.col(i), schema))

    return res

def process_row(df, epoch_id):
    try:
        from pyspark.sql.functions import expr, decode
        df = df.select('value')
        qtd = df.count()
        if qtd > 0:
            df = df.withColumn('json', expr("decode(value, 'UTF-8')")).select('json')
            df = parseJSON(df, 'json')
            
            # Definindo o id do twitter como key do documento no mongoDB 
            df1 = df.withColumn("_id", df["json.id"])
            # Mascaramento do campo author_id, simulando uma informacao sensivel
            df1 = df.withColumn("json", psf.col("json").withField("author_id", psf.sha2(df["json.author_id"],256)))        
            # Adicionando coluna de controle contendo a data de carga
            current_time = datetime.datetime.now(pytz.timezone('America/Sao_Paulo')).strftime("%Y-%m-%d %H:%M:%S")
            df1 = df1.withColumn('dat_ref_carga_fast', psf.lit(current_time))

            print("Inserindo {} tweet(s) no mongoDB".format(qtd))
            print("Voce pode consultar o banco em: http://localhost:9999/db/twitter/santander_tts")
            df1.write.mode('append').format('mongodb').save()
    except Exception as e:
        print('Failed : ' + str(e))
        pass
    pass

query = df.writeStream.foreachBatch(process_row).start() 

spark.streams.awaitAnyTermination()
