# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import sys

#importando módulo utilities de outro path
sys.path.insert(0, '/app/utils')
from utilities import init_logger, current_time_sp
from settings import youtube, mongo
logger = init_logger('twitter-batch-mongo-to-silver')

# Leitura das configurações
mongo_db = youtube['mongo_db']
mongo_coll = youtube['mongo_coll']
mongo_host = mongo['host']
mongo_ui = mongo['mongo-express']

# Inicia a sessão Spark realizando o download dos pacotes necessários
spark = SparkSession \
    .builder \
    .appName("YouTube-Batch-Mongo-To-Silver") \
    .config("spark.mongodb.read.connection.uri", f"{mongo_host}{mongo_db}.{mongo_coll}?readPreference=primaryPreferred") \
    .config("spark.mongodb.write.connection.uri", f"{mongo_host}{mongo_db}.{mongo_coll}") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector:10.0.2,io.delta:delta-core_2.12:1.2.1") \
    .getOrCreate()
sc = spark.sparkContext
spark.sparkContext.setLogLevel("ERROR")

# Import depois do download do pacote delta
from delta.tables import *

# Leitura do mongo (db e collection especificadas na SparkSession). Retorna coluna video no formato struct
logger.info('Inciando leitura da origem MongoDB')
df_youtube = spark.read.format('mongodb').load()

# Gera dataframe no formato tabular, apenas para os campos pertinentes para a camada Silver
df_youtube_tab = df_youtube.select('video.resourceId.videoId','video.*','video.statistics.*', 'video.thumbnails.maxres.*','dat_ref_carga_batch')\
                            .drop('resourceId','statistics', 'thumbnails')

# Cria variavel de data na timezone de SP
now = current_time_sp()

# Adiciona colunas de controle e popula data de carga
df_final = df_youtube_tab.withColumnRenamed('dat_ref_carga_batch','dat_ref_carga_origem')\
                          .withColumn('dat_ref_carga', f.lit(now)).withColumn('dat_ref_update', f.lit(''))

# Identifica se é a primeira carga para definir escrita
try:
    deltaTable = DeltaTable.forPath(spark, "/home/jovyan/work/lake/youtube/videos")
    primeira_carga = False
except Exception as e:
    primeira_carga = True

try:
    if primeira_carga:
        # Grava os dados no formato delta, criando o diretório
        logger.info('Primeira carga, inserindo dados no destino')
        df_final.write.format("delta").save("/home/jovyan/work/lake/youtube/videos")
    else:
        # Realiza o "upsert", atualizando os registros caso exista no destino (a partir da chave unica de videoId). 
        # Apenas o campo "dat_ref_carga" se mantem inalterado, para rastreabilidade de mudanças.
        logger.info('Realizando upsert dos dados')
        deltaTable.alias("oldData") \
        .merge(
            df_final.alias("updates"),
                "oldData.videoId = updates.videoId") \
        .whenMatchedUpdate(set =
            {
                "videoId":"updates.videoId",
                "channelId":"updates.channelId",
                "channelTitle":"updates.channelTitle",
                "description":"updates.description",
                "playlistId":"updates.playlistId",
                "position":"updates.position",
                "publishedAt":"updates.publishedAt",
                "title":"updates.title",
                "videoOwnerChannelId":"updates.videoOwnerChannelId",
                "videoOwnerChannelTitle":"updates.videoOwnerChannelTitle",
                "commentCount":"updates.commentCount",
                "favoriteCount":"updates.favoriteCount",
                "likeCount":"updates.likeCount",
                "viewCount":"updates.viewCount",
                "url":"updates.url",
                "width":"updates.width",
                "height":"updates.height",
                "dat_ref_carga_origem":"updates.dat_ref_carga_origem",
                "dat_ref_carga":"oldData.dat_ref_carga",
                "dat_ref_update": f.lit(now)
            }
        ) \
        .whenNotMatchedInsertAll() \
        .execute()

except Exception as e:
    print(e)
    sys.exit(3)

logger.info('Dados carregados')
sys.exit(42)