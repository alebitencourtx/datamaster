# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, _parse_datatype_string
import sys

#importando módulo utilities de outro path
sys.path.insert(0, '/app/utils')
from utilities import init_logger, current_time_sp
from settings import twitter, mongo
logger = init_logger('twitter-batch-mongo-to-silver')

# Leitura das configurações
bearer_token = twitter['bearer_token']
mongo_db = twitter['mongo_db']
mongo_coll = twitter['mongo_coll']
mongo_host = mongo['host']
mongo_ui = mongo['mongo-express']

# Inicia a sessão Spark realizando o download dos pacotes necessários
spark = SparkSession \
    .builder \
    .appName("twitter-batch-mongo-to-silver") \
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

def get_max_data_carga(df_origem):
    """
    Retorna a maior data de carga, comparando os campos de fast e batch.
    Esta data será utilizada para compor a regra de extração delta.
    """    
    df_max_data = df_origem.withColumn("dat_max_carga", f.when(df_origem.dat_ref_carga_fast >= df_origem.dat_ref_carga_batch, df_origem.dat_ref_carga_fast)
                                                 .when(df_origem.dat_ref_carga_fast.isNull(), df_origem.dat_ref_carga_batch)
                                                 .when(df_origem.dat_ref_carga_batch.isNull(), df_origem.dat_ref_carga_fast)
                                                 .otherwise(df_origem.dat_ref_carga_batch))
    return df_max_data

def get_ref_carga_destino(partition):
    """
    Recebe a ultima partição carregada e realiza a leitura apenas desta para recuperar e retornar a ultima data de carga da origem.
    Com essa data (timestamp) é possivel seguir com a extração delta.
    """
    if partition == 'False':
        logger.info('Diretório de destino não existe: Primeira execução')
        return '01-01-1900'
    else:
        try:
            df = spark.read.format("delta").load('/home/jovyan/work/lake/twitter/santander_tts/{}/'.format(partition))
            max_data = df.select(f.max(f.col('dat_ref_carga_origem')).alias('max_col1')).first().max_col1

            return max_data
        except Exception as e:
            logger.exception('Erro na leitura da partição de destino')
            sys.exit(3)

# Recebe da shell o nome da ultima partição carregada ou "False", caso primeira execução
if len(sys.argv) > 1:
    partition = sys.argv[1]
else:
    logger.exception('Necessário 1 argumento: ultima partição')
    sys.exit(3)
    

# Leitura do mongo (db e collection especificadas na SparkSession).
# O schema está fixo devido a possibilidade de flexibilidade do Mongo
schema =  'struct<_id:string,dat_ref_carga_batch:string,dat_ref_carga_fast:string,json:struct<data:struct<author_id:string,created_at:string,geo:struct<coordinates:struct<type:string,coordinates:array<double>>,place_id:string>,id:string,lang:string,public_metrics:struct<retweet_count:int,reply_count:int,like_count:int,quote_count:int>,source:string,text:string>,includes:struct<places:array<struct<country:string,country_code:string,full_name:string,id:string>>,users:array<struct<id:string,location:string,name:string,username:string,verified:boolean>>>>>'
final_schema = _parse_datatype_string(schema)
logger.info('Inciando leitura da origem MongoDB')
df_twitter = spark.read.format('mongodb').load(schema=final_schema)

# Gera dataframe no formato tabular, apenas para os campos pertinentes para a camada Silver
df_twitter_tab = df_twitter.select('*','json.*','json.data.public_metrics.*','json.includes.*', 'json.data.geo.*')\
        .withColumn("zipped", f.explode_outer(f.arrays_zip("places","users"))).drop('public_metrics','places','users','geo')\
        .select('data.*','data.public_metrics.*','includes.*','zipped.places.country','zipped.places.country_code',f.col('zipped.places.full_name').alias('full_name_place'),f.col('zipped.users.id').alias('id_user'),'zipped.users.location','zipped.users.name','zipped.users.username','zipped.users.verified','coordinates.coordinates', 'coordinates.type','place_id',f.col("dat_ref_carga_batch"),f.col("dat_ref_carga_fast"))\
        .drop('public_metrics','places','users','geo')

# Filtro com data de ultima carga para extração delta. Caso primeira execução, carregará tudo
logger.info('Buscando ultima data de carga no destino')
dt_ultima_carga = get_ref_carga_destino(partition)

logger.info('Buscando a data mais rescente por registro na origem (fast ou batch)')
df_twitter_tab_max_data = get_max_data_carga(df_twitter_tab)

logger.info(f'Extraindo dados a partir da data {dt_ultima_carga}')
df_twitter_delta = df_twitter_tab_max_data.where(f.col('dat_max_carga') > dt_ultima_carga)

# Cria variavel de data na timezone de SP
now = current_time_sp()
# Adiciona colunas de controle e popula data de carga
df_final = df_twitter_delta.withColumnRenamed('dat_max_carga','dat_ref_carga_origem')\
                            .withColumn('dat_ref_carga', f.lit(now))\
                            .withColumn('dat_ref_update', f.lit(''))\
                            .drop('dat_ref_carga_batch','dat_ref_carga_fast')

# Cria coluna que será utilizada no particionamento, com base na data de criação do tweet
df_final_partition = df_final.withColumn('dt_create_tweet', f.substring(f.col('created_at'),1,10))

try:
    # Identifica se é a primeira carga para definir escrita
    if dt_ultima_carga == '01-01-1900':
        logger.info('Primeira carga, inserindo dados no destino')
        df_final_partition.write.partitionBy('dt_create_tweet').format("delta").save("/home/jovyan/work/lake/twitter/santander_tts")
    else:
        # Realiza o "upsert", atualizando os registros caso exista no destino (a partir da chave unica de id e dt_create_tweet). 
        # O id do tweet é sempre único, o uso do campo dt_create_tweet como chave é uma boa pratica uma vez que é o campo de partição e otimiza o processo de merge
        # Apenas o campo "dat_ref_carga" se mantem inalterado, para rastreabilidade de mudanças.
            
        deltaTable = DeltaTable.forPath(spark, "/home/jovyan/work/lake/twitter/santander_tts")
        logger.info('Realizando upsert dos dados')
        res = deltaTable.alias("oldData") \
        .merge(
            df_final_partition.alias("updates"),
            "oldData.id = updates.id AND oldData.dt_create_tweet = updates.dt_create_tweet") \
        .whenMatchedUpdate(set =
            {
                "author_id":"updates.author_id",
                "created_at":"updates.created_at",
                "id":"updates.id",
                "lang":"updates.lang",
                "source":"updates.source",
                "text":"updates.text",
                "like_count":"updates.like_count",
                "quote_count":"updates.quote_count",
                "reply_count":"updates.reply_count",
                "retweet_count":"updates.retweet_count",
                "dat_ref_carga_origem":"updates.dat_ref_carga_origem",
                "dat_ref_carga":"oldData.dat_ref_carga",
                "dat_ref_update": f.lit(now)
            }
        ) \
        .whenNotMatchedInsertAll() \
        .execute()

except Exception as e:
    logger.exception('Falha na gravação: ')
    sys.exit(3)

logger.info('Dados carregados')
sys.exit(42)
