# -*- coding: utf-8 -*-
import tweepy
import pymongo
import sys

#importando módulo utilities de outro path
sys.path.insert(0, '/app/utils')
from utilities import init_logger, current_time_sp
from settings import twitter, mongo
logger = init_logger('batch_bronze_tt_tweets')

# Leitura das configurações
bearer_token = twitter['bearer_token']
mongo_db = twitter['mongo_db']
mongo_coll = twitter['mongo_coll']
mongo_host = mongo['host']
mongo_ui = mongo['mongo-express']

def monta_json(tweet):
    """
    Metodo estruta o dicionario no layout correta para ingestao no MongoDB
    """
    # Define a data atual com timezone
    now = current_time_sp()
    # Recebe o tweet e monta na estrutura {'_id', 'json'{}, 'dat_ref_carga_atlz'}
    dict_raw = tweet    
    dict_final = {}
    dict_final["_id"] = dict_raw['data']["id"]
    dict_final["json"] = dict_raw
    dict_final['dat_ref_carga_batch'] = now
    
    return dict_final


def insere_mongo(document, mongo_db, mongo_coll, host=mongo_host):
    """
    Metodo realiza a ingestão no mongo, na mesma tabela do fluxo fast, no método upsert.
    A coluna de dat_ref_carga_fast nao foi passada e por isso nao sera alterada, podendo assim rastrear as mudancas da tabela.
    Com ambos os fluxos (fast e batch) inserindo na mesma tabela, temos uma arquitetura lambda de ingestao.
    """
    try:
        myclient = pymongo.MongoClient(host)
        mydb = myclient[mongo_db]
        mycol = mydb[mongo_coll]

        x = mycol.update_one(
                            filter={
                                    '_id': document['_id'],
                                    },
                            update={
                                '$set': document,
                                    },
                            upsert=True
                             )
    except Exception as e:
        logger.exception('Erro na ingestão do MongoDB : ')
        sys.exit(3)   
 
if __name__ == "__main__":
    # Recebe da shell a quantidade de tweets a serem extraidos
    if len(sys.argv) > 1:
        qtd_results = int(sys.argv[1])
    else:
        print('Quantidade de tweets não informada, a extração seguirá com valor default de 100')
        print('Caso desege definir a quantidade, informe um numero multiplo de 50 na próxima execução')
        qtd_results = 100
    


    limit = int(qtd_results/50)
    logger.info('Extraindo {} tweets'.format(qtd_results))
    
    try:
        # Realiza conexão e faz a busca no endpoit 'search_recent_tweets'
        # Realiza a paginação do retorno com base no valor de quantidade de tweets informado
        client = tweepy.Client(bearer_token)
        for tweet in tweepy.Paginator(client.search_recent_tweets,' "santander" -is:retweet -is:reply', 
                                                    tweet_fields=["id","author_id","text","created_at","geo","lang","public_metrics","source"],
                                                    user_fields = ['name','username','location','verified'],
                                                    expansions = ['geo.place_id','author_id'],
                                                    place_fields = ['country','country_code'],
                                                    max_results=50,
                                                    limit=limit
                                                ):
            tweets = tweet.data
            users = tweet.includes['users']
            # 'places' não é obrigatório no retorno, por isso o tratamento
            try:
                places = tweet.includes['places']
            except:
                places = False
                pass
            
            dict_final = dict()
            dict_final['includes']= dict()
            dict_final['includes']['users']= []
            dict_final['includes']['places']= []

            logger.info('Realizando ingestão no MongoDB')
            # Compõe o dicionário com as informações de user e place, monta Json e envia ao MongoDB
            for tweet in tweets:
                dict_final['data']=tweet.data
                for user in users: 
                    if user['id'] == tweet['author_id']:
                        users_list = []
                        users_list.append(user.data)
                        dict_final['includes']['users'] = users_list

                if 'place_id' in str(tweet.data):
                    if places:
                        for place in places:
                            if place['id'] == tweet['geo']['place_id']:
                                places_list = []
                                places_list.append(place.data)
                                dict_final['includes']['places'] = places_list

                document = monta_json(dict_final)
                insere_mongo(document, mongo_db, mongo_coll)
        logger.info(f'Ingestão concluida, consulte em: {mongo_ui}/db/{mongo_db}/{mongo_coll}')
    except Exception as e:
        logger.exception('Erro na ingestão dos dados!')
        sys.exit(3)