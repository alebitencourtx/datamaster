import tweepy
import datetime
import pytz
import pymongo
import sys

bearer_token = "AAAAAAAAAAAAAAAAAAAAABxAcwEAAAAAX%2BINcZZQMxgqbj3dyWETCNji0r8%3DbpVjZuIH9D6RMoE1iZnTmO0qwqCTumpppDNRu5XSE2R68OWEA9"

def montaJSON(tweet):
    """
    Metodo estruta o dicionario no layout correta para ingestao no MongoDB
    """
    # Define a data atual com timezone
    current_time = datetime.datetime.now(pytz.timezone('America/Sao_Paulo')).strftime("%Y-%m-%d %H:%M:%S")
    # Recebe o tweet e monta na estrutura {'_id', 'json'{}, 'dat_ref_carga_atlz'}
    dict_raw = tweet.data    
    dict_final = {}
    dict_final["_id"] = dict_raw["id"]
    dict_final["json"] = dict_raw
    dict_final['dat_ref_carga_batch'] = current_time
    
    return dict_final

def ingestMongo(document):
    """
    Metodo realiza a ingestão no mongo, na mesma tabela do fluxo fast, no método upsert.
    A coluna de dat_ref_carga_fast nao foi passada e por isso nao sera alterada, podendo assim rastrear as mudancas da tabela.
    Com ambos os fluxos (fast e batch) inserindo na mesma tabela, temos uma arquitetura lambda de ingestao.
    """
    try:
        myclient = pymongo.MongoClient("mongodb://mongodb:27017/")
        mydb = myclient["twitter"]
        mycol = mydb["santander_tts"]

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
        print('Failed : ' + str(e))
        sys.exit(3)   

        
if __name__ == "__main__":
    
    # Consulta no endpoit do twitter, a busca retorna tweets dos ultimos 7 dias
    # Buscando pelo termo "santander", onde nao seja retweet nem reply
    print("Buscando tweets...")
    client = tweepy.Client(bearer_token)
    response = client.search_recent_tweets("santander  -is:retweet -is:reply ", 
                                           max_results=10,
                                           tweet_fields=["id","author_id","text","created_at","geo","lang","public_metrics","source"]
                                          )

    # Lista de tweets retornados com base no filtro aplicado na busca
    tweets = response.data
    print("Tweets recebidos, iniciando ingestao no MongoDB")
    for tweet in tweets:
        document = montaJSON(tweet)
        ingestMongo(document)
    print("Carga concluida, consulte em: http://localhost:9999/db/twitter/santander_tts")