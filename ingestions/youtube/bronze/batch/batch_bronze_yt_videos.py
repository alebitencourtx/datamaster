# -*- coding: utf-8 -*-
from googleapiclient.discovery import build

import pymongo
import sys

#importando módulo utilities de outro path
sys.path.insert(0, '/app/utils')
from utilities import init_logger, current_time_sp
from settings import youtube, mongo
logger = init_logger('batch_bronze_yt_videos')

# Leitura das configurações
key = youtube['key']
mongo_db = youtube['mongo_db']
mongo_coll = youtube['mongo_coll']
mongo_host = mongo['host']
mongo_ui = mongo['mongo-express']


def monta_json(tweet):
    """
    Estruta o dicionário no layout correta para ingestão no MongoDB
    """
    # Define a data atual com timezone
    now = current_time_sp()
    # Recebe o tweet e monta na estrutura {'_id', 'json'{}, 'dat_ref_carga_batch'}
    dict_raw = tweet    
    dict_final = {}
    dict_final["_id"] = dict_raw['video']['resourceId']['videoId']
    dict_final["video"] = dict_raw['video']
    dict_final['dat_ref_carga_batch'] = now
    return dict_final


def insere_mongo(document, mongo_db, mongo_coll, host=mongo_host):
    """
    Metodo realiza a ingestão no mongo, no método upsert, utilizando o id do vídeo como _id do documento no banco.
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
        logger.exception('Erro na ingestão do MongoDB: ')
        sys.exit(3)    

def busca_id_canal(nome_canal):
    """
    Busca pelo nome do canal para retornar o ID, necessário para as próximas buscas
    """

    # Utiliza o endpoint de busca para obter o ID do canal pelo nome
    # Utiliza o primeiro retorno que, por sua vez, é ordenado pela relevancia da busca. 
    res = youtube.search().list(part="snippet", type="channel", q=nome_canal).execute()
    channelId = res['items'][0]['id']['channelId']
    return channelId

def busca_videos_canal(nome_canal):
    """
    Retorna todos os vídeos do canal, a partir da playlist "uploads".
    Realizando paginação no retorno do endpoint para interar sob toda a playlist.
    """
    
    channelId = busca_id_canal(nome_canal)
    # Consulta o endpoint de canais para obter a playlist de uploads
    res = youtube.channels().list(part='contentDetails', id=channelId,maxResults=50).execute()
    
    for item in res['items']:
        playlist_uploads = item["contentDetails"]["relatedPlaylists"]["uploads"]

    # Busca o detalhe de cada video na playlist "uploads" e adiciona a lista.
    # Realiza paginação para buscar todos os videos
    list_videos = []
    nextPage_token = None
    while True:
      res = youtube.playlistItems().list(part='snippet', playlistId = playlist_uploads, maxResults=50, pageToken=nextPage_token).execute()
      list_videos += res['items']

      nextPage_token = res.get('nextPageToken')

      if nextPage_token is None:
        break
    return list_videos

def busca_estatisticas_video(list_videos):
    """
    Retorna as estatisticas de cada vídeo para compor o resultado final
    """
    # Gera uma lista de id de todos os videos
    videos_ids = list(map(lambda x: x['snippet']['resourceId']['videoId'], list_videos))
    
    # Utiliza o endpoint de videos para obter as estatisticas de views, likes e comentários.
    stats = []
    for video_id in videos_ids:
      res = youtube.videos().list(part='statistics', id=video_id).execute()
      stats += res['items']
    return stats

if __name__ == "__main__":
    try:
        logger.info('Logando na API e chamando métodos de busca')
        youtube = build('youtube','v3', developerKey=key)
        list_videos = busca_videos_canal('Santander Brasil')
        stats = busca_estatisticas_video(list_videos)
        logger.info('%d videos retornados', len(list_videos))

        # Inicializa a estrutura do dicionário para agregar a estatistica ao video
        dict_final = dict()
        dict_final['video'] = dict()
        dict_final['video']['statistics'] = dict()
        
        # Compõe o dicionário com a estatistica de cada vídeo, monta o Json e envia ao MongoDB
        logger.info('Enviado videos ao MongoDB.')
        for video in list_videos:
            for stat in stats:
                if video['snippet']['resourceId']['videoId'] == stat['id']:
                    dict_final['video'] = video['snippet']
                    dict_final['video']['statistics'] = stat['statistics']

                    document = monta_json(dict_final)
                    insere_mongo(document, mongo_db, mongo_coll)
        logger.info(f'Ingestão concluida, consulte em: {mongo_ui}/db/{mongo_db}/{mongo_coll}')
    except Exception as e:
        logger.exception('Erro no main : ')
        sys.exit(3)  


    