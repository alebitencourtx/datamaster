import tweepy
from kafka import KafkaProducer
import json
import sys

#importando módulo utilities de outro path
sys.path.insert(0, '/app/utils')
from utilities import init_logger, current_time_sp
from settings import twitter, kafka
logger = init_logger('streaming_bronze_tt_tweets_extract')

# Leitura das configurações
bearer_token = twitter['bearer_token']
broker = kafka['broker']
kafka_ui = kafka['kafka-ui']
twitterTopic = kafka['twitterTopic']

logger.info("Aguardando tweets...")

class TweetsListenerSantander(tweepy.StreamingClient):

    def on_data(self, raw_data):
        try:
            data = json.loads(raw_data.decode())
            if 'santander' in str(data['data']['text']).lower():
                logger.info("Enviado tweet ao Kafka")
                logger.info(f"Acompanhe o tópico em: {kafka_ui}{twitterTopic}")
                producer.send(twitterTopic, str(data).encode())

        except Exception as e:
            logger.exception('Failed : ' + str(e))


producer = KafkaProducer(bootstrap_servers=broker)

ttsantander = TweetsListenerSantander(bearer_token)
# Filtra a busca pelo termo "santander" e que nao sejam retweet ou reply
ttsantander.add_rules(tweepy.StreamRule(value="santander -is:retweet -is:reply "))
# Filtra os campos de retorno
ttsantander.filter(tweet_fields=["id","author_id","text","created_at","geo","lang","public_metrics","source"],
                    user_fields = ['name','username','location','verified'],
                    expansions = ['geo.place_id', 'author_id'],
                    place_fields = ['country','country_code']
                  )
                  