import tweepy
from kafka import KafkaProducer
import json

BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAABxAcwEAAAAAX%2BINcZZQMxgqbj3dyWETCNji0r8%3DbpVjZuIH9D6RMoE1iZnTmO0qwqCTumpppDNRu5XSE2R68OWEA9"

class TweetsListenerSantander(tweepy.StreamingClient ):

    def on_data(self, raw_data):
        try:
            data = json.loads(raw_data.decode())
            print("Enviado tweet ao Kafka")
            print("Voce pode acompanhar o tópico em: http://localhost:8089/ui/clusters/local/topics/twitter")
            producer.send("twitter", str(data['data']).encode())

        except Exception as e:
            print('Failed : ' + str(e))


producer = KafkaProducer(bootstrap_servers='kafka:9092')

ttsantander = TweetsListenerSantander(BEARER_TOKEN)
# Filtra a busca pelo termo "santander" e que nao sejam retweet ou reply
ttsantander.add_rules(tweepy.StreamRule(value="santander -is:retweet -is:reply "))
# Filtra os campos de retorno
ttsantander.filter(tweet_fields=["id","author_id","text","created_at","geo","lang","public_metrics","source"])