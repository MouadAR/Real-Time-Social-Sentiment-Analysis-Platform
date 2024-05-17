import json
import tweepy
import configparser
from kafka import KafkaProducer

class TweeterStreamListener(tweepy.StreamListener):
    """ A class to read the twitter stream and push it to Kafka """

    def __init__(self, api):
        self.api = api
        super(tweepy.StreamListener, self).__init__()
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        

    def on_status(self, status):
        """ This method is called whenever new data arrives from live stream.
        We asynchronously push this data to kafka queue """
        msg = status.text
        try:
            future = self.producer.send('twitter_stream', value=msg)
            result = future.get(timeout=10)
        except Exception as e:
            print("Failed to send message to Kafka:", str(e))
            return False
        return True

    def on_error(self, status_code):
        print("Error received in kafka producer waaaaa")
        return True  # Don't kill the stream

    def on_timeout(self):
        return True  # Don't kill the stream

if __name__ == '__main__':
    # Read the credentials from 'twitter-app-credentials.txt' file
    config = configparser.ConfigParser()
    config.read('twitter-app-credentials.txt')
    consumer_key = config['DEFAULT']['consumerKey']
    consumer_secret = config['DEFAULT']['consumerSecret']
    access_key = config['DEFAULT']['accessToken']
    access_secret = config['DEFAULT']['accessTokenSecret']

    # Create Auth object
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    api = tweepy.API(auth)

    # Create stream and bind the listener to it
    stream = tweepy.Stream(auth, listener=TweeterStreamListener(api))

    # Custom Filter rules pull all traffic for those filters in real time.
    stream.filter(locations=[-180, -90, 180, 90], languages=['en'])
