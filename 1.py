# -*- coding: utf-8 -*-
"""
Created on Fri Jul  2 23:48:16 2021

@author: SOUKAYNA ABIBOU
"""

import tweepy
import time
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer


"""API ACCESS KEYS"""
access_token = "835845423478763520-wqkuBoB4RxiXmyNPHOLirmQi1dZoHrV"
access_token_secret = "2ms1M4S6Iwyvuohsp4RGBTU2luUPfdZ5nrka5fUJb5jtQ"
consumer_key = "RHnpXisXzyRZ98sXFgVIQj0T3"
consumer_secret = "rUrSGMAnUUZgMxCDQ3n5QUJXVbpCO5aucVhw7xkbAOTfyXIDpv"

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api=tweepy.API(auth,wait_on_rate_limit=True)

from datetime import datetime
def normalize_timestamp(time):
    mytime=datetime.strptime(time,"%Y-%m-%d %H:%M:%S")
    return (mytime.strftime("%Y-%m-%d %H:%M:%S"))
producer=KafkaProducer(bootstrap_servers='localhost:9092')
topic_name='mytopic'
def get_twitter_data():
    res=api.search("Covid-19")
    for i in res:
        record=''
        record+=str(i.user.id_str)
        record+=';'
        record+=str(normalize_timestamp(str(i.created_at)))
        record+=';'
        record+=str(i.user.followers_count)
        record+=';'
        record+=str(i.user.location)
        record+=';'
        record+=str(i.favorite_count)
        record+=';'
        record+=str(i.retweet_count)
        record+=';'
        record+=str(i.text)
        record+=';'
        producer.send(topic_name,str.encode(record))
        
get_twitter_data()
def periodic_work(interval):
    while True:
        get_twitter_data()
        time.sleep(interval)
periodic_work(60*0.1)
consumer=KafkaConsumer(bootstrap_servers='localhost:9092',auto_offset_reset='latest',group_id='test4',consumer_timeout_ms=10000)
consumer.subscribe('mytopic')
type(consumer)
for message in consumer:
    print(message)



