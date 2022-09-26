from kafka import KafkaConsumer
import json

def Convert(string):
    li = list(string.split(" "))
    return li

topic_name = 'mytopic'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    auto_commit_interval_ms=5000,
    fetch_max_bytes=128,
    max_poll_records=100,

)
text = list()
i = 0
import elasticsearch as es
for message in consumer:
    if(i<50):
        tweets = message.value
        es.index(index="trump_index", doc_type="test_doc", body={"author":dict_data["user"]["screen_name"], "date":dict_data["created_at"], "message":dict_data["text"]})
        text.append(tweets)
        print(i)
        i = i + 1
    else:
        break






"""for message in consumer:
    tweets = message.value
    print(tweets)
    print(consumer.__dir__)
print(dir(consumer))

consumer.__str__
print(str(consumer))"""


"""all_words = list(itertools.chain(*consumer))
counts = collections.Counter(all_words)
line = counts.most_common(15)
for i in line:
    print(i)

for message in consumer:
    tweets = (message.value)
    print(tweets)"""

