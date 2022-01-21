import requests

import dramatiq

from dramatiq.brokers.kafka import KafkaBroker

kafka_broker = KafkaBroker(urls=["127.0.0.1:9092"])
dramatiq.set_broker(kafka_broker)

queue_name = "test13456"


@dramatiq.actor(queue_name=queue_name, priority=0)
def count_words(url):
    import time
    response = requests.get(url)
    time.sleep(5)
    count = len(response.text.split(" "))
    print(f"There are {count} words at {url!r}.")


@dramatiq.actor(queue_name=queue_name, priority=10)
def count_letter(url):
    import time
    response = requests.get(url)
    time.sleep(5)
    count = len(response.text)
    print(f"There are {count} letters at {url!r}.")

# while True:
# count_words.send("https://cyware.com")


# x = iter(kafka_broker.consume(queue_name, 1))
# for i in x:
#     print(x)
