from confluent_kafka import Consumer
import os
import connection_mongo as mongo 
from connection_redis import manager_redis

KAFKA = os.getenv("KAFKA","local:9092")
CONFIG_CONSUMER = {
    'bootstrap.servers': KAFKA,
    'group.id': 'kitchen-team',
    'auto.offset.reset': 'earliest'} 

consumer = Consumer(CONFIG_CONSUMER)


def subscribe():
    consumer.subscribe(['pizza-orders'])
    while True:
        msg = consumer.poll(15)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        print('Received message)' )
        key = msg.key().decode('utf-8')
        value = msg.value().decode('utf-8')
        doc = mongo.collection.find_one({"order_id":key})
        doc['status'] = 'DELIVERED'
        manager_redis.delete(key)
        print(value)
        