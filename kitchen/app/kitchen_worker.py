from confluent_kafka import Consumer
import os
import connection_mongo as mongo 
from connection_redis import manager_redis
import time

KAFKA = os.getenv("KAFKA","local:9092")
CONFIG_CONSUMER = {
    'bootstrap.servers': KAFKA,
    'group.id': 'kitchen-team',
    'auto.offset.reset': 'earliest'} 

consumer = Consumer(CONFIG_CONSUMER)


def subscribe():
    consumer.subscribe(['pizza-orders'])
    while True:
        msg = consumer.poll()
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        print('Received message)' )
        key = msg.key().decode('utf-8')
        mongo.collection.update_one(
        {"order_id": key}, 
        {"$set": {"status": "DELIVERED"}}
        )
        manager_redis.delete(f"order:{key}")
        time.sleep(20)
        